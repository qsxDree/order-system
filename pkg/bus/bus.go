package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/logger"
	"github.com/ordersys/pkg/metrics"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeName = "orders.topic"
)

// HandlerFunc is the signature of a message handler.
type HandlerFunc func(ctx context.Context, raw *events.RawEnvelope) error

// PublishOptions provides optional headers and overrides for published messages.
type PublishOptions struct {
	CorrelationID string
	CausationID   string
	MessageID     string
}

// SubscribeOptions defines parameters for subscribing a queue to the topic exchange.
type SubscribeOptions struct {
	Queue       string
	RoutingKeys []string
	Prefetch    int
	MaxRetries  int
	Handler     HandlerFunc
}

// Config holds configuration parameters for the MessageBus.
type Config struct {
	URL      string
	Producer string
	Logger   *slog.Logger
}

// MessageBus encapsulates RabbitMQ connection, channel, publishing, and subscription management.
type MessageBus struct {
	cfg     Config
	conn    *amqp.Connection
	pubChan *amqp.Channel
	pubMu   sync.Mutex
	closed  bool
	mu      sync.Mutex
}

// NewMessageBus creates a new MessageBus instance.
func NewMessageBus(cfg Config) *MessageBus {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &MessageBus{
		cfg: cfg,
	}
}

// Connect establishes a connection to RabbitMQ with retry loops and asserts the durable topic exchange.
func (b *MessageBus) Connect(ctx context.Context, retries int, delay time.Duration) error {
	if retries <= 0 {
		retries = 10
	}
	if delay <= 0 {
		delay = 2 * time.Second
	}

	var lastErr error
	for attempt := 1; attempt <= retries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := amqp.Dial(b.cfg.URL)
		if err == nil {
			ch, err := conn.Channel()
			if err != nil {
				_ = conn.Close()
				lastErr = err
				b.cfg.Logger.Warn("failed to open publisher channel, retrying...", "attempt", attempt, "err", err)
				time.Sleep(delay)
				continue
			}

			// Declare durable topic exchange
			err = ch.ExchangeDeclare(
				ExchangeName,
				"topic",
				true,  // durable
				false, // auto-deleted
				false, // internal
				false, // no-wait
				nil,   // arguments
			)
			if err != nil {
				_ = ch.Close()
				_ = conn.Close()
				lastErr = err
				b.cfg.Logger.Warn("failed to declare exchange, retrying...", "attempt", attempt, "err", err)
				time.Sleep(delay)
				continue
			}

			b.mu.Lock()
			b.conn = conn
			b.pubChan = ch
			b.closed = false
			b.mu.Unlock()

			b.cfg.Logger.Info("connected to RabbitMQ", "exchange", ExchangeName)
			return nil
		}

		lastErr = err
		b.cfg.Logger.Warn("RabbitMQ not ready, retrying...", "attempt", attempt, "max", retries, "err", err)
		time.Sleep(delay)
	}

	return fmt.Errorf("could not connect to RabbitMQ after %d attempts: %w", retries, lastErr)
}

// Publish publishes a typed payload wrapped in an EventEnvelope to the orders.topic exchange.
func (b *MessageBus) Publish(ctx context.Context, eventType string, data any, opts ...PublishOptions) (string, error) {
	b.pubMu.Lock()
	defer b.pubMu.Unlock()

	if b.pubChan == nil {
		return "", errors.New("message bus not connected — call Connect first")
	}

	var opt PublishOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	msgID := opt.MessageID
	if msgID == "" {
		msgID = uuid.NewString()
	}

	corrID := opt.CorrelationID
	if corrID == "" {
		corrID = logger.GetCorrelationID(ctx)
		if corrID == "" {
			corrID = uuid.NewString()
		}
	}

	envelope := events.NewEnvelope(b.cfg.Producer, eventType, data, events.EnvelopeOptions{
		MessageID:     msgID,
		CorrelationID: corrID,
		CausationID:   opt.CausationID,
	})

	body, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("failed to marshal event envelope: %w", err)
	}

	pub := amqp.Publishing{
		DeliveryMode:  amqp.Persistent,
		MessageId:     msgID,
		CorrelationId: corrID,
		ContentType:   "application/json",
		Type:          eventType,
		Timestamp:     time.Now().UTC(),
		Body:          body,
	}

	err = b.pubChan.PublishWithContext(
		ctx,
		ExchangeName,
		eventType,
		false, // mandatory
		false, // immediate
		pub,
	)
	if err != nil {
		return "", fmt.Errorf("failed to publish event: %w", err)
	}

	metrics.EventsPublishedTotal.WithLabelValues(eventType, b.cfg.Producer).Inc()
	b.cfg.Logger.DebugContext(ctx, "published event",
		"type", eventType,
		"messageId", msgID,
		"correlationId", corrID,
	)

	return msgID, nil
}

// Subscribe declares the main queue, retry queue, and DLQ, then begins processing messages.
func (b *MessageBus) Subscribe(ctx context.Context, opts SubscribeOptions) error {
	b.mu.Lock()
	conn := b.conn
	b.mu.Unlock()

	if conn == nil {
		return errors.New("message bus not connected")
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open consumer channel: %w", err)
	}

	prefetch := opts.Prefetch
	if prefetch <= 0 {
		prefetch = 10
	}
	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	queue := opts.Queue
	retryQueue := queue + ".retry"
	dlq := queue + ".dlq"

	// 1. Declare Main Queue
	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return fmt.Errorf("failed to declare queue %s: %w", queue, err)
	}

	// 2. Declare DLQ
	_, err = ch.QueueDeclare(dlq, true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return fmt.Errorf("failed to declare dlq %s: %w", dlq, err)
	}

	// 3. Declare Retry Queue with DLX back to main queue
	retryArgs := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": queue,
	}
	_, err = ch.QueueDeclare(retryQueue, true, false, false, false, retryArgs)
	if err != nil {
		_ = ch.Close()
		return fmt.Errorf("failed to declare retry queue %s: %w", retryQueue, err)
	}

	// 4. Bind Main Queue to Exchange for each routing key
	for _, key := range opts.RoutingKeys {
		if err := ch.QueueBind(queue, key, ExchangeName, false, nil); err != nil {
			_ = ch.Close()
			return fmt.Errorf("failed to bind queue %s to %s with key %s: %w", queue, ExchangeName, key, err)
		}
	}

	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return fmt.Errorf("failed to set channel QoS: %w", err)
	}

	deliveries, err := ch.Consume(
		queue,
		"",    // consumer tag
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		_ = ch.Close()
		return fmt.Errorf("failed to start consuming from %s: %w", queue, err)
	}

	b.cfg.Logger.Info("subscribed to queue",
		"queue", queue,
		"routingKeys", opts.RoutingKeys,
		"maxRetries", maxRetries,
	)

	go b.consumeLoop(ctx, ch, deliveries, queue, retryQueue, dlq, maxRetries, opts.Handler)
	return nil
}

func (b *MessageBus) consumeLoop(
	ctx context.Context,
	ch *amqp.Channel,
	deliveries <-chan amqp.Delivery,
	queue, retryQueue, dlq string,
	maxRetries int,
	handler HandlerFunc,
) {
	defer func() {
		_ = ch.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-deliveries:
			if !ok {
				return
			}
			b.handleDelivery(ctx, ch, msg, queue, retryQueue, dlq, maxRetries, handler)
		}
	}
}

func (b *MessageBus) handleDelivery(
	ctx context.Context,
	ch *amqp.Channel,
	msg amqp.Delivery,
	queue, retryQueue, dlq string,
	maxRetries int,
	handler HandlerFunc,
) {
	// Parse raw envelope
	rawEnv, err := events.ParseRaw(msg.Body)
	if err != nil {
		b.cfg.Logger.Error("unprocessable poison pill message -> sending to DLQ",
			"queue", queue,
			"err", err,
		)
		headers := msg.Headers
		if headers == nil {
			headers = amqp.Table{}
		}
		headers["x-error"] = "unprocessable"
		headers["x-original-queue"] = queue

		_ = ch.PublishWithContext(ctx, "", dlq, false, false, amqp.Publishing{
			DeliveryMode:  amqp.Persistent,
			Headers:       headers,
			Body:          msg.Body,
			MessageId:     msg.MessageId,
			CorrelationId: msg.CorrelationId,
			Type:          msg.Type,
		})
		_ = msg.Ack(false)
		return
	}

	msgCtx := logger.WithCorrelationID(ctx, rawEnv.CorrelationID)
	startTime := time.Now()

	// Execute handler
	handleErr := handler(msgCtx, rawEnv)
	duration := time.Since(startTime).Seconds()
	metrics.ProcessingDurationSeconds.WithLabelValues(rawEnv.Type, queue).Observe(duration)

	if handleErr == nil {
		metrics.EventsConsumedTotal.WithLabelValues(rawEnv.Type, queue, "success").Inc()
		_ = msg.Ack(false)
		return
	}

	// Handler failed - determine attempt count
	attempts := 1
	if rawVal, exists := msg.Headers["x-attempts"]; exists {
		switch v := rawVal.(type) {
		case int32:
			attempts = int(v) + 1
		case int64:
			attempts = int(v) + 1
		case int:
			attempts = v + 1
		}
	}

	headers := msg.Headers
	if headers == nil {
		headers = amqp.Table{}
	}
	headers["x-attempts"] = int32(attempts)

	forwardPub := amqp.Publishing{
		DeliveryMode:  amqp.Persistent,
		Headers:       headers,
		Body:          msg.Body,
		MessageId:     msg.MessageId,
		CorrelationId: msg.CorrelationId,
		Type:          msg.Type,
	}

	if attempts <= maxRetries {
		delay := CalculateBackoff(attempts)
		b.cfg.Logger.WarnContext(msgCtx, "handler failed -> publishing to retry queue",
			"attempts", attempts,
			"maxRetries", maxRetries,
			"delayMs", delay.Milliseconds(),
			"queue", queue,
			"err", handleErr.Error(),
		)

		forwardPub.Expiration = fmt.Sprintf("%d", delay.Milliseconds())
		_ = ch.PublishWithContext(ctx, "", retryQueue, false, false, forwardPub)
		metrics.EventsConsumedTotal.WithLabelValues(rawEnv.Type, queue, "retry").Inc()
	} else {
		b.cfg.Logger.ErrorContext(msgCtx, "retries exhausted -> sending to DLQ",
			"attempts", attempts,
			"queue", queue,
			"err", handleErr.Error(),
		)
		headers["x-error"] = handleErr.Error()
		headers["x-original-queue"] = queue
		forwardPub.Headers = headers

		_ = ch.PublishWithContext(ctx, "", dlq, false, false, forwardPub)
		metrics.EventsConsumedTotal.WithLabelValues(rawEnv.Type, queue, "dlq").Inc()
	}

	_ = msg.Ack(false)
}

// ReplayDeadLettered pulls up to limit messages from queue.dlq and re-publishes them to orders.topic.
func (b *MessageBus) ReplayDeadLettered(ctx context.Context, queue string, limit int) (int, error) {
	b.pubMu.Lock()
	defer b.pubMu.Unlock()

	if b.conn == nil {
		return 0, errors.New("message bus not connected")
	}

	ch, err := b.conn.Channel()
	if err != nil {
		return 0, fmt.Errorf("failed to open replay channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if limit <= 0 {
		limit = 50
	}

	dlq := queue + ".dlq"
	count := 0

	for i := 0; i < limit; i++ {
		msg, ok, err := ch.Get(dlq, false)
		if err != nil {
			return count, fmt.Errorf("error reading from dlq: %w", err)
		}
		if !ok {
			break
		}

		eventType := msg.Type
		if eventType == "" {
			var raw events.RawEnvelope
			if err := json.Unmarshal(msg.Body, &raw); err == nil {
				eventType = raw.Type
			}
		}

		err = ch.PublishWithContext(ctx, ExchangeName, eventType, false, false, amqp.Publishing{
			DeliveryMode:  amqp.Persistent,
			MessageId:     msg.MessageId,
			CorrelationId: msg.CorrelationId,
			Type:          eventType,
			Body:          msg.Body,
		})
		if err != nil {
			_ = msg.Nack(false, true)
			return count, fmt.Errorf("failed to republish dlq message: %w", err)
		}

		_ = msg.Ack(false)
		count++
	}

	if count > 0 {
		b.cfg.Logger.Info("replayed dead-lettered messages", "queue", queue, "count", count)
	}
	return count, nil
}

// Close gracefully closes the publisher channel and connection.
func (b *MessageBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}
	b.closed = true

	var errPub, errConn error
	if b.pubChan != nil {
		errPub = b.pubChan.Close()
	}
	if b.conn != nil {
		errConn = b.conn.Close()
	}

	if errPub != nil {
		return errPub
	}
	return errConn
}
