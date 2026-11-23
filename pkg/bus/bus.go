package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
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
	cfg        Config
	conn       *amqp.Connection
	pubChan    *amqp.Channel
	pubReturns <-chan amqp.Return
	pubMu      sync.Mutex
	closed     bool
	mu         sync.Mutex
	done       chan struct{}
	stopped    sync.Once
}

// NewMessageBus creates a new MessageBus instance.
func NewMessageBus(cfg Config) *MessageBus {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &MessageBus{
		cfg:  cfg,
		done: make(chan struct{}),
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
		if err := ctx.Err(); err != nil {
			return err
		}
		conn, err := amqp.DialConfig(b.cfg.URL, amqp.Config{
			Dial: func(network, address string) (net.Conn, error) {
				conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
					_ = conn.Close()
					return nil, err
				}
				return conn, nil
			},
		})
		if err == nil {
			var ch *amqp.Channel
			ch, err = conn.Channel()
			if err == nil {
				err = ch.ExchangeDeclare(ExchangeName, "topic", true, false, false, false, nil)
			}
			var returns <-chan amqp.Return
			if err == nil {
				returns, err = enableConfirms(ch)
			}
			if err == nil {
				b.pubMu.Lock()
				b.mu.Lock()
				b.conn, b.pubChan, b.pubReturns, b.closed = conn, ch, returns, false
				b.mu.Unlock()
				b.pubMu.Unlock()
				closed := conn.NotifyClose(make(chan *amqp.Error, 1))
				go func() {
					if err := <-closed; err != nil {
						b.cfg.Logger.Error("RabbitMQ connection closed", "err", err)
					}
					b.stopped.Do(func() { close(b.done) })
				}()
				b.cfg.Logger.Info("connected to RabbitMQ", "exchange", ExchangeName)
				return nil
			}
			_ = conn.Close()
		}
		lastErr = err
		b.cfg.Logger.Warn("RabbitMQ connection failed", "attempt", attempt, "err", err)
		if attempt < retries {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return fmt.Errorf("could not connect to RabbitMQ after %d attempts: %w", retries, lastErr)
}

// Done closes when the broker connection or an active consumer stops.
func (b *MessageBus) Done() <-chan struct{} { return b.done }

func enableConfirms(ch *amqp.Channel) (<-chan amqp.Return, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, err
	}
	return ch.NotifyReturn(make(chan amqp.Return, 1)), nil
}

func publishConfirmed(ctx context.Context, ch *amqp.Channel, returns <-chan amqp.Return,
	exchange, key string, msg amqp.Publishing) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, true, false, msg)
	if err != nil {
		return err
	}
	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		_ = ch.Close()
		return fmt.Errorf("waiting for publisher confirmation: %w", err)
	}
	if !acked {
		return errors.New("broker did not confirm publication")
	}
	select {
	case returned := <-returns:
		return fmt.Errorf("unroutable publication to %s: %s", key, returned.ReplyText)
	default:
		return nil
	}
}

// Publish publishes a typed payload wrapped in an EventEnvelope to the orders.topic exchange.
func (b *MessageBus) Publish(ctx context.Context, eventType string, data any, opts ...PublishOptions) (string, error) {
	b.pubMu.Lock()
	defer b.pubMu.Unlock()

	b.mu.Lock()
	conn, closed := b.conn, b.closed
	b.mu.Unlock()
	if conn == nil || closed || conn.IsClosed() {
		return "", errors.New("message bus not connected")
	}
	if b.pubChan == nil || b.pubChan.IsClosed() {
		ch, err := conn.Channel()
		if err != nil {
			return "", err
		}
		returns, err := enableConfirms(ch)
		if err != nil {
			_ = ch.Close()
			return "", err
		}
		b.pubChan, b.pubReturns = ch, returns
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

	err = publishConfirmed(ctx, b.pubChan, b.pubReturns, ExchangeName, eventType, pub)
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
	if opts.Queue == "" || len(opts.RoutingKeys) == 0 || opts.Handler == nil {
		return errors.New("queue, routing keys and handler are required")
	}
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

	returns, err := enableConfirms(ch)
	if err != nil {
		_ = ch.Close()
		return err
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

	go b.consumeLoop(ctx, ch, returns, deliveries, queue, retryQueue, dlq, maxRetries, opts.Handler)
	return nil
}

func (b *MessageBus) consumeLoop(
	ctx context.Context,
	ch *amqp.Channel,
	returns <-chan amqp.Return,
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
				if ctx.Err() == nil {
					b.cfg.Logger.ErrorContext(ctx, "consumer stopped unexpectedly", "queue", queue)
					b.stopped.Do(func() { close(b.done) })
				}
				return
			}
			b.handleDelivery(ctx, ch, returns, msg, queue, retryQueue, dlq, maxRetries, handler)
		}
	}
}

func (b *MessageBus) handleDelivery(
	ctx context.Context,
	ch *amqp.Channel,
	returns <-chan amqp.Return,
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

		b.forwardDelivery(ctx, ch, returns, msg, dlq, amqp.Publishing{
			DeliveryMode:  amqp.Persistent,
			Headers:       headers,
			Body:          msg.Body,
			MessageId:     msg.MessageId,
			CorrelationId: msg.CorrelationId,
			Type:          msg.Type,
		})
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
		b.forwardDelivery(ctx, ch, returns, msg, retryQueue, forwardPub)
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

		b.forwardDelivery(ctx, ch, returns, msg, dlq, forwardPub)
		metrics.EventsConsumedTotal.WithLabelValues(rawEnv.Type, queue, "dlq").Inc()
	}

}

func (b *MessageBus) forwardDelivery(ctx context.Context, ch *amqp.Channel, returns <-chan amqp.Return,
	msg amqp.Delivery, queue string, publication amqp.Publishing) {
	if err := publishConfirmed(ctx, ch, returns, "", queue, publication); err != nil {
		b.cfg.Logger.ErrorContext(ctx, "forwarding failed; retaining original delivery", "queue", queue, "err", err)
		_ = msg.Nack(false, true)
		return
	}
	if err := msg.Ack(false); err != nil {
		b.cfg.Logger.ErrorContext(ctx, "failed to acknowledge forwarded delivery", "err", err)
	}
}

// ReplayDeadLettered returns dead letters only to their original queue, with a fresh retry budget.
func (b *MessageBus) ReplayDeadLettered(ctx context.Context, queue string, limit int) (int, error) {
	b.mu.Lock()
	conn := b.conn
	b.mu.Unlock()
	if conn == nil {
		return 0, errors.New("message bus not connected")
	}
	ch, err := conn.Channel()
	if err != nil {
		return 0, fmt.Errorf("failed to open replay channel: %w", err)
	}
	defer func() { _ = ch.Close() }()
	returns, err := enableConfirms(ch)
	if err != nil {
		return 0, err
	}
	if _, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil); err != nil {
		return 0, fmt.Errorf("replay target queue does not exist: %w", err)
	}

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

		err = publishConfirmed(ctx, ch, returns, "", queue, amqp.Publishing{
			DeliveryMode: amqp.Persistent, ContentType: msg.ContentType,
			MessageId: msg.MessageId, CorrelationId: msg.CorrelationId,
			Type: msg.Type, Body: msg.Body, Timestamp: msg.Timestamp,
		})
		if err != nil {
			_ = msg.Nack(false, true)
			return count, fmt.Errorf("failed to republish dlq message: %w", err)
		}

		if err := msg.Ack(false); err != nil {
			return count, fmt.Errorf("failed to acknowledge replay: %w", err)
		}
		count++
	}

	if count > 0 {
		b.cfg.Logger.Info("replayed dead-lettered messages", "queue", queue, "count", count)
	}
	return count, nil
}

// Close gracefully closes the publisher channel and connection.
func (b *MessageBus) Close() error {
	b.pubMu.Lock()
	defer b.pubMu.Unlock()
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
