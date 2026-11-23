package integration

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ordersys/pkg/bus"
	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/logger"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

func TestMessageBusReliability(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("skipping integration test without RUN_INTEGRATION=1")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	log := logger.NewLogger("bus-test", "debug")

	rmqContainer, err := rabbitmq.Run(ctx, "rabbitmq:3.13-management-alpine")
	if err != nil {
		t.Fatalf("failed to start rabbitmq container: %v", err)
	}
	defer func() {
		_ = testcontainers.TerminateContainer(rmqContainer)
	}()

	amqpURL, err := rmqContainer.AmqpURL(ctx)
	if err != nil {
		t.Fatalf("failed to get amqp url: %v", err)
	}

	b := bus.NewMessageBus(bus.Config{
		URL:      amqpURL,
		Producer: "bus-test",
		Logger:   log,
	})

	if err := b.Connect(ctx, 5, time.Second); err != nil {
		t.Fatalf("failed to connect message bus: %v", err)
	}
	defer func() { _ = b.Close() }()
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()

	if _, err := b.Publish(ctx, "unbound.event", map[string]string{"orderId": "o1"}); err == nil {
		t.Fatal("reported success for an unroutable publication")
	}
	var observed atomic.Int32
	if err := b.Subscribe(ctx, bus.SubscribeOptions{
		Queue: "test.observer", RoutingKeys: []string{events.TypeOrderCreated},
		Handler: func(context.Context, *events.RawEnvelope) error { observed.Add(1); return nil },
	}); err != nil {
		t.Fatal(err)
	}

	queueName := "test.always-fails"
	var attempts atomic.Int32

	err = b.Subscribe(ctx, bus.SubscribeOptions{
		Queue: queueName,
		RoutingKeys: []string{
			events.TypeOrderCreated,
		},
		MaxRetries: 1, // 1 retry -> 2 attempts total, then DLQ
		Handler: func(ctx context.Context, raw *events.RawEnvelope) error {
			if raw.MessageID != "stable-id" {
				t.Errorf("message identity changed: %s", raw.MessageID)
			}
			attempts.Add(1)
			return errors.New("boom")
		},
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	payload := events.OrderCreatedPayload{
		OrderID:    "o1",
		CustomerID: "c1",
		Items: []events.OrderItem{
			{ProductID: "p1", SKU: "S1", Quantity: 1, UnitPrice: 1},
		},
		TotalAmount: 1,
		Currency:    "USD",
	}

	if _, err := b.Publish(ctx, events.TypeOrderCreated, payload, bus.PublishOptions{MessageID: "stable-id"}); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	waitFor(t, func() bool {
		q, err := ch.QueueDeclarePassive(queueName+".dlq", true, false, false, false, nil)
		return err == nil && q.Messages == 1 && observed.Load() == 1
	})

	if got := attempts.Load(); got < 2 {
		t.Fatalf("expected at least 2 attempts, got %d", got)
	}

	replayed, err := b.ReplayDeadLettered(ctx, queueName, 10)
	if err != nil {
		t.Fatalf("failed to replay dead-lettered: %v", err)
	}
	if replayed < 1 {
		t.Errorf("expected at least 1 replayed message, got %d", replayed)
	}
	waitFor(t, func() bool {
		q, err := ch.QueueDeclarePassive(queueName+".dlq", true, false, false, false, nil)
		return err == nil && q.Messages == 1 && attempts.Load() == 4
	})
	if got := observed.Load(); got != 1 {
		t.Fatalf("replay reached unrelated subscriber %d times", got)
	}
	// Invalid envelopes bypass business handlers and are retained in the DLQ.
	if err := ch.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		q, err := ch.QueueDeclarePassive(queueName+".dlq", true, false, false, false, nil)
		return err == nil && q.Messages == 2
	})
	if got := attempts.Load(); got != 4 {
		t.Fatalf("poison message reached business handler: attempts=%d", got)
	}
	if _, err := ch.QueueDelete("test.observer", false, false, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("unexpected consumer closure did not signal the service to restart")
	}
}

func waitFor(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for broker state")
		case <-ticker.C:
		}
	}
}
