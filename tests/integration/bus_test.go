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

	queueName := "test.always-fails"
	var attempts atomic.Int32

	err = b.Subscribe(ctx, bus.SubscribeOptions{
		Queue: queueName,
		RoutingKeys: []string{
			events.TypeOrderCreated,
		},
		MaxRetries: 1, // 1 retry -> 2 attempts total, then DLQ
		Handler: func(ctx context.Context, raw *events.RawEnvelope) error {
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

	if _, err := b.Publish(ctx, events.TypeOrderCreated, payload); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Wait for the initial attempt + backoff retry to land on DLQ
	time.Sleep(6 * time.Second)

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
}
