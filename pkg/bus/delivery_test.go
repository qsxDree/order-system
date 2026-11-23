package bus

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ordersys/pkg/events"
	amqp "github.com/rabbitmq/amqp091-go"
)

type acknowledgements struct {
	acked, requeued bool
}

func (a *acknowledgements) Ack(uint64, bool) error { a.acked = true; return nil }
func (a *acknowledgements) Nack(_ uint64, _ bool, requeue bool) error {
	a.requeued = requeue
	return nil
}
func (a *acknowledgements) Reject(_ uint64, requeue bool) error { a.requeued = requeue; return nil }

func TestForwardFailureRetainsOriginalDelivery(t *testing.T) {
	body, err := json.Marshal(events.NewEnvelope("test", events.TypeOrderConfirmed, events.OrderConfirmedPayload{OrderID: "o1"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		body    []byte
		headers amqp.Table
	}{
		{"poison", []byte(`{}`), nil},
		{"retry", body, nil},
		{"exhausted", body, amqp.Table{"x-attempts": int32(5)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			ack := &acknowledgements{}
			b := NewMessageBus(Config{})
			b.handleDelivery(ctx, nil, nil, amqp.Delivery{Body: tc.body, Headers: tc.headers, Acknowledger: ack},
				"queue", "queue.retry", "queue.dlq", 5, func(context.Context, *events.RawEnvelope) error { return errors.New("failed") })
			if ack.acked || !ack.requeued {
				t.Fatalf("lost delivery: %+v", ack)
			}
		})
	}
}

func TestConnectRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	b := NewMessageBus(Config{URL: "amqp://localhost:1"})
	if err := b.Connect(ctx, 10, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want cancellation", err)
	}
}
