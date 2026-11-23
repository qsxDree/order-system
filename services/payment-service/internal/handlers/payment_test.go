package handlers

import (
	"log/slog"
	"testing"

	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/testutil"
)

func TestPaymentAndOutcomeCommitTogether(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failed], func(t *testing.T) {
			pool := testutil.NewDB(t, "payment")
			h := NewPaymentHandler(pool, slog.Default())
			raw := testutil.Event(t, events.TypeOrderCreated, events.OrderCreatedPayload{
				OrderID: "o1", CustomerID: "c1", Currency: "USD", TotalAmount: 0.3,
				Items:    []events.OrderItem{{ProductID: "p1", SKU: "S1", Quantity: 3, UnitPrice: 0.1}},
				Simulate: &events.SimulateOptions{PaymentFailure: failed},
			})
			// Force an outbox error to prove the payment and idempotency marker roll back.
			if _, err := pool.Exec(t.Context(), `ALTER TABLE outbox ADD CONSTRAINT reject_test CHECK (false)`); err != nil {
				t.Fatal(err)
			}
			if err := h.HandleMessage(t.Context(), raw); err == nil {
				t.Fatal("expected outbox failure")
			}
			var payments, processed int
			if err := pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM payments), (SELECT count(*) FROM processed_messages)`).Scan(&payments, &processed); err != nil {
				t.Fatal(err)
			}
			if payments != 0 || processed != 0 {
				t.Fatal("committed payment without durable outcome")
			}
			if _, err := pool.Exec(t.Context(), `ALTER TABLE outbox DROP CONSTRAINT reject_test`); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := h.HandleMessage(t.Context(), raw); err != nil {
					t.Fatal(err)
				}
			}
			var status, eventType string
			var count int
			if err := pool.QueryRow(t.Context(), `SELECT status, (SELECT type FROM outbox), (SELECT count(*) FROM payments) FROM payments`).Scan(&status, &eventType, &count); err != nil {
				t.Fatal(err)
			}
			wantStatus, wantEvent := "SUCCEEDED", events.TypePaymentSucceeded
			if failed {
				wantStatus, wantEvent = "FAILED", events.TypePaymentFailed
			}
			if count != 1 || status != wantStatus || eventType != wantEvent {
				t.Fatalf("status=%s event=%s count=%d", status, eventType, count)
			}
			if err := h.HandleMessage(t.Context(), testutil.Event(t, events.TypeOrderCancelled, events.OrderCancelledPayload{OrderID: "o1"})); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(t.Context(), `SELECT status FROM payments`).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if !failed && status != "REFUNDED" {
				t.Fatal("successful payment was not refunded")
			}
		})
	}
}

func TestCancellationBeforeCreatePreventsCharge(t *testing.T) {
	pool := testutil.NewDB(t, "payment")
	h := NewPaymentHandler(pool, slog.Default())
	if err := h.HandleMessage(t.Context(), testutil.Event(t, events.TypeOrderCancelled, events.OrderCancelledPayload{OrderID: "o1"})); err != nil {
		t.Fatal(err)
	}
	if err := h.HandleMessage(t.Context(), testutil.Event(t, events.TypeOrderCreated, events.OrderCreatedPayload{
		OrderID: "o1", CustomerID: "c1", Currency: "USD", TotalAmount: 1,
		Items: []events.OrderItem{{ProductID: "p1", SKU: "S1", Quantity: 1, UnitPrice: 1}},
	})); err != nil {
		t.Fatal(err)
	}
	var payments, outcomes int
	if err := pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM payments), (SELECT count(*) FROM outbox)`).Scan(&payments, &outcomes); err != nil {
		t.Fatal(err)
	}
	if payments != 0 || outcomes != 0 {
		t.Fatal("charged a cancelled order")
	}
}
