package handlers

import (
	"log/slog"
	"testing"
	"time"

	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/testutil"
)

func TestDeliveryResumesAfterRestartAndCannotRegress(t *testing.T) {
	pool := testutil.NewDB(t, "shipping")
	h := NewShippingHandler(pool, time.Hour, slog.Default())
	raw := testutil.Event(t, events.TypeOrderConfirmed, events.OrderConfirmedPayload{OrderID: "o1"})
	for range 2 {
		if err := h.HandleMessage(t.Context(), raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.deliverDue(t.Context()); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := pool.QueryRow(t.Context(), `SELECT status FROM shipments`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "DISPATCHED" {
		t.Fatal("delivered before transit delay")
	}

	// A new handler has no in-memory timers, but still finishes persisted dispatches.
	restarted := NewShippingHandler(pool, 0, slog.Default())
	for range 2 {
		if err := restarted.deliverDue(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if err := restarted.HandleMessage(t.Context(), testutil.Event(t, events.TypeOrderConfirmed, events.OrderConfirmedPayload{OrderID: "o1"})); err != nil {
		t.Fatal(err)
	}
	var dispatched, delivered int
	if err := pool.QueryRow(t.Context(), `SELECT status,
		(SELECT count(*) FROM outbox WHERE type = 'shipment.dispatched'),
		(SELECT count(*) FROM outbox WHERE type = 'shipment.delivered') FROM shipments`).Scan(&status, &dispatched, &delivered); err != nil {
		t.Fatal(err)
	}
	if status != "DELIVERED" || dispatched != 1 || delivered != 1 {
		t.Fatalf("status=%s dispatches=%d deliveries=%d", status, dispatched, delivered)
	}
}
