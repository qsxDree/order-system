package saga

import (
	"log/slog"
	"sync"
	"testing"

	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/testutil"
)

func TestConcurrentSuccessesConfirmOnceAndShipmentCannotRegress(t *testing.T) {
	pool := testutil.NewDB(t, "order")
	if _, err := pool.Exec(t.Context(), `INSERT INTO orders (id, customer_id, total_amount) VALUES ('o1', 'c1', 1)`); err != nil {
		t.Fatal(err)
	}
	c := NewCoordinator(pool, slog.Default())
	messages := []*events.RawEnvelope{
		testutil.Event(t, events.TypeInventoryReserved, events.InventoryReservedPayload{OrderID: "o1", ReservationID: "r1"}),
		testutil.Event(t, events.TypePaymentSucceeded, events.PaymentSucceededPayload{OrderID: "o1", PaymentID: "p1", Amount: 1}),
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(messages))
	for _, raw := range messages {
		wg.Go(func() { errs <- c.HandleMessage(t.Context(), raw) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range messages {
		if err := c.HandleMessage(t.Context(), raw); err != nil {
			t.Fatal(err)
		}
	}
	var status string
	var confirmations int
	if err := pool.QueryRow(t.Context(), `SELECT status, (SELECT count(*) FROM outbox WHERE type = 'order.confirmed') FROM orders`).Scan(&status, &confirmations); err != nil {
		t.Fatal(err)
	}
	if status != "CONFIRMED" || confirmations != 1 {
		t.Fatalf("status=%s confirmations=%d", status, confirmations)
	}
	for _, raw := range []*events.RawEnvelope{
		testutil.Event(t, events.TypeShipmentDelivered, events.ShipmentDeliveredPayload{OrderID: "o1"}),
		testutil.Event(t, events.TypeShipmentDispatched, events.ShipmentDispatchedPayload{OrderID: "o1", TrackingNumber: "TRK-1"}),
	} {
		if err := c.HandleMessage(t.Context(), raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := pool.QueryRow(t.Context(), `SELECT status FROM orders`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "DELIVERED" {
		t.Fatalf("out-of-order dispatch regressed delivered order: %s", status)
	}
}
