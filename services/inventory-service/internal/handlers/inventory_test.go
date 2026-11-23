package handlers

import (
	"log/slog"
	"testing"

	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/testutil"
	"github.com/ordersys/services/inventory-service/internal/seed"
)

func TestReservationsAreAtomicAndSeedPreservesStock(t *testing.T) {
	pool := testutil.NewDB(t, "inventory")
	ctx := t.Context()
	if err := seed.SeedProducts(ctx, pool, slog.Default()); err != nil {
		t.Fatal(err)
	}
	h := NewInventoryHandler(pool, slog.Default())
	payload := events.OrderCreatedPayload{
		OrderID: "rejected", CustomerID: "c1", Currency: "USD", TotalAmount: 144,
		Items: []events.OrderItem{
			{ProductID: "prod-espresso", SKU: "ESP-01", Quantity: 1, UnitPrice: 24},
			{ProductID: "prod-kettle", SKU: "KTL-01", Quantity: 2, UnitPrice: 60},
		},
	}
	if _, err := pool.Exec(ctx, `UPDATE inventory SET available = 0 WHERE product_id = 'prod-kettle'`); err != nil {
		t.Fatal(err)
	}
	rejected := testutil.Event(t, events.TypeOrderCreated, payload)
	for range 2 {
		if err := h.HandleMessage(ctx, rejected); err != nil {
			t.Fatal(err)
		}
	}
	var available, reserved, reservations, rejectedEvents int
	if err := pool.QueryRow(ctx, `SELECT available, reserved,
		(SELECT count(*) FROM reservations), (SELECT count(*) FROM outbox WHERE type = 'inventory.rejected')
		FROM inventory WHERE product_id = 'prod-espresso'`).Scan(&available, &reserved, &reservations, &rejectedEvents); err != nil {
		t.Fatal(err)
	}
	if available != 50 || reserved != 0 || reservations != 0 || rejectedEvents != 1 {
		t.Fatalf("partial reservation or duplicate event: stock=%d/%d reservations=%d events=%d", available, reserved, reservations, rejectedEvents)
	}

	payload.OrderID, payload.Items, payload.TotalAmount = "reserved", payload.Items[:1], 24
	created := testutil.Event(t, events.TypeOrderCreated, payload)
	for range 2 {
		if err := h.HandleMessage(ctx, created); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.SeedProducts(ctx, pool, slog.Default()); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT available, reserved FROM inventory WHERE product_id = 'prod-espresso'`).Scan(&available, &reserved); err != nil {
		t.Fatal(err)
	}
	if available != 49 || reserved != 1 {
		t.Fatalf("seed reset reserved stock: %d/%d", available, reserved)
	}
	for range 2 {
		if err := h.HandleMessage(ctx, testutil.Event(t, events.TypeOrderCancelled, events.OrderCancelledPayload{OrderID: "reserved"})); err != nil {
			t.Fatal(err)
		}
	}
	if err := pool.QueryRow(ctx, `SELECT available, reserved FROM inventory WHERE product_id = 'prod-espresso'`).Scan(&available, &reserved); err != nil {
		t.Fatal(err)
	}
	if available != 50 || reserved != 0 {
		t.Fatalf("duplicate restoration: %d/%d", available, reserved)
	}

	payload.OrderID = "cancelled-first"
	if err := h.HandleMessage(ctx, testutil.Event(t, events.TypeOrderCancelled, events.OrderCancelledPayload{OrderID: payload.OrderID})); err != nil {
		t.Fatal(err)
	}
	if err := h.HandleMessage(ctx, testutil.Event(t, events.TypeOrderCreated, payload)); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM reservations WHERE order_id = $1`, payload.OrderID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("reserved stock after cancellation")
	}
}

func TestUnknownProductIsRejected(t *testing.T) {
	pool := testutil.NewDB(t, "inventory")
	h := NewInventoryHandler(pool, slog.Default())
	created := testutil.Event(t, events.TypeOrderCreated, events.OrderCreatedPayload{
		OrderID: "o1", CustomerID: "c1", Currency: "USD", TotalAmount: 1,
		Items: []events.OrderItem{{ProductID: "unknown", SKU: "UNKNOWN", Quantity: 1, UnitPrice: 1}},
	})
	if err := h.HandleMessage(t.Context(), created); err != nil {
		t.Fatal(err)
	}
	var products, rejected int
	if err := pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM products), (SELECT count(*) FROM outbox WHERE type = 'inventory.rejected')`).Scan(&products, &rejected); err != nil {
		t.Fatal(err)
	}
	if products != 0 || rejected != 1 {
		t.Fatalf("invented stock for unknown product: products=%d rejected=%d", products, rejected)
	}
}

func TestClientCannotOverrideCatalogPrice(t *testing.T) {
	pool := testutil.NewDB(t, "inventory")
	if err := seed.SeedProducts(t.Context(), pool, slog.Default()); err != nil {
		t.Fatal(err)
	}
	h := NewInventoryHandler(pool, slog.Default())
	raw := testutil.Event(t, events.TypeOrderCreated, events.OrderCreatedPayload{
		OrderID: "o1", CustomerID: "c1", Currency: "USD", TotalAmount: 0.01,
		Items: []events.OrderItem{{ProductID: "prod-kettle", SKU: "KTL-01", Quantity: 1, UnitPrice: 0.01}},
	})
	if err := h.HandleMessage(t.Context(), raw); err != nil {
		t.Fatal(err)
	}
	var reservations, rejected int
	if err := pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM reservations), (SELECT count(*) FROM outbox WHERE type = 'inventory.rejected')`).Scan(&reservations, &rejected); err != nil {
		t.Fatal(err)
	}
	if reservations != 0 || rejected != 1 {
		t.Fatalf("accepted client price override: reservations=%d rejected=%d", reservations, rejected)
	}
}
