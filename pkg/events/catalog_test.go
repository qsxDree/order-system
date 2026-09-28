package events_test

import (
	"encoding/json"
	"testing"

	"github.com/ordersys/pkg/events"
)

func TestEventCatalog_AllTypesRegistered(t *testing.T) {
	expected := []string{
		"order.created",
		"inventory.reserved",
		"inventory.rejected",
		"payment.succeeded",
		"payment.failed",
		"order.confirmed",
		"order.cancelled",
		"shipment.dispatched",
		"shipment.delivered",
	}

	for _, exp := range expected {
		if !events.IsValidEventType(exp) {
			t.Errorf("expected %s to be a valid event type", exp)
		}
	}

	if events.IsValidEventType("order.exploded") {
		t.Error("expected order.exploded to be invalid")
	}
}

func TestEventCatalog_ParseValidOrderCreated(t *testing.T) {
	rawJSON := `{
		"orderId": "o1",
		"customerId": "cust",
		"items": [{"productId": "p", "sku": "S", "quantity": 1, "unitPrice": 5.0}],
		"totalAmount": 5.0,
		"currency": "USD"
	}`

	parsed, err := events.UnmarshalPayload(events.TypeOrderCreated, []byte(rawJSON))
	if err != nil {
		t.Fatalf("expected valid unmarshal, got: %v", err)
	}

	payload, ok := parsed.(events.OrderCreatedPayload)
	if !ok {
		t.Fatalf("expected OrderCreatedPayload, got %T", parsed)
	}

	if payload.OrderID != "o1" || payload.CustomerID != "cust" || len(payload.Items) != 1 || payload.TotalAmount != 5.0 {
		t.Errorf("unexpected payload content: %+v", payload)
	}
}

func TestEventCatalog_RejectInvalidPayload(t *testing.T) {
	// Empty items
	rawJSON := `{
		"orderId": "o1",
		"customerId": "cust",
		"items": [],
		"totalAmount": 5.0,
		"currency": "USD"
	}`

	_, err := events.UnmarshalPayload(events.TypeOrderCreated, []byte(rawJSON))
	if err == nil {
		t.Fatal("expected error on empty items, got nil")
	}
}

func TestEventCatalog_SimulateFlagsCarried(t *testing.T) {
	rawJSON := `{
		"orderId": "o1",
		"customerId": "cust",
		"items": [{"productId": "p", "sku": "S", "quantity": 1, "unitPrice": 5.0}],
		"totalAmount": 5.0,
		"currency": "USD",
		"simulate": {
			"paymentFailure": true,
			"outOfStock": false
		}
	}`

	parsed, err := events.UnmarshalPayload(events.TypeOrderCreated, []byte(rawJSON))
	if err != nil {
		t.Fatalf("expected valid unmarshal, got: %v", err)
	}

	payload := parsed.(events.OrderCreatedPayload)
	if payload.Simulate == nil || !payload.Simulate.PaymentFailure {
		t.Errorf("expected simulate.paymentFailure=true, got: %+v", payload.Simulate)
	}
}

func TestEventCatalog_AllEventPayloadsUnmarshal(t *testing.T) {
	cases := []struct {
		eventType string
		json      string
	}{
		{events.TypeInventoryReserved, `{"orderId": "o1", "reservationId": "r1"}`},
		{events.TypeInventoryRejected, `{"orderId": "o1", "reason": "out of stock"}`},
		{events.TypePaymentSucceeded, `{"orderId": "o1", "paymentId": "p1", "amount": 20.5}`},
		{events.TypePaymentFailed, `{"orderId": "o1", "reason": "card declined"}`},
		{events.TypeOrderConfirmed, `{"orderId": "o1"}`},
		{events.TypeOrderCancelled, `{"orderId": "o1", "reason": "inventory unavailable"}`},
		{events.TypeShipmentDispatched, `{"orderId": "o1", "trackingNumber": "TRK-12345"}`},
		{events.TypeShipmentDelivered, `{"orderId": "o1"}`},
	}

	for _, c := range cases {
		t.Run(c.eventType, func(t *testing.T) {
			val, err := events.UnmarshalPayload(c.eventType, []byte(c.json))
			if err != nil {
				t.Fatalf("failed to unmarshal %s: %v", c.eventType, err)
			}
			out, err := json.Marshal(val)
			if err != nil {
				t.Fatalf("failed to re-marshal: %v", err)
			}
			if len(out) == 0 {
				t.Errorf("empty marshal output")
			}
		})
	}
}
