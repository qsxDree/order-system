package events_test

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/ordersys/pkg/events"
)

func TestOrderTotal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		quantity int
		price    float64
		invalid  bool
	}{
		{"decimal cents", 3, 0.10, false},
		{"zero price", 1, 0, false},
		{"large valid price", 1, 9999999999.99, false},
		{"zero quantity", 0, 1, true},
		{"int32 overflow", 1 << 32, 1, true},
		{"negative price", 1, -1, true},
		{"subcent price", 1, 1.005, true},
		{"total overflow", 1000000, 9999999999, true},
		{"NaN", 1, math.NaN(), true},
		{"infinity", 1, math.Inf(1), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			total, err := events.OrderTotal([]events.OrderItem{{ProductID: "p1", SKU: "S1", Quantity: tc.quantity, UnitPrice: tc.price}})
			if (err != nil) != tc.invalid {
				t.Fatalf("total %v, err %v", total, err)
			}
			if tc.name == "decimal cents" && total != 0.30 {
				t.Fatalf("total = %.20f, want 0.30", total)
			}
		})
	}
	item := events.OrderItem{ProductID: "p1", SKU: "S1", Quantity: 1, UnitPrice: 1}
	if _, err := events.OrderTotal([]events.OrderItem{item, item}); err == nil {
		t.Fatal("accepted duplicate products")
	}
}

func TestParseRawRejectsMissingIdentity(t *testing.T) {
	valid, err := json.Marshal(events.NewEnvelope("test", "order.confirmed", events.OrderConfirmedPayload{OrderID: "o1"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range [][]byte{[]byte(`{}`), []byte(`null`), []byte(`{"messageId":"x","data":{}}`)} {
		if _, err := events.ParseRaw(body); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
	if _, err := events.ParseRaw(valid); err != nil {
		t.Fatal(err)
	}
}
