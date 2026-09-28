package consumer_test

import (
	"testing"

	"github.com/ordersys/services/notification-service/internal/consumer"
)

func TestNotificationTemplates(t *testing.T) {
	cases := []struct {
		eventType       string
		expectedChannel string
	}{
		{"order.confirmed", "email"},
		{"order.cancelled", "email"},
		{"shipment.dispatched", "sms"},
		{"shipment.delivered", "email"},
	}

	for _, c := range cases {
		tmpl := consumer.GetTemplate(c.eventType)
		if tmpl.Channel != c.expectedChannel {
			t.Errorf("for %s expected channel %s, got %s", c.eventType, c.expectedChannel, tmpl.Channel)
		}
		if tmpl.Template == "" {
			t.Errorf("for %s expected non-empty template", c.eventType)
		}
	}
}
