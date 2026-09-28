package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ordersys/pkg/metrics"
)

func TestMetrics_HandlerExposesMetrics(t *testing.T) {
	metrics.EventsPublishedTotal.WithLabelValues("order.created", "order-service").Inc()
	metrics.EventsConsumedTotal.WithLabelValues("order.created", "inventory-service.events", "success").Inc()
	metrics.ProcessingDurationSeconds.WithLabelValues("order.created", "inventory-service.events").Observe(0.015)
	metrics.NotificationsSentTotal.WithLabelValues("order.confirmed", "email").Inc()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	metrics.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	for _, metricName := range []string{
		"events_published_total",
		"events_consumed_total",
		"event_processing_duration_seconds",
		"notifications_sent_total",
	} {
		if !strings.Contains(body, metricName) {
			t.Errorf("expected metrics output to contain %s", metricName)
		}
	}
}
