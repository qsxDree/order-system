package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry is the default Prometheus registry used across the application.
var Registry = prometheus.NewRegistry()

var (
	// EventsPublishedTotal counts events published to the bus.
	EventsPublishedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "events_published_total",
			Help: "Events published to the bus",
		},
		[]string{"type", "producer"},
	)

	// EventsConsumedTotal counts events consumed from the bus, labeled by status (success | retry | dlq).
	EventsConsumedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "events_consumed_total",
			Help: "Events consumed from the bus, by outcome",
		},
		[]string{"type", "queue", "status"},
	)

	// ProcessingDurationSeconds measures handler execution time per event.
	ProcessingDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "event_processing_duration_seconds",
			Help:    "Handler processing time per event",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"type", "queue"},
	)

	// NotificationsSentTotal counts notifications sent by event type and channel.
	NotificationsSentTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_sent_total",
			Help: "Total notifications dispatched",
		},
		[]string{"event_type", "channel"},
	)
)

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		EventsPublishedTotal,
		EventsConsumedTotal,
		ProcessingDurationSeconds,
		NotificationsSentTotal,
	)
}

// Handler returns an HTTP handler for serving the Prometheus metrics endpoint.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}
