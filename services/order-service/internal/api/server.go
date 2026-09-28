package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/bus"
	"github.com/ordersys/pkg/metrics"
	"github.com/ordersys/services/order-service/internal/config"
)

func NewRouter(cfg config.Config, pool *pgxpool.Pool, b *bus.MessageBus, logger *slog.Logger) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"service": "order-service",
		})
	})

	r.Handle("/metrics", metrics.Handler())

	// Product catalog proxy (BFF pattern for frontend)
	r.Get("/api/products", handleCatalogProxy(cfg, httpClient))

	// Orders CRUD
	r.Route("/api/orders", func(r chi.Router) {
		r.Post("/", handleCreateOrder(pool, logger))
		r.Get("/", handleListOrders(pool, logger))
		r.Get("/{id}", handleGetOrder(pool, logger))
	})

	// Ops endpoints (Health aggregator, RabbitMQ Queues inspect, DLQ replay)
	r.Route("/api/ops", func(r chi.Router) {
		r.Get("/health", handleOpsHealth(cfg, httpClient))
		r.Get("/queues", handleOpsQueues(cfg, httpClient))
		r.Post("/dlq/{queue}/replay", handleReplayDLQ(b, logger))
	})

	return r
}
