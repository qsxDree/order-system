package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ordersys/services/order-service/internal/config"
)

func TestOrderServiceHealthAndMetrics(t *testing.T) {
	cfg := config.Config{
		Port: 4001,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	router := NewRouter(cfg, nil, nil, logger)

	t.Run("GET /healthz returns ok", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rec.Code)
		}

		var body map[string]string
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if body["status"] != "ok" || body["service"] != "order-service" {
			t.Errorf("unexpected body: %+v", body)
		}
	})

	t.Run("GET /metrics returns 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rec.Code)
		}
	})
}
