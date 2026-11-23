package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/ordersys/pkg/bus"
	"github.com/ordersys/services/order-service/internal/config"
)

type HealthInfo struct {
	Name   string `json:"name"`
	Status string `json:"status"` // "up" | "down"
}

type HealthResponse struct {
	Services []HealthInfo `json:"services"`
}

type RabbitQueueRaw struct {
	Name                   string `json:"name"`
	Messages               int    `json:"messages"`
	MessagesReady          int    `json:"messages_ready"`
	MessagesUnacknowledged int    `json:"messages_unacknowledged"`
	Consumers              int    `json:"consumers"`
}

type QueueInfo struct {
	Name      string `json:"name"`
	Messages  int    `json:"messages"`
	Ready     int    `json:"ready"`
	Unacked   int    `json:"unacked"`
	Consumers int    `json:"consumers"`
	IsDlq     bool   `json:"isDlq"`
}

type QueuesResponse struct {
	Queues []QueueInfo `json:"queues"`
}

func handleCatalogProxy(cfg config.Config, httpClient *http.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqURL := fmt.Sprintf("%s/api/products", cfg.Services.Inventory)
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, reqURL, nil)
		if err != nil {
			http.Error(w, `{"error":"inventory_unavailable","products":[]}`, http.StatusBadGateway)
			return
		}

		resp, err := httpClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				_ = resp.Body.Close()
			}
			http.Error(w, `{"error":"inventory_unavailable","products":[]}`, http.StatusBadGateway)
			return
		}
		defer func() { _ = resp.Body.Close() }()

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.Copy(w, resp.Body)
	}
}

func handleOpsHealth(cfg config.Config, httpClient *http.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		targets := map[string]string{
			"order-service":        fmt.Sprintf("http://localhost:%d/healthz", cfg.Port),
			"inventory-service":    fmt.Sprintf("%s/healthz", cfg.Services.Inventory),
			"payment-service":      fmt.Sprintf("%s/healthz", cfg.Services.Payment),
			"shipping-service":     fmt.Sprintf("%s/healthz", cfg.Services.Shipping),
			"notification-service": fmt.Sprintf("%s/healthz", cfg.Services.Notification),
		}

		var wg sync.WaitGroup
		results := make([]HealthInfo, 0, len(targets))
		var mu sync.Mutex

		for name, url := range targets {
			wg.Add(1)
			go func(serviceName, healthURL string) {
				defer wg.Done()
				status := "down"

				reqCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
				defer cancel()

				req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, healthURL, nil)
				if err == nil {
					resp, err := httpClient.Do(req)
					if err == nil && resp.StatusCode == http.StatusOK {
						status = "up"
					}
					if resp != nil {
						_ = resp.Body.Close()
					}
				}

				mu.Lock()
				results = append(results, HealthInfo{Name: serviceName, Status: status})
				mu.Unlock()
			}(name, url)
		}

		wg.Wait()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(HealthResponse{Services: results})
	}
}

func handleOpsQueues(cfg config.Config, httpClient *http.Client) http.HandlerFunc {
	authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", cfg.Mgmt.User, cfg.Mgmt.Pass)))

	return func(w http.ResponseWriter, r *http.Request) {
		reqURL := fmt.Sprintf("%s/api/queues", cfg.Mgmt.URL)
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, reqURL, nil)
		if err != nil {
			http.Error(w, `{"error":"rabbitmq_mgmt_unavailable","queues":[]}`, http.StatusBadGateway)
			return
		}
		req.Header.Set("Authorization", authHeader)

		resp, err := httpClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				_ = resp.Body.Close()
			}
			http.Error(w, `{"error":"rabbitmq_mgmt_unavailable","queues":[]}`, http.StatusBadGateway)
			return
		}
		defer func() { _ = resp.Body.Close() }()

		var rawQueues []RabbitQueueRaw
		if err := json.NewDecoder(resp.Body).Decode(&rawQueues); err != nil {
			http.Error(w, `{"error":"rabbitmq_mgmt_unavailable","queues":[]}`, http.StatusBadGateway)
			return
		}

		filtered := make([]QueueInfo, 0, len(rawQueues))
		for _, q := range rawQueues {
			if strings.HasPrefix(q.Name, "amq.") {
				continue
			}
			filtered = append(filtered, QueueInfo{
				Name:      q.Name,
				Messages:  q.Messages,
				Ready:     q.MessagesReady,
				Unacked:   q.MessagesUnacknowledged,
				Consumers: q.Consumers,
				IsDlq:     strings.HasSuffix(q.Name, ".dlq"),
			})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(QueuesResponse{Queues: filtered})
	}
}

func handleReplayDLQ(b *bus.MessageBus, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rawQueue := chi.URLParam(r, "queue")
		baseQueue := strings.TrimSuffix(rawQueue, ".dlq")
		switch baseQueue {
		case "order-service.saga", "inventory-service.events", "payment-service.events", "shipping-service.events", "notification-service.events":
		default:
			http.Error(w, `{"error":"invalid_queue"}`, http.StatusBadRequest)
			return
		}

		replayed, err := b.ReplayDeadLettered(r.Context(), baseQueue, 50)
		if err != nil {
			logger.ErrorContext(r.Context(), "dlq replay failed", "queue", baseQueue, "err", err)
			http.Error(w, `{"error":"replay_failed"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"queue":    baseQueue,
			"replayed": replayed,
		})
	}
}
