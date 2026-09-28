package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/metrics"
	"github.com/ordersys/services/inventory-service/internal/db"
	"github.com/ordersys/services/inventory-service/internal/seed"
)

type InventoryInfo struct {
	Available int32 `json:"available"`
	Reserved  int32 `json:"reserved"`
}

type ProductResponse struct {
	ID        string         `json:"id"`
	SKU       string         `json:"sku"`
	Name      string         `json:"name"`
	Price     string         `json:"price"`
	Inventory *InventoryInfo `json:"inventory"`
}

type CatalogResponse struct {
	Products []ProductResponse `json:"products"`
}

func numericToString(n pgtype.Numeric) string {
	val, err := n.Value()
	if err != nil || val == nil {
		return "0.00"
	}
	return fmt.Sprintf("%v", val)
}

func NewRouter(pool *pgxpool.Pool, logger *slog.Logger) http.Handler {
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

	queries := db.New(pool)

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"service": "inventory-service",
		})
	})

	r.Handle("/metrics", metrics.Handler())

	r.Get("/api/products", func(w http.ResponseWriter, r *http.Request) {
		rows, err := queries.ListProductsWithInventory(r.Context())
		if err != nil {
			logger.ErrorContext(r.Context(), "failed to list products", "err", err)
			http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
			return
		}

		products := make([]ProductResponse, 0, len(rows))
		for _, row := range rows {
			products = append(products, ProductResponse{
				ID:    row.ID,
				SKU:   row.Sku,
				Name:  row.Name,
				Price: numericToString(row.Price),
				Inventory: &InventoryInfo{
					Available: int32(row.Available),
					Reserved:  int32(row.Reserved),
				},
			})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(CatalogResponse{Products: products})
	})

	r.Post("/api/seed", func(w http.ResponseWriter, r *http.Request) {
		if err := seed.SeedProducts(r.Context(), pool, logger); err != nil {
			logger.ErrorContext(r.Context(), "failed to seed catalog", "err", err)
			http.Error(w, `{"error":"seed_failed"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "seeded",
			"count":  len(seed.DefaultSeed),
		})
	})

	return r
}
