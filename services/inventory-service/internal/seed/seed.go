package seed

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/services/inventory-service/internal/db"
)

type SeedItem struct {
	ID    string
	SKU   string
	Name  string
	Price string
	Stock int32
}

var DefaultSeed = []SeedItem{
	{ID: "prod-espresso", SKU: "ESP-01", Name: "Espresso Beans 1kg", Price: "24.00", Stock: 50},
	{ID: "prod-mug", SKU: "MUG-01", Name: "Ceramic Mug", Price: "14.00", Stock: 200},
	{ID: "prod-grinder", SKU: "GRD-01", Name: "Hand Grinder", Price: "45.00", Stock: 30},
	{ID: "prod-filter", SKU: "FLT-01", Name: "Paper Filters (100)", Price: "8.00", Stock: 500},
	{ID: "prod-kettle", SKU: "KTL-01", Name: "Gooseneck Kettle", Price: "60.00", Stock: 5},
}

// SeedProducts seeds the catalog with default items if not present. Idempotent.
func SeedProducts(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	queries := db.New(pool)

	for _, item := range DefaultSeed {
		var num pgtype.Numeric
		if err := num.Scan(item.Price); err != nil {
			return fmt.Errorf("failed to scan price %s: %w", item.Price, err)
		}

		_, err := queries.UpsertProduct(ctx, db.UpsertProductParams{
			ID:    item.ID,
			Sku:   item.SKU,
			Name:  item.Name,
			Price: num,
		})
		if err != nil {
			return fmt.Errorf("failed to upsert product %s: %w", item.SKU, err)
		}

		_, err = queries.UpsertInventory(ctx, db.UpsertInventoryParams{
			ProductID: item.ID,
			Available: item.Stock,
			Reserved:  0,
		})
		if err != nil {
			return fmt.Errorf("failed to upsert inventory for product %s: %w", item.SKU, err)
		}
	}

	logger.Info("catalog seeded", "products", len(DefaultSeed))
	return nil
}
