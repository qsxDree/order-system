package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/bus"
	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/idempotency"
	"github.com/ordersys/services/inventory-service/internal/db"
)

type InventoryHandler struct {
	pool   *pgxpool.Pool
	bus    *bus.MessageBus
	logger *slog.Logger
}

func NewInventoryHandler(pool *pgxpool.Pool, b *bus.MessageBus, logger *slog.Logger) *InventoryHandler {
	return &InventoryHandler{
		pool:   pool,
		bus:    b,
		logger: logger,
	}
}

// InsufficientStockError indicates stock reservation failed due to low quantity or simulation.
type InsufficientStockError struct {
	Reason string
}

func (e *InsufficientStockError) Error() string {
	return e.Reason
}

func (h *InventoryHandler) HandleMessage(ctx context.Context, raw *events.RawEnvelope) error {
	switch raw.Type {
	case events.TypeOrderCreated:
		return h.onOrderCreated(ctx, raw)
	case events.TypeOrderCancelled:
		return h.onOrderCancelled(ctx, raw)
	default:
		h.logger.DebugContext(ctx, "ignoring unhandled event type in inventory service", "type", raw.Type)
		return nil
	}
}

func (h *InventoryHandler) onOrderCreated(ctx context.Context, raw *events.RawEnvelope) error {
	parsed, err := events.UnmarshalPayload(raw.Type, raw.Data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal order.created payload: %w", err)
	}
	payload, ok := parsed.(events.OrderCreatedPayload)
	if !ok {
		return fmt.Errorf("unexpected payload type: %T", parsed)
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Idempotency check inside transaction
	newMsg, err := idempotency.TryMarkProcessed(ctx, tx, raw.MessageID, raw.Type)
	if err != nil {
		return fmt.Errorf("idempotency check failed: %w", err)
	}
	if !newMsg {
		h.logger.DebugContext(ctx, "duplicate order.created event ignored", "messageId", raw.MessageID)
		return nil
	}

	queries := db.New(tx)
	opts := bus.PublishOptions{
		CorrelationID: raw.CorrelationID,
		CausationID:   raw.MessageID,
	}

	// Failure simulation toggle
	if payload.Simulate != nil && payload.Simulate.OutOfStock {
		reason := "simulated out-of-stock (demo toggle)"
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		_, pubErr := h.bus.Publish(ctx, events.TypeInventoryRejected, events.InventoryRejectedPayload{
			OrderID: payload.OrderID,
			Reason:  reason,
		}, opts)
		h.logger.WarnContext(ctx, "inventory rejected (simulated)", "orderId", payload.OrderID)
		return pubErr
	}

	// Attempt stock reservation
	for _, item := range payload.Items {
		inv, err := queries.GetInventoryForUpdate(ctx, item.ProductID)
		if errors.Is(err, pgx.ErrNoRows) {
			// Auto-create product and initial stock for unknown items
			var priceNum pgtype.Numeric
			_ = priceNum.Scan(fmt.Sprintf("%.2f", item.UnitPrice))
			_, _ = queries.UpsertProduct(ctx, db.UpsertProductParams{
				ID:    item.ProductID,
				Sku:   item.SKU,
				Name:  item.SKU,
				Price: priceNum,
			})
			inv, err = queries.UpsertInventory(ctx, db.UpsertInventoryParams{
				ProductID: item.ProductID,
				Available: 100,
				Reserved:  0,
			})
		}
		if err != nil {
			return fmt.Errorf("failed to lock inventory for product %s: %w", item.ProductID, err)
		}

		if item.Quantity <= 0 || item.Quantity > 1_000_000 {
			return fmt.Errorf("invalid quantity: %d", item.Quantity)
		}
		itemQty := int32(item.Quantity) //nolint:gosec // bounded above

		if inv.Available < itemQty {
			reason := fmt.Sprintf("insufficient stock for %s (need %d, have %d)", item.SKU, item.Quantity, inv.Available)
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			_, pubErr := h.bus.Publish(ctx, events.TypeInventoryRejected, events.InventoryRejectedPayload{
				OrderID: payload.OrderID,
				Reason:  reason,
			}, opts)
			h.logger.WarnContext(ctx, "inventory rejected", "orderId", payload.OrderID, "reason", reason)
			return pubErr
		}

		// Deduct available & increment reserved
		_, err = queries.ReserveStock(ctx, db.ReserveStockParams{
			Available: itemQty,
			ProductID: item.ProductID,
		})
		if err != nil {
			return fmt.Errorf("failed to reserve stock for product %s: %w", item.ProductID, err)
		}

		// Record reservation
		_, err = queries.CreateReservation(ctx, db.CreateReservationParams{
			ID:        uuid.NewString(),
			OrderID:   payload.OrderID,
			ProductID: item.ProductID,
			Quantity:  itemQty,
			Status:    "HELD",
		})
		if err != nil {
			return fmt.Errorf("failed to create reservation record: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit reservation: %w", err)
	}

	_, pubErr := h.bus.Publish(ctx, events.TypeInventoryReserved, events.InventoryReservedPayload{
		OrderID:       payload.OrderID,
		ReservationID: payload.OrderID,
	}, opts)
	if pubErr != nil {
		return fmt.Errorf("failed to publish inventory.reserved: %w", pubErr)
	}

	h.logger.InfoContext(ctx, "inventory reserved", "orderId", payload.OrderID, "items", len(payload.Items))
	return nil
}

func (h *InventoryHandler) onOrderCancelled(ctx context.Context, raw *events.RawEnvelope) error {
	parsed, err := events.UnmarshalPayload(raw.Type, raw.Data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal order.cancelled payload: %w", err)
	}
	payload, ok := parsed.(events.OrderCancelledPayload)
	if !ok {
		return fmt.Errorf("unexpected payload type: %T", parsed)
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	newMsg, err := idempotency.TryMarkProcessed(ctx, tx, raw.MessageID, raw.Type)
	if err != nil {
		return fmt.Errorf("idempotency check failed: %w", err)
	}
	if !newMsg {
		h.logger.DebugContext(ctx, "duplicate order.cancelled event ignored", "messageId", raw.MessageID)
		return nil
	}

	queries := db.New(tx)
	held, err := queries.ListHeldReservations(ctx, payload.OrderID)
	if err != nil {
		return fmt.Errorf("failed to list held reservations: %w", err)
	}

	for _, r := range held {
		_, err = queries.RestoreStock(ctx, db.RestoreStockParams{
			Available: r.Quantity,
			ProductID: r.ProductID,
		})
		if err != nil {
			return fmt.Errorf("failed to restore stock for product %s: %w", r.ProductID, err)
		}

		_, err = queries.ReleaseReservation(ctx, r.ID)
		if err != nil {
			return fmt.Errorf("failed to release reservation %s: %w", r.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit stock restoration: %w", err)
	}

	if len(held) > 0 {
		h.logger.InfoContext(ctx, "inventory released (compensation)", "orderId", payload.OrderID, "releasedCount", len(held))
	}
	return nil
}
