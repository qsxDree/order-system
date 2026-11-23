package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/idempotency"
	"github.com/ordersys/pkg/outbox"
	"github.com/ordersys/services/inventory-service/internal/db"
)

type InventoryHandler struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

func NewInventoryHandler(pool *pgxpool.Pool, logger *slog.Logger) *InventoryHandler {
	return &InventoryHandler{
		pool:   pool,
		logger: logger,
	}
}

// InsufficientStockError rejects an order without retaining partial reservations.
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
	if err := queries.LockOrder(ctx, payload.OrderID); err != nil {
		return err
	}
	cancelled, err := queries.IsOrderCancelled(ctx, payload.OrderID)
	if err != nil {
		return err
	}
	if cancelled {
		return tx.Commit(ctx)
	}

	// A savepoint keeps the processed-message marker while rolling back every partial reservation.
	reservationTx, err := tx.Begin(ctx)
	if err != nil {
		return err
	}
	err = h.reserve(ctx, db.New(reservationTx), payload)
	var stockErr *InsufficientStockError
	if errors.As(err, &stockErr) {
		if err := reservationTx.Rollback(ctx); err != nil {
			return err
		}
		if err := outbox.Enqueue(ctx, tx, payload.OrderID, events.TypeInventoryRejected,
			raw.CorrelationID, raw.MessageID, events.InventoryRejectedPayload{
				OrderID: payload.OrderID, Reason: stockErr.Reason,
			}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	if err := reservationTx.Commit(ctx); err != nil {
		return err
	}

	if err := outbox.Enqueue(ctx, tx, payload.OrderID, events.TypeInventoryReserved,
		raw.CorrelationID, raw.MessageID, events.InventoryReservedPayload{
			OrderID: payload.OrderID, ReservationID: payload.OrderID,
		}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (h *InventoryHandler) reserve(ctx context.Context, queries *db.Queries, payload events.OrderCreatedPayload) error {
	if payload.Simulate != nil && payload.Simulate.OutOfStock {
		return &InsufficientStockError{Reason: "simulated out-of-stock (demo toggle)"}
	}

	items := slices.Clone(payload.Items)
	slices.SortFunc(items, func(a, b events.OrderItem) int { return strings.Compare(a.ProductID, b.ProductID) })
	for _, item := range items {
		inv, err := queries.GetInventoryForUpdate(ctx, item.ProductID)
		if errors.Is(err, pgx.ErrNoRows) {
			return &InsufficientStockError{Reason: fmt.Sprintf("unknown product %s", item.ProductID)}
		}
		if err != nil {
			return fmt.Errorf("failed to lock inventory for product %s: %w", item.ProductID, err)
		}
		price, err := inv.Price.Float64Value()
		if err != nil {
			return fmt.Errorf("failed to read catalog price: %w", err)
		}
		if item.SKU != inv.Sku || item.UnitPrice != price.Float64 {
			return &InsufficientStockError{Reason: fmt.Sprintf("catalog details changed for product %s", item.ProductID)}
		}
		quantity := int32(item.Quantity) //nolint:gosec // UnmarshalPayload bounds quantities to 1,000,000.
		if inv.Available < quantity {
			return &InsufficientStockError{Reason: fmt.Sprintf("insufficient stock for %s (need %d, have %d)",
				item.SKU, item.Quantity, inv.Available)}
		}
		_, err = queries.ReserveStock(ctx, db.ReserveStockParams{
			Available: quantity, ProductID: item.ProductID,
		})
		if err != nil {
			return fmt.Errorf("failed to reserve stock: %w", err)
		}
		_, err = queries.CreateReservation(ctx, db.CreateReservationParams{
			ID: uuid.NewString(), OrderID: payload.OrderID, ProductID: item.ProductID,
			Quantity: quantity, Status: "HELD",
		})
		if err != nil {
			return fmt.Errorf("failed to create reservation: %w", err)
		}
	}
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
	if err := queries.LockOrder(ctx, payload.OrderID); err != nil {
		return err
	}
	if err := queries.MarkOrderCancelled(ctx, payload.OrderID); err != nil {
		return err
	}
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
