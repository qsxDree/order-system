package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/idempotency"
	"github.com/ordersys/pkg/outbox"
	"github.com/ordersys/services/shipping-service/internal/db"
)

type ShippingHandler struct {
	pool         *pgxpool.Pool
	deliverAfter time.Duration
	logger       *slog.Logger
}

func NewShippingHandler(pool *pgxpool.Pool, deliverAfter time.Duration, logger *slog.Logger) *ShippingHandler {
	return &ShippingHandler{
		pool:         pool,
		deliverAfter: deliverAfter,
		logger:       logger,
	}
}

func (h *ShippingHandler) HandleMessage(ctx context.Context, raw *events.RawEnvelope) error {
	switch raw.Type {
	case events.TypeOrderConfirmed:
		return h.onOrderConfirmed(ctx, raw)
	default:
		h.logger.DebugContext(ctx, "ignoring unhandled event type in shipping service", "type", raw.Type)
		return nil
	}
}

func (h *ShippingHandler) onOrderConfirmed(ctx context.Context, raw *events.RawEnvelope) error {
	parsed, err := events.UnmarshalPayload(raw.Type, raw.Data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal order.confirmed payload: %w", err)
	}
	payload, ok := parsed.(events.OrderConfirmedPayload)
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
		h.logger.DebugContext(ctx, "duplicate order.confirmed event ignored", "messageId", raw.MessageID)
		return nil
	}

	queries := db.New(tx)
	trackingNumber := fmt.Sprintf("TRK-%s", strings.ToUpper(uuid.NewString()[:10]))
	shipmentID := uuid.NewString()

	shipment, err := queries.CreateShipment(ctx, db.CreateShipmentParams{
		ID:             shipmentID,
		OrderID:        payload.OrderID,
		Status:         "DISPATCHED",
		TrackingNumber: trackingNumber,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return fmt.Errorf("failed to create shipment: %w", err)
	}

	if err := outbox.Enqueue(ctx, tx, payload.OrderID, events.TypeShipmentDispatched,
		raw.CorrelationID, raw.MessageID, events.ShipmentDispatchedPayload{
			OrderID: payload.OrderID, TrackingNumber: shipment.TrackingNumber,
		}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// StartDeliveries resumes dispatched shipments from the database after a restart.
func (h *ShippingHandler) StartDeliveries(ctx context.Context) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				workCtx, stop := context.WithTimeout(ctx, 10*time.Second)
				if err := h.deliverDue(workCtx); err != nil && ctx.Err() == nil {
					h.logger.ErrorContext(ctx, "delivery simulation failed", "err", err)
				}
				stop()
			}
		}
	}()
	return func() { cancel(); <-done }
}

func (h *ShippingHandler) deliverDue(ctx context.Context) error {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	shipments, err := queries.ListDueShipments(ctx, pgtype.Timestamptz{
		Time: time.Now().Add(-h.deliverAfter), Valid: true,
	})
	if err != nil {
		return err
	}
	for _, shipment := range shipments {
		if _, err := queries.UpdateShipmentStatus(ctx, db.UpdateShipmentStatusParams{
			OrderID: shipment.OrderID, Status: "DELIVERED",
		}); err != nil {
			return err
		}
		if err := outbox.Enqueue(ctx, tx, shipment.OrderID, events.TypeShipmentDelivered,
			shipment.OrderID, "", events.ShipmentDeliveredPayload{OrderID: shipment.OrderID}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
