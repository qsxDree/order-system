package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/bus"
	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/idempotency"
	"github.com/ordersys/services/shipping-service/internal/db"
)

type ShippingHandler struct {
	pool         *pgxpool.Pool
	bus          *bus.MessageBus
	deliverAfter time.Duration
	logger       *slog.Logger
}

func NewShippingHandler(pool *pgxpool.Pool, b *bus.MessageBus, deliverAfter time.Duration, logger *slog.Logger) *ShippingHandler {
	return &ShippingHandler{
		pool:         pool,
		bus:          b,
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

	shipment, err := queries.UpsertShipment(ctx, db.UpsertShipmentParams{
		ID:             shipmentID,
		OrderID:        payload.OrderID,
		Status:         "DISPATCHED",
		TrackingNumber: trackingNumber,
	})
	if err != nil {
		return fmt.Errorf("failed to upsert shipment: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit dispatched shipment: %w", err)
	}

	opts := bus.PublishOptions{
		CorrelationID: raw.CorrelationID,
		CausationID:   raw.MessageID,
	}

	_, pubErr := h.bus.Publish(ctx, events.TypeShipmentDispatched, events.ShipmentDispatchedPayload{
		OrderID:        payload.OrderID,
		TrackingNumber: shipment.TrackingNumber,
	}, opts)
	if pubErr != nil {
		return fmt.Errorf("failed to publish shipment.dispatched: %w", pubErr)
	}

	h.logger.InfoContext(ctx, "shipment dispatched 📦", "orderId", payload.OrderID, "trackingNumber", shipment.TrackingNumber)

	// Simulate transit -> delivery background task
	orderID := payload.OrderID
	corrID := raw.CorrelationID
	go func() {
		time.Sleep(h.deliverAfter)
		deliverCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()

		if err := h.deliver(deliverCtx, orderID, corrID); err != nil {
			h.logger.Error("delivery simulation failed", "orderId", orderID, "err", err)
		}
	}()

	return nil
}

func (h *ShippingHandler) deliver(ctx context.Context, orderID string, correlationID string) error {
	queries := db.New(h.pool)
	_, err := queries.UpdateShipmentStatus(ctx, db.UpdateShipmentStatusParams{
		Status:  "DELIVERED",
		OrderID: orderID,
	})
	if err != nil {
		return fmt.Errorf("failed to update shipment to DELIVERED: %w", err)
	}

	_, err = h.bus.Publish(ctx, events.TypeShipmentDelivered, events.ShipmentDeliveredPayload{
		OrderID: orderID,
	}, bus.PublishOptions{
		CorrelationID: correlationID,
	})
	if err != nil {
		return fmt.Errorf("failed to publish shipment.delivered: %w", err)
	}

	h.logger.Info("shipment delivered ✅", "orderId", orderID)
	return nil
}
