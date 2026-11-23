package saga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/idempotency"
	"github.com/ordersys/pkg/outbox"
	"github.com/ordersys/services/order-service/internal/db"
)

type Coordinator struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

func NewCoordinator(pool *pgxpool.Pool, logger *slog.Logger) *Coordinator {
	return &Coordinator{
		pool:   pool,
		logger: logger,
	}
}

func (s *Coordinator) HandleMessage(ctx context.Context, raw *events.RawEnvelope) error {
	switch raw.Type {
	case events.TypeInventoryReserved:
		return s.onInventoryReserved(ctx, raw)
	case events.TypePaymentSucceeded:
		return s.onPaymentSucceeded(ctx, raw)
	case events.TypeInventoryRejected:
		return s.onInventoryRejected(ctx, raw)
	case events.TypePaymentFailed:
		return s.onPaymentFailed(ctx, raw)
	case events.TypeShipmentDispatched:
		return s.onShipmentDispatched(ctx, raw)
	case events.TypeShipmentDelivered:
		return s.onShipmentDelivered(ctx, raw)
	default:
		s.logger.DebugContext(ctx, "ignoring unhandled event in saga coordinator", "type", raw.Type)
		return nil
	}
}

func (s *Coordinator) recordEvent(ctx context.Context, tx db.DBTX, raw *events.RawEnvelope, orderID string) error {
	queries := db.New(tx)
	_, err := queries.CreateOrderEvent(ctx, db.CreateOrderEventParams{
		ID:       uuid.NewString(),
		OrderID:  orderID,
		Type:     raw.Type,
		Producer: raw.Producer,
		Payload:  raw.Data,
	})
	return err
}

func (s *Coordinator) onInventoryReserved(ctx context.Context, raw *events.RawEnvelope) error {
	parsed, err := events.UnmarshalPayload(raw.Type, raw.Data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal inventory.reserved: %w", err)
	}
	payload := parsed.(events.InventoryReservedPayload)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	newMsg, err := idempotency.TryMarkProcessed(ctx, tx, raw.MessageID, raw.Type)
	if err != nil {
		return err
	}
	if !newMsg {
		return nil
	}

	if err := s.recordEvent(ctx, tx, raw, payload.OrderID); err != nil {
		return err
	}

	queries := db.New(tx)
	_, err = queries.UpdateOrderInventoryReserved(ctx, payload.OrderID)
	if err != nil {
		return fmt.Errorf("failed to update order inventory_reserved: %w", err)
	}

	if err := s.maybeConfirm(ctx, tx, payload.OrderID, raw.CorrelationID, raw.MessageID); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Coordinator) onPaymentSucceeded(ctx context.Context, raw *events.RawEnvelope) error {
	parsed, err := events.UnmarshalPayload(raw.Type, raw.Data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal payment.succeeded: %w", err)
	}
	payload := parsed.(events.PaymentSucceededPayload)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	newMsg, err := idempotency.TryMarkProcessed(ctx, tx, raw.MessageID, raw.Type)
	if err != nil {
		return err
	}
	if !newMsg {
		return nil
	}

	if err := s.recordEvent(ctx, tx, raw, payload.OrderID); err != nil {
		return err
	}

	queries := db.New(tx)
	_, err = queries.UpdateOrderPaymentSucceeded(ctx, payload.OrderID)
	if err != nil {
		return fmt.Errorf("failed to update order payment_succeeded: %w", err)
	}

	if err := s.maybeConfirm(ctx, tx, payload.OrderID, raw.CorrelationID, raw.MessageID); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Coordinator) maybeConfirm(ctx context.Context, tx pgx.Tx, orderID, correlationID, causationID string) error {
	queries := db.New(tx)
	confirmed, err := queries.ConfirmOrder(ctx, orderID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to confirm order: %w", err)
	}

	confirmPayload := events.OrderConfirmedPayload{OrderID: orderID}
	payloadBytes, _ := json.Marshal(confirmPayload)

	_, err = queries.CreateOrderEvent(ctx, db.CreateOrderEventParams{
		ID:       uuid.NewString(),
		OrderID:  orderID,
		Type:     events.TypeOrderConfirmed,
		Producer: "order-service",
		Payload:  payloadBytes,
	})
	if err != nil {
		return fmt.Errorf("failed to record confirmed order event: %w", err)
	}

	err = outbox.Enqueue(ctx, tx, orderID, events.TypeOrderConfirmed, correlationID, causationID, confirmPayload)
	if err != nil {
		return fmt.Errorf("failed to enqueue order.confirmed outbox: %w", err)
	}

	s.logger.InfoContext(ctx, "order confirmed ✅", "orderId", confirmed.ID)
	return nil
}

func (s *Coordinator) onInventoryRejected(ctx context.Context, raw *events.RawEnvelope) error {
	parsed, err := events.UnmarshalPayload(raw.Type, raw.Data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal inventory.rejected: %w", err)
	}
	payload := parsed.(events.InventoryRejectedPayload)

	return s.cancelOrder(ctx, raw, payload.OrderID, fmt.Sprintf("inventory rejected: %s", payload.Reason))
}

func (s *Coordinator) onPaymentFailed(ctx context.Context, raw *events.RawEnvelope) error {
	parsed, err := events.UnmarshalPayload(raw.Type, raw.Data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal payment.failed: %w", err)
	}
	payload := parsed.(events.PaymentFailedPayload)

	return s.cancelOrder(ctx, raw, payload.OrderID, fmt.Sprintf("payment failed: %s", payload.Reason))
}

func (s *Coordinator) cancelOrder(ctx context.Context, raw *events.RawEnvelope, orderID, reason string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	newMsg, err := idempotency.TryMarkProcessed(ctx, tx, raw.MessageID, raw.Type)
	if err != nil {
		return err
	}
	if !newMsg {
		return nil
	}

	if err := s.recordEvent(ctx, tx, raw, orderID); err != nil {
		return err
	}

	queries := db.New(tx)
	cancelled, err := queries.CancelOrder(ctx, db.CancelOrderParams{
		ID:                 orderID,
		CancellationReason: pgtype.Text{String: reason, Valid: true},
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return fmt.Errorf("failed to cancel order: %w", err)
	}

	cancelPayload := events.OrderCancelledPayload{OrderID: orderID, Reason: reason}
	payloadBytes, _ := json.Marshal(cancelPayload)

	_, err = queries.CreateOrderEvent(ctx, db.CreateOrderEventParams{
		ID:       uuid.NewString(),
		OrderID:  orderID,
		Type:     events.TypeOrderCancelled,
		Producer: "order-service",
		Payload:  payloadBytes,
	})
	if err != nil {
		return fmt.Errorf("failed to record cancelled order event: %w", err)
	}

	err = outbox.Enqueue(ctx, tx, orderID, events.TypeOrderCancelled, raw.CorrelationID, raw.MessageID, cancelPayload)
	if err != nil {
		return fmt.Errorf("failed to enqueue order.cancelled outbox: %w", err)
	}

	s.logger.WarnContext(ctx, "order cancelled — compensating ↩️", "orderId", cancelled.ID, "reason", reason)
	return tx.Commit(ctx)
}

func (s *Coordinator) onShipmentDispatched(ctx context.Context, raw *events.RawEnvelope) error {
	parsed, err := events.UnmarshalPayload(raw.Type, raw.Data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal shipment.dispatched: %w", err)
	}
	payload := parsed.(events.ShipmentDispatchedPayload)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	newMsg, err := idempotency.TryMarkProcessed(ctx, tx, raw.MessageID, raw.Type)
	if err != nil {
		return err
	}
	if !newMsg {
		return nil
	}

	if err := s.recordEvent(ctx, tx, raw, payload.OrderID); err != nil {
		return err
	}

	queries := db.New(tx)
	if err := queries.MarkOrderShipped(ctx, payload.OrderID); err != nil {
		return fmt.Errorf("failed to update order status: %w", err)
	}

	return tx.Commit(ctx)
}

func (s *Coordinator) onShipmentDelivered(ctx context.Context, raw *events.RawEnvelope) error {
	parsed, err := events.UnmarshalPayload(raw.Type, raw.Data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal shipment.delivered: %w", err)
	}
	payload := parsed.(events.ShipmentDeliveredPayload)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	newMsg, err := idempotency.TryMarkProcessed(ctx, tx, raw.MessageID, raw.Type)
	if err != nil {
		return err
	}
	if !newMsg {
		return nil
	}

	if err := s.recordEvent(ctx, tx, raw, payload.OrderID); err != nil {
		return err
	}

	queries := db.New(tx)
	if err := queries.MarkOrderDelivered(ctx, payload.OrderID); err != nil {
		return fmt.Errorf("failed to update order status: %w", err)
	}

	return tx.Commit(ctx)
}
