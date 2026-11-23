package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/idempotency"
	"github.com/ordersys/pkg/outbox"
	"github.com/ordersys/services/payment-service/internal/db"
)

type PaymentHandler struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

func NewPaymentHandler(pool *pgxpool.Pool, logger *slog.Logger) *PaymentHandler {
	return &PaymentHandler{
		pool:   pool,
		logger: logger,
	}
}

func (h *PaymentHandler) HandleMessage(ctx context.Context, raw *events.RawEnvelope) error {
	switch raw.Type {
	case events.TypeOrderCreated:
		return h.onOrderCreated(ctx, raw)
	case events.TypeOrderCancelled:
		return h.onOrderCancelled(ctx, raw)
	default:
		h.logger.DebugContext(ctx, "ignoring unhandled event type in payment service", "type", raw.Type)
		return nil
	}
}

func (h *PaymentHandler) onOrderCreated(ctx context.Context, raw *events.RawEnvelope) error {
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

	var amount pgtype.Numeric
	if err := amount.Scan(fmt.Sprintf("%.2f", payload.TotalAmount)); err != nil {
		return fmt.Errorf("invalid payment amount: %w", err)
	}
	payment, err := queries.CreatePayment(ctx, db.CreatePaymentParams{
		ID: uuid.NewString(), OrderID: payload.OrderID, Amount: amount,
	})
	if err != nil {
		return fmt.Errorf("failed to create payment: %w", err)
	}

	status := db.UpdatePaymentStatusParams{
		ID: payment.ID, Status: "SUCCEEDED",
		ProviderRef: pgtype.Text{String: "ch_" + uuid.NewString()[:12], Valid: true},
	}
	eventType := events.TypePaymentSucceeded
	var result any = events.PaymentSucceededPayload{
		OrderID: payload.OrderID, PaymentID: payment.ID, Amount: payload.TotalAmount,
	}
	if payload.Simulate != nil && payload.Simulate.PaymentFailure {
		status.Status = "FAILED"
		status.ProviderRef = pgtype.Text{}
		status.FailureCode = pgtype.Text{String: "card_declined", Valid: true}
		eventType = events.TypePaymentFailed
		result = events.PaymentFailedPayload{OrderID: payload.OrderID, Reason: "card declined (demo toggle)"}
	}
	if _, err := queries.UpdatePaymentStatus(ctx, status); err != nil {
		return fmt.Errorf("failed to finalize payment: %w", err)
	}
	if err := outbox.Enqueue(ctx, tx, payload.OrderID, eventType,
		raw.CorrelationID, raw.MessageID, result); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (h *PaymentHandler) onOrderCancelled(ctx context.Context, raw *events.RawEnvelope) error {
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
	refunded, err := queries.RefundPaymentByOrderID(ctx, payload.OrderID)
	if err != nil {
		return fmt.Errorf("failed to refund payment: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit refund: %w", err)
	}

	if len(refunded) > 0 {
		h.logger.InfoContext(ctx, "payment refunded (compensation)", "orderId", payload.OrderID, "refundedCount", len(refunded))
	}
	return nil
}
