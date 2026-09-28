package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/bus"
	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/idempotency"
	"github.com/ordersys/services/payment-service/internal/db"
)

type PaymentHandler struct {
	pool   *pgxpool.Pool
	bus    *bus.MessageBus
	logger *slog.Logger
}

func NewPaymentHandler(pool *pgxpool.Pool, b *bus.MessageBus, logger *slog.Logger) *PaymentHandler {
	return &PaymentHandler{
		pool:   pool,
		bus:    b,
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
	paymentID := uuid.NewString()

	var amtNum pgtype.Numeric
	_ = amtNum.Scan(fmt.Sprintf("%.2f", payload.TotalAmount))

	payment, err := queries.CreatePayment(ctx, db.CreatePaymentParams{
		ID:      paymentID,
		OrderID: payload.OrderID,
		Amount:  amtNum,
	})
	if err != nil {
		return fmt.Errorf("failed to create pending payment: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit initial payment record: %w", err)
	}

	// Simulate provider latency (150ms)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(150 * time.Millisecond):
	}

	opts := bus.PublishOptions{
		CorrelationID: raw.CorrelationID,
		CausationID:   raw.MessageID,
	}

	queriesPool := db.New(h.pool)

	// Simulated failure
	if payload.Simulate != nil && payload.Simulate.PaymentFailure {
		_, err := queriesPool.UpdatePaymentStatus(ctx, db.UpdatePaymentStatusParams{
			ID:          payment.ID,
			Status:      "FAILED",
			ProviderRef: pgtype.Text{Valid: false},
			FailureCode: pgtype.Text{String: "card_declined", Valid: true},
		})
		if err != nil {
			return fmt.Errorf("failed to update payment to FAILED: %w", err)
		}

		_, pubErr := h.bus.Publish(ctx, events.TypePaymentFailed, events.PaymentFailedPayload{
			OrderID: payload.OrderID,
			Reason:  "card declined (demo toggle)",
		}, opts)
		h.logger.WarnContext(ctx, "payment failed (simulated)", "orderId", payload.OrderID, "paymentId", payment.ID)
		return pubErr
	}

	// Succeeded
	providerRef := fmt.Sprintf("ch_%s", uuid.NewString()[:12])
	_, err = queriesPool.UpdatePaymentStatus(ctx, db.UpdatePaymentStatusParams{
		ID:          payment.ID,
		Status:      "SUCCEEDED",
		ProviderRef: pgtype.Text{String: providerRef, Valid: true},
		FailureCode: pgtype.Text{Valid: false},
	})
	if err != nil {
		return fmt.Errorf("failed to update payment to SUCCEEDED: %w", err)
	}

	_, pubErr := h.bus.Publish(ctx, events.TypePaymentSucceeded, events.PaymentSucceededPayload{
		OrderID:   payload.OrderID,
		PaymentID: payment.ID,
		Amount:    payload.TotalAmount,
	}, opts)
	if pubErr != nil {
		return fmt.Errorf("failed to publish payment.succeeded: %w", pubErr)
	}

	h.logger.InfoContext(ctx, "payment succeeded", "orderId", payload.OrderID, "paymentId", payment.ID, "amount", payload.TotalAmount)
	return nil
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
