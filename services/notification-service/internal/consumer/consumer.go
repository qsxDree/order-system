package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/events"
	"github.com/ordersys/pkg/idempotency"
	"github.com/ordersys/pkg/metrics"
	"github.com/ordersys/services/notification-service/internal/db"
)

type NotificationConsumer struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

func NewNotificationConsumer(pool *pgxpool.Pool, logger *slog.Logger) *NotificationConsumer {
	return &NotificationConsumer{
		pool:   pool,
		logger: logger,
	}
}

type orderPayloadHelper struct {
	OrderID string `json:"orderId"`
}

func (c *NotificationConsumer) HandleMessage(ctx context.Context, raw *events.RawEnvelope) error {
	var helper orderPayloadHelper
	if err := json.Unmarshal(raw.Data, &helper); err != nil {
		return fmt.Errorf("failed to extract orderId from event data: %w", err)
	}

	tmpl := GetTemplate(raw.Type)

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	newMsg, err := idempotency.TryMarkProcessed(ctx, tx, raw.MessageID, raw.Type)
	if err != nil {
		return fmt.Errorf("idempotency check failed: %w", err)
	}
	if !newMsg {
		c.logger.DebugContext(ctx, "duplicate notification event ignored", "messageId", raw.MessageID)
		return nil
	}

	queries := db.New(tx)
	_, err = queries.CreateNotification(ctx, db.CreateNotificationParams{
		OrderID:  pgtype.Text{String: helper.OrderID, Valid: helper.OrderID != ""},
		Channel:  tmpl.Channel,
		Template: tmpl.Template,
	})
	if err != nil {
		return fmt.Errorf("failed to create notification: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit notification: %w", err)
	}

	metrics.NotificationsSentTotal.WithLabelValues(raw.Type, tmpl.Channel).Inc()
	c.logger.InfoContext(ctx, "sent notification",
		"eventType", raw.Type,
		"channel", tmpl.Channel,
		"orderId", helper.OrderID,
	)

	return nil
}
