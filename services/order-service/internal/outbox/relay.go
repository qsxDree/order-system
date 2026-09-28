package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/bus"
	"github.com/ordersys/services/order-service/internal/db"
)

// Enqueue appends an event to the outbox table inside an existing transaction.
func Enqueue(ctx context.Context, tx db.DBTX, aggregateID, eventType, correlationID string, causationID string, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal outbox payload: %w", err)
	}

	queries := db.New(tx)
	caus := pgtype.Text{String: causationID, Valid: causationID != ""}

	_, err = queries.CreateOutbox(ctx, db.CreateOutboxParams{
		ID:            uuid.NewString(),
		AggregateID:   aggregateID,
		Type:          eventType,
		Payload:       payloadBytes,
		CorrelationID: correlationID,
		CausationID:   caus,
	})
	if err != nil {
		return fmt.Errorf("failed to enqueue outbox event: %w", err)
	}
	return nil
}

// Relay polls the outbox periodically and forwards unpublished events to RabbitMQ.
type Relay struct {
	pool    *pgxpool.Pool
	bus     *bus.MessageBus
	logger  *slog.Logger
	stopCh  chan struct{}
	wg      sync.WaitGroup
	running bool
	mu      sync.Mutex
}

func NewRelay(pool *pgxpool.Pool, b *bus.MessageBus, logger *slog.Logger) *Relay {
	return &Relay{
		pool:   pool,
		bus:    b,
		logger: logger,
		stopCh: make(chan struct{}),
	}
}

func (r *Relay) Start(interval time.Duration) {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}

	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return
	}
	r.running = true
	r.mu.Unlock()

	r.wg.Add(1)
	go r.loop(interval)
	r.logger.Info("outbox relay started", "interval", interval.String())
}

func (r *Relay) Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	close(r.stopCh)
	r.mu.Unlock()

	r.wg.Wait()
	r.logger.Info("outbox relay stopped")
}

func (r *Relay) loop(interval time.Duration) {
	defer r.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := r.flush(ctx); err != nil {
				r.logger.Error("outbox flush failed", "err", err)
			}
			cancel()
		}
	}
}

func (r *Relay) flush(ctx context.Context) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin outbox transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	queries := db.New(tx)
	rows, err := queries.PollUnpublishedOutbox(ctx, 50)
	if err != nil {
		return fmt.Errorf("failed to poll unpublished outbox: %w", err)
	}

	for _, row := range rows {
		var payload any
		if err := json.Unmarshal(row.Payload, &payload); err != nil {
			r.logger.Error("corrupt outbox payload, skipping", "id", row.ID, "err", err)
			continue
		}

		causationID := ""
		if row.CausationID.Valid {
			causationID = row.CausationID.String
		}

		opts := bus.PublishOptions{
			MessageID:     row.ID,
			CorrelationID: row.CorrelationID,
			CausationID:   causationID,
		}

		_, err = r.bus.Publish(ctx, row.Type, payload, opts)
		if err != nil {
			return fmt.Errorf("failed to publish outbox event %s: %w", row.ID, err)
		}

		if err := queries.MarkOutboxPublished(ctx, row.ID); err != nil {
			return fmt.Errorf("failed to mark outbox as published: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit outbox flush: %w", err)
	}

	if len(rows) > 0 {
		r.logger.Debug("outbox flushed", "count", len(rows))
	}
	return nil
}
