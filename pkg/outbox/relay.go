package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/bus"
)

// Enqueue appends an event to the outbox table inside an existing transaction.
func Enqueue(ctx context.Context, tx pgx.Tx, aggregateID, eventType, correlationID string, causationID string, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal outbox payload: %w", err)
	}

	_, err = tx.Exec(ctx, `INSERT INTO outbox
		(id, aggregate_id, type, payload, correlation_id, causation_id)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		uuid.NewString(), aggregateID, eventType, payloadBytes, correlationID,
		pgtype.Text{String: causationID, Valid: causationID != ""})
	if err != nil {
		return fmt.Errorf("failed to enqueue outbox event: %w", err)
	}
	return nil
}

// Relay polls the outbox periodically and forwards unpublished events to RabbitMQ.
type Publisher interface {
	Publish(context.Context, string, any, ...bus.PublishOptions) (string, error)
}

type Relay struct {
	pool    *pgxpool.Pool
	bus     Publisher
	logger  *slog.Logger
	stopCh  chan struct{}
	wg      sync.WaitGroup
	running bool
	mu      sync.Mutex
}

func NewRelay(pool *pgxpool.Pool, b Publisher, logger *slog.Logger) *Relay {
	return &Relay{
		pool:   pool,
		bus:    b,
		logger: logger,
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
	r.stopCh = make(chan struct{})
	r.wg.Add(1)
	go r.loop(interval)
	r.mu.Unlock()
	r.logger.Info("outbox relay started", "interval", interval.String())
}

func (r *Relay) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.running {
		return
	}
	close(r.stopCh)

	r.wg.Wait()
	r.running = false
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

	rows, err := tx.Query(ctx, `SELECT id, type, payload, correlation_id, causation_id
		FROM outbox WHERE published = false ORDER BY created_at, id
		LIMIT 50 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return fmt.Errorf("failed to poll unpublished outbox: %w", err)
	}

	type event struct {
		id, eventType, correlationID string
		payload                      json.RawMessage
		causationID                  pgtype.Text
	}
	batch, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (event, error) {
		var e event
		err := row.Scan(&e.id, &e.eventType, &e.payload, &e.correlationID, &e.causationID)
		return e, err
	})
	if err != nil {
		return fmt.Errorf("failed to read outbox: %w", err)
	}

	for _, row := range batch {
		opts := bus.PublishOptions{
			MessageID:     row.id,
			CorrelationID: row.correlationID,
			CausationID:   row.causationID.String,
		}

		_, err = r.bus.Publish(ctx, row.eventType, row.payload, opts)
		if err != nil {
			return fmt.Errorf("failed to publish outbox event %s: %w", row.id, err)
		}

		if _, err := tx.Exec(ctx, `UPDATE outbox SET published = true WHERE id = $1`, row.id); err != nil {
			return fmt.Errorf("failed to mark outbox as published: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit outbox flush: %w", err)
	}

	if len(batch) > 0 {
		r.logger.Debug("outbox flushed", "count", len(batch))
	}
	return nil
}
