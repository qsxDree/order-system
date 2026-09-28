package idempotency

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DBTX represents a database executor compatible with pgx pool, connection, and transactions.
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row
}

// IsProcessed checks if the message ID has already been recorded in processed_messages.
func IsProcessed(ctx context.Context, db DBTX, messageID string) (bool, error) {
	const query = `SELECT 1 FROM processed_messages WHERE id = $1 LIMIT 1`
	var exists int
	err := db.QueryRow(ctx, query, messageID).Scan(&exists)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to query processed_messages: %w", err)
	}
	return true, nil
}

// MarkProcessed inserts a message ID and its event type into processed_messages.
func MarkProcessed(ctx context.Context, db DBTX, messageID string, eventType string) error {
	const query = `
		INSERT INTO processed_messages (id, type, processed_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (id) DO NOTHING
	`
	_, err := db.Exec(ctx, query, messageID, eventType)
	if err != nil {
		return fmt.Errorf("failed to mark message as processed: %w", err)
	}
	return nil
}

// TryMarkProcessed attempts to atomically insert the message into processed_messages.
// Returns true if the message was new (inserted), false if it was already processed (duplicate).
func TryMarkProcessed(ctx context.Context, db DBTX, messageID string, eventType string) (bool, error) {
	const query = `
		INSERT INTO processed_messages (id, type, processed_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (id) DO NOTHING
	`
	tag, err := db.Exec(ctx, query, messageID, eventType)
	if err != nil {
		return false, fmt.Errorf("failed to atomically record processed message: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
