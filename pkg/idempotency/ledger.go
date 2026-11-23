package idempotency

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// DBTX represents a database executor compatible with pgx pool, connection, and transactions.
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// TryMarkProcessed attempts to atomically insert the message into processed_messages.
// Returns true if the message was new (inserted), false if it was already processed (duplicate).
func TryMarkProcessed(ctx context.Context, db DBTX, messageID string, eventType string) (bool, error) {
	if messageID == "" || eventType == "" {
		return false, fmt.Errorf("message ID and event type are required")
	}
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
