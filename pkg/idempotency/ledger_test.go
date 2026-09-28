package idempotency_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ordersys/pkg/idempotency"
)

type mockRow struct {
	exists bool
	err    error
}

func (m *mockRow) Scan(dest ...any) error {
	if m.err != nil {
		return m.err
	}
	if !m.exists {
		return pgx.ErrNoRows
	}
	if len(dest) > 0 {
		if ptr, ok := dest[0].(*int); ok {
			*ptr = 1
		}
	}
	return nil
}

type mockDBTX struct {
	processed map[string]string
}

func newMockDBTX() *mockDBTX {
	return &mockDBTX{processed: make(map[string]string)}
}

func (m *mockDBTX) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if len(arguments) >= 2 {
		id, _ := arguments[0].(string)
		typ, _ := arguments[1].(string)
		if _, exists := m.processed[id]; exists {
			return pgconn.NewCommandTag("INSERT 0 0"), nil
		}
		m.processed[id] = typ
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	}
	return pgconn.NewCommandTag("INSERT 0 0"), nil
}

func (m *mockDBTX) QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row {
	if len(arguments) >= 1 {
		id, _ := arguments[0].(string)
		_, exists := m.processed[id]
		return &mockRow{exists: exists}
	}
	return &mockRow{exists: false}
}

func TestLedger_IsProcessedAndTryMark(t *testing.T) {
	db := newMockDBTX()
	ctx := context.Background()

	processed, err := idempotency.IsProcessed(ctx, db, "msg-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processed {
		t.Error("expected msg-1 to not be processed yet")
	}

	inserted, err := idempotency.TryMarkProcessed(ctx, db, "msg-1", "order.created")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !inserted {
		t.Error("expected msg-1 to be marked as processed")
	}

	// Second attempt should report duplicate
	insertedSecond, err := idempotency.TryMarkProcessed(ctx, db, "msg-1", "order.created")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if insertedSecond {
		t.Error("expected duplicate message to not be inserted")
	}

	// IsProcessed should now return true
	processedAfter, err := idempotency.IsProcessed(ctx, db, "msg-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !processedAfter {
		t.Error("expected msg-1 to be processed")
	}
}
