package idempotency_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ordersys/pkg/idempotency"
)

type mockDBTX struct {
	processed bool
	err       error
}

func (m *mockDBTX) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	if m.err != nil {
		return pgconn.CommandTag{}, m.err
	}
	if m.processed {
		return pgconn.NewCommandTag("INSERT 0 0"), nil
	}
	m.processed = true
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func TestTryMarkProcessed(t *testing.T) {
	db := &mockDBTX{}
	for _, want := range []bool{true, false} {
		got, err := idempotency.TryMarkProcessed(t.Context(), db, "msg-1", "order.created")
		if err != nil || got != want {
			t.Fatalf("got (%v, %v), want (%v, nil)", got, err, want)
		}
	}
	db.err = errors.New("database unavailable")
	if _, err := idempotency.TryMarkProcessed(t.Context(), db, "msg-2", "order.created"); !errors.Is(err, db.err) {
		t.Fatalf("lost database error: %v", err)
	}
	if _, err := idempotency.TryMarkProcessed(t.Context(), &mockDBTX{}, "", "order.created"); err == nil {
		t.Fatal("accepted empty message ID")
	}
}
