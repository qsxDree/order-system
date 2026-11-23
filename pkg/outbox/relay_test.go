package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/ordersys/pkg/bus"
	"github.com/ordersys/pkg/testutil"
)

type testPublisher struct {
	fail     bool
	ids      []string
	payloads []string
}

func (p *testPublisher) Publish(_ context.Context, _ string, data any, opts ...bus.PublishOptions) (string, error) {
	p.ids = append(p.ids, opts[0].MessageID)
	body, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	p.payloads = append(p.payloads, string(body))
	if p.fail {
		return "", errors.New("broker unavailable")
	}
	return opts[0].MessageID, nil
}

func TestRelayRetainsFailedEventsAndTheirIdentity(t *testing.T) {
	pool := testutil.NewDB(t, "order")
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	payload := json.RawMessage(`{"exact":9007199254740993}`)
	if err := Enqueue(t.Context(), tx, "o1", "test.event", "c1", "cause", payload); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	publisher := &testPublisher{fail: true}
	r := NewRelay(pool, publisher, slog.Default())
	if err := r.flush(t.Context()); err == nil {
		t.Fatal("expected broker failure")
	}
	var published bool
	if err := pool.QueryRow(t.Context(), `SELECT published FROM outbox`).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published {
		t.Fatal("marked an unconfirmed event published")
	}
	publisher.fail = false
	if err := r.flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(publisher.ids) != 2 || publisher.ids[0] != publisher.ids[1] || publisher.payloads[1] != string(payload) {
		t.Fatalf("changed event identity or numeric payload: %+v", publisher)
	}
	if err := pool.QueryRow(t.Context(), `SELECT published FROM outbox`).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if !published {
		t.Fatal("confirmed event left unpublished")
	}
	if err := r.flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(publisher.ids) != 2 {
		t.Fatal("republished completed event")
	}
}

func TestRelayCanStopAndRestart(t *testing.T) {
	r := NewRelay(nil, nil, slog.Default())
	for range 2 {
		r.Start(time.Hour)
		r.Start(time.Hour)
		r.Stop()
		r.Stop()
	}
}
