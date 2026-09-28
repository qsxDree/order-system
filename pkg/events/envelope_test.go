package events_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ordersys/pkg/events"
)

type sampleData struct {
	Foo string `json:"foo"`
	Bar int    `json:"bar"`
}

func TestEventEnvelope_JSONRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	env := events.NewEnvelope("test-producer", "test.event", sampleData{Foo: "hello", Bar: 42}, events.EnvelopeOptions{
		MessageID:     "msg-123",
		CorrelationID: "corr-456",
		CausationID:   "caus-789",
		OccurredAt:    now,
		Version:       1,
	})

	bytes, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("failed to marshal envelope: %v", err)
	}

	var raw events.RawEnvelope
	if err := json.Unmarshal(bytes, &raw); err != nil {
		t.Fatalf("failed to unmarshal raw envelope: %v", err)
	}

	if raw.MessageID != "msg-123" {
		t.Errorf("expected MessageID msg-123, got %s", raw.MessageID)
	}
	if raw.CorrelationID != "corr-456" {
		t.Errorf("expected CorrelationID corr-456, got %s", raw.CorrelationID)
	}
	if raw.CausationID != "caus-789" {
		t.Errorf("expected CausationID caus-789, got %s", raw.CausationID)
	}
	if raw.Type != "test.event" {
		t.Errorf("expected Type test.event, got %s", raw.Type)
	}
	if raw.Version != 1 {
		t.Errorf("expected Version 1, got %d", raw.Version)
	}
	if raw.Producer != "test-producer" {
		t.Errorf("expected Producer test-producer, got %s", raw.Producer)
	}

	var data sampleData
	if err := json.Unmarshal(raw.Data, &data); err != nil {
		t.Fatalf("failed to unmarshal data: %v", err)
	}
	if data.Foo != "hello" || data.Bar != 42 {
		t.Errorf("unexpected data content: %+v", data)
	}
}
