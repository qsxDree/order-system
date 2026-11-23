package events

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// EventEnvelope standard message envelope wrapping every event on the bus.
// Matches the wire protocol defined in the TypeScript EventEnvelope.
type EventEnvelope[T any] struct {
	MessageID     string `json:"messageId"`
	CorrelationID string `json:"correlationId"`
	CausationID   string `json:"causationId,omitempty"`
	Type          string `json:"type"`
	Version       int    `json:"version"`
	OccurredAt    string `json:"occurredAt"` // RFC3339 / ISO-8601
	Producer      string `json:"producer"`
	Data          T      `json:"data"`
}

// RawEnvelope is used for unmarshaling messages when the payload type is determined dynamically.
type RawEnvelope struct {
	MessageID     string          `json:"messageId"`
	CorrelationID string          `json:"correlationId"`
	CausationID   string          `json:"causationId,omitempty"`
	Type          string          `json:"type"`
	Version       int             `json:"version"`
	OccurredAt    string          `json:"occurredAt"`
	Producer      string          `json:"producer"`
	Data          json.RawMessage `json:"data"`
}

// EnvelopeOptions provides optional parameters when building an event envelope.
type EnvelopeOptions struct {
	MessageID     string
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
	Version       int
}

// NewEnvelope constructs a typed EventEnvelope with sensible defaults.
func NewEnvelope[T any](producer string, eventType string, data T, opts ...EnvelopeOptions) EventEnvelope[T] {
	var opt EnvelopeOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	msgID := opt.MessageID
	if msgID == "" {
		msgID = uuid.NewString()
	}

	corrID := opt.CorrelationID
	if corrID == "" {
		corrID = uuid.NewString()
	}

	occAt := opt.OccurredAt
	if occAt.IsZero() {
		occAt = time.Now().UTC()
	}

	version := opt.Version
	if version <= 0 {
		version = 1
	}

	return EventEnvelope[T]{
		MessageID:     msgID,
		CorrelationID: corrID,
		CausationID:   opt.CausationID,
		Type:          eventType,
		Version:       version,
		OccurredAt:    occAt.Format(time.RFC3339Nano),
		Producer:      producer,
		Data:          data,
	}
}

// ParseRaw parses raw bytes into a RawEnvelope.
func ParseRaw(data []byte) (*RawEnvelope, error) {
	var env RawEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	if strings.TrimSpace(env.MessageID) == "" || strings.TrimSpace(env.CorrelationID) == "" ||
		strings.TrimSpace(env.Type) == "" || strings.TrimSpace(env.Producer) == "" || env.Version != 1 {
		return nil, fmt.Errorf("invalid event envelope: identity, type, producer and version 1 are required")
	}
	if _, err := time.Parse(time.RFC3339Nano, env.OccurredAt); err != nil {
		return nil, fmt.Errorf("invalid event timestamp: %w", err)
	}
	if len(env.Data) == 0 || string(env.Data) == "null" || env.Data[0] != '{' {
		return nil, fmt.Errorf("event data must be a JSON object")
	}
	return &env, nil
}
