package logger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/ordersys/pkg/logger"
)

func TestLogger_CorrelationIDPropagation(t *testing.T) {
	buf := &bytes.Buffer{}
	log := logger.NewLogger("test-service", "debug", buf)

	ctx := context.Background()
	ctx = logger.WithCorrelationID(ctx, "corr-xyz-123")

	log.InfoContext(ctx, "processing order", "orderId", "o-456")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse log JSON: %v, raw: %s", err, buf.String())
	}

	if entry["service"] != "test-service" {
		t.Errorf("expected service=test-service, got %v", entry["service"])
	}
	if entry["correlationId"] != "corr-xyz-123" {
		t.Errorf("expected correlationId=corr-xyz-123, got %v", entry["correlationId"])
	}
	if entry["orderId"] != "o-456" {
		t.Errorf("expected orderId=o-456, got %v", entry["orderId"])
	}
	if entry["msg"] != "processing order" {
		t.Errorf("expected msg='processing order', got %v", entry["msg"])
	}
}
