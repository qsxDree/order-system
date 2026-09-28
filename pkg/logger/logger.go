package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

type contextKey string

const (
	correlationIDKey contextKey = "correlationId"
)

// WithCorrelationID attaches a correlation ID to the context.
func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationIDKey, correlationID)
}

// GetCorrelationID retrieves the correlation ID from the context.
func GetCorrelationID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(correlationIDKey).(string); ok {
		return v
	}
	return ""
}

// ContextHandler wraps an slog.Handler to inject context values (like correlationId) into record attributes.
type ContextHandler struct {
	slog.Handler
}

// Handle extracts context values and appends them before delegating to the wrapped handler.
func (h *ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		if corrID := GetCorrelationID(ctx); corrID != "" {
			r.AddAttrs(slog.String("correlationId", corrID))
		}
	}
	return h.Handler.Handle(ctx, r)
}

// ParseLevel parses a string into an slog.Level.
func ParseLevel(levelStr string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(levelStr)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// NewLogger creates a new structured JSON logger for the given service name.
func NewLogger(service string, levelStr string, w ...io.Writer) *slog.Logger {
	var writer io.Writer = os.Stdout
	if len(w) > 0 && w[0] != nil {
		writer = w[0]
	}

	opts := &slog.HandlerOptions{
		Level: ParseLevel(levelStr),
	}

	baseHandler := slog.NewJSONHandler(writer, opts)
	ctxHandler := &ContextHandler{
		Handler: baseHandler.WithAttrs([]slog.Attr{
			slog.String("service", service),
		}),
	}

	return slog.New(ctxHandler)
}
