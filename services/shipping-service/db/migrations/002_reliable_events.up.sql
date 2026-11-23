CREATE TABLE IF NOT EXISTS outbox (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    aggregate_id TEXT NOT NULL,
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id TEXT,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_published_created_at ON outbox(published, created_at);

