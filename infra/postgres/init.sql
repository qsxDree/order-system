-- One PostgreSQL instance, one database per service (no shared tables — services
-- may only reach each other's data via events, never via cross-database queries).
CREATE DATABASE order_db;
CREATE DATABASE inventory_db;
CREATE DATABASE payment_db;
CREATE DATABASE shipping_db;
CREATE DATABASE notification_db;

\connect order_db;

CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    customer_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    total_amount NUMERIC(12, 2) NOT NULL,
    currency TEXT NOT NULL DEFAULT 'USD',
    address JSONB,
    inventory_reserved BOOLEAN NOT NULL DEFAULT FALSE,
    payment_succeeded BOOLEAN NOT NULL DEFAULT FALSE,
    cancellation_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

CREATE TABLE IF NOT EXISTS order_items (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    order_id TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    quantity INT NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);

CREATE TABLE IF NOT EXISTS order_events (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    order_id TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    producer TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events(order_id);

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

CREATE TABLE IF NOT EXISTS processed_messages (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

\connect inventory_db;

CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    sku TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    price NUMERIC(12, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory (
    product_id TEXT PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
    available INT NOT NULL DEFAULT 0,
    reserved INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS reservations (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    order_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INT NOT NULL,
    status TEXT NOT NULL DEFAULT 'HELD',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reservations_order_id ON reservations(order_id);

CREATE TABLE IF NOT EXISTS processed_messages (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

CREATE TABLE IF NOT EXISTS order_cancellations (
    order_id TEXT PRIMARY KEY
);

\connect payment_db;

CREATE TABLE IF NOT EXISTS payments (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    order_id TEXT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    provider_ref TEXT,
    failure_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id);

CREATE TABLE IF NOT EXISTS processed_messages (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

CREATE TABLE IF NOT EXISTS order_cancellations (
    order_id TEXT PRIMARY KEY
);

\connect shipping_db;

CREATE TABLE IF NOT EXISTS shipments (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    order_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'CREATED',
    tracking_number TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_messages (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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


\connect notification_db;

CREATE TABLE IF NOT EXISTS notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id TEXT,
    channel TEXT NOT NULL,
    template TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'SENT',
    sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_messages (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
