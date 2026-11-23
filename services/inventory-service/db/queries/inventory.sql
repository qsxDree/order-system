-- name: ListProductsWithInventory :many
SELECT 
    p.id,
    p.sku,
    p.name,
    p.price,
    COALESCE(i.available, 0) AS available,
    COALESCE(i.reserved, 0) AS reserved
FROM products p
LEFT JOIN inventory i ON p.id = i.product_id
ORDER BY p.sku ASC;

-- name: UpsertProduct :one
INSERT INTO products (id, sku, name, price)
VALUES ($1, $2, $3, $4)
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name, price = EXCLUDED.price
RETURNING *;

-- name: GetInventoryForUpdate :one
SELECT i.*, p.sku, p.price FROM inventory i
JOIN products p ON p.id = i.product_id
WHERE i.product_id = $1 LIMIT 1 FOR UPDATE OF i;

-- name: SeedInventory :exec
INSERT INTO inventory (product_id, available, reserved)
VALUES ($1, $2, $3)
ON CONFLICT (product_id) DO NOTHING;

-- name: ReserveStock :one
UPDATE inventory
SET available = available - $1, reserved = reserved + $1
WHERE product_id = $2 AND available >= $1
RETURNING *;

-- name: CreateReservation :one
INSERT INTO reservations (id, order_id, product_id, quantity, status, created_at)
VALUES ($1, $2, $3, $4, $5, NOW())
RETURNING *;

-- name: ListHeldReservations :many
SELECT * FROM reservations
WHERE order_id = $1 AND status = 'HELD'
ORDER BY product_id FOR UPDATE;

-- name: ReleaseReservation :one
UPDATE reservations
SET status = 'RELEASED'
WHERE id = $1
RETURNING *;

-- name: RestoreStock :one
UPDATE inventory
SET available = available + $1, reserved = reserved - $1
WHERE product_id = $2
RETURNING *;

-- name: LockOrder :exec
SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0));

-- name: IsOrderCancelled :one
SELECT EXISTS(SELECT 1 FROM order_cancellations WHERE order_id = $1);

-- name: MarkOrderCancelled :exec
INSERT INTO order_cancellations (order_id) VALUES ($1)
ON CONFLICT (order_id) DO NOTHING;
