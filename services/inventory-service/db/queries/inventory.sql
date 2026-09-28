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

-- name: GetProduct :one
SELECT * FROM products WHERE id = $1 LIMIT 1;

-- name: GetProductBySKU :one
SELECT * FROM products WHERE sku = $1 LIMIT 1;

-- name: UpsertProduct :one
INSERT INTO products (id, sku, name, price)
VALUES ($1, $2, $3, $4)
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name, price = EXCLUDED.price
RETURNING *;

-- name: UpsertProductBySKU :one
INSERT INTO products (id, sku, name, price)
VALUES ($1, $2, $3, $4)
ON CONFLICT (sku) DO UPDATE
SET name = EXCLUDED.name, price = EXCLUDED.price
RETURNING *;

-- name: GetInventory :one
SELECT * FROM inventory WHERE product_id = $1 LIMIT 1;

-- name: GetInventoryForUpdate :one
SELECT * FROM inventory WHERE product_id = $1 LIMIT 1 FOR UPDATE;

-- name: UpsertInventory :one
INSERT INTO inventory (product_id, available, reserved)
VALUES ($1, $2, $3)
ON CONFLICT (product_id) DO UPDATE
SET available = EXCLUDED.available, reserved = EXCLUDED.reserved
RETURNING *;

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
WHERE order_id = $1 AND status = 'HELD';

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
