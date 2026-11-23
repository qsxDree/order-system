-- name: CreateOrder :one
INSERT INTO orders (
    id, customer_id, status, total_amount, currency, address,
    inventory_reserved, payment_succeeded, cancellation_reason, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, NOW(), NOW()
)
RETURNING *;

-- name: CreateOrderItem :one
INSERT INTO order_items (
    id, order_id, product_id, sku, quantity, unit_price
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING *;

-- name: CreateOrderEvent :one
INSERT INTO order_events (
    id, order_id, type, producer, payload, created_at
) VALUES (
    $1, $2, $3, $4, $5, NOW()
)
RETURNING *;

-- name: GetOrder :one
SELECT * FROM orders WHERE id = $1 LIMIT 1;

-- name: ListOrderItems :many
SELECT * FROM order_items WHERE order_id = $1;

-- name: ListOrderEvents :many
SELECT * FROM order_events WHERE order_id = $1 ORDER BY created_at ASC;

-- name: ListOrders :many
SELECT * FROM orders
WHERE ($1::text = '' OR status = $1)
ORDER BY created_at DESC
LIMIT $2;

-- name: UpdateOrderInventoryReserved :one
UPDATE orders
SET inventory_reserved = true, updated_at = NOW()
WHERE id = $1
RETURNING *;

-- name: UpdateOrderPaymentSucceeded :one
UPDATE orders
SET payment_succeeded = true, updated_at = NOW()
WHERE id = $1
RETURNING *;

-- name: ConfirmOrder :one
UPDATE orders
SET status = 'CONFIRMED', updated_at = NOW()
WHERE id = $1 AND status = 'PENDING' AND inventory_reserved = true AND payment_succeeded = true
RETURNING *;

-- name: CancelOrder :one
UPDATE orders
SET status = 'CANCELLED', cancellation_reason = $2, updated_at = NOW()
WHERE id = $1 AND status = 'PENDING'
RETURNING *;

-- name: MarkOrderShipped :exec
UPDATE orders SET status = 'SHIPPED', updated_at = NOW()
WHERE id = $1 AND status = 'CONFIRMED';

-- name: MarkOrderDelivered :exec
UPDATE orders SET status = 'DELIVERED', updated_at = NOW()
WHERE id = $1 AND status IN ('CONFIRMED', 'SHIPPED');
