-- name: CreateShipment :one
INSERT INTO shipments (
    id, order_id, status, tracking_number, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, NOW(), NOW()
)
ON CONFLICT (order_id) DO NOTHING
RETURNING *;

-- name: UpdateShipmentStatus :one
UPDATE shipments
SET status = $2, updated_at = NOW()
WHERE order_id = $1
RETURNING *;

-- name: ListDueShipments :many
SELECT * FROM shipments
WHERE status = 'DISPATCHED' AND updated_at <= $1
ORDER BY updated_at, id LIMIT 50
FOR UPDATE SKIP LOCKED;
