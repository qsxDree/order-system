-- name: UpsertShipment :one
INSERT INTO shipments (
    id, order_id, status, tracking_number, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, NOW(), NOW()
)
ON CONFLICT (order_id) DO UPDATE
SET status = EXCLUDED.status, updated_at = NOW()
RETURNING *;

-- name: GetShipmentByOrderID :one
SELECT * FROM shipments WHERE order_id = $1 LIMIT 1;

-- name: UpdateShipmentStatus :one
UPDATE shipments
SET status = $2, updated_at = NOW()
WHERE order_id = $1
RETURNING *;
