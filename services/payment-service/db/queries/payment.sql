-- name: CreatePayment :one
INSERT INTO payments (
    id, order_id, amount, status, created_at, updated_at
) VALUES (
    $1, $2, $3, 'PENDING', NOW(), NOW()
)
RETURNING *;

-- name: GetPayment :one
SELECT * FROM payments WHERE id = $1 LIMIT 1;

-- name: UpdatePaymentStatus :one
UPDATE payments
SET status = $2, provider_ref = $3, failure_code = $4, updated_at = NOW()
WHERE id = $1
RETURNING *;

-- name: RefundPaymentByOrderID :many
UPDATE payments
SET status = 'REFUNDED', updated_at = NOW()
WHERE order_id = $1 AND status = 'SUCCEEDED'
RETURNING *;
