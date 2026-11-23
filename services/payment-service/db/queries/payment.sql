-- name: CreatePayment :one
INSERT INTO payments (
    id, order_id, amount, status, created_at, updated_at
) VALUES (
    $1, $2, $3, 'PENDING', NOW(), NOW()
)
RETURNING *;

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

-- name: LockOrder :exec
SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0));

-- name: IsOrderCancelled :one
SELECT EXISTS(SELECT 1 FROM order_cancellations WHERE order_id = $1);

-- name: MarkOrderCancelled :exec
INSERT INTO order_cancellations (order_id) VALUES ($1)
ON CONFLICT (order_id) DO NOTHING;
