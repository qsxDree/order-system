-- name: CreateNotification :one
INSERT INTO notifications (
    order_id, channel, template, status, sent_at
) VALUES (
    $1, $2, $3, 'SENT', NOW()
)
RETURNING *;
