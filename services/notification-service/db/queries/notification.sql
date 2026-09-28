-- name: CreateNotification :one
INSERT INTO notifications (
    order_id, channel, template, status, sent_at
) VALUES (
    $1, $2, $3, 'SENT', NOW()
)
RETURNING *;

-- name: ListNotificationsByOrderID :many
SELECT * FROM notifications
WHERE order_id = $1
ORDER BY sent_at DESC;
