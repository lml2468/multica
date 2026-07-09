-- =====================
-- Outbound webhook delivery history
-- =====================
-- See migration 151. One row per dispatcher deliver() call for an outbound
-- webhook subscription (migration 150). All reads are scoped by workspace_id
-- for multi-tenancy; the handler additionally scopes by subscription_id after
-- loading the subscription (owner/admin gated).

-- name: CreateOutboundWebhookDelivery :one
-- Written best-effort by the dispatcher at the terminal outcome of a delivery.
-- request_body / response_body / error / response_status / redelivered_from_id
-- are optional (a transport-error-only delivery has no response_status/body).
INSERT INTO outbound_webhook_delivery (
    workspace_id, subscription_id, event, status, attempt_count,
    response_status, request_body, response_body, error, redelivered_from_id
) VALUES (
    $1, $2, $3, $4, $5,
    sqlc.narg('response_status'), sqlc.narg('request_body'),
    sqlc.narg('response_body'), sqlc.narg('error'),
    sqlc.narg('redelivered_from_id')
) RETURNING *;

-- name: GetOutboundWebhookDeliveryInWorkspace :one
-- Workspace-scoped full read (includes bodies) for the detail + redeliver
-- endpoints. The handler verifies subscription_id ownership after loading.
SELECT * FROM outbound_webhook_delivery
WHERE id = $1 AND workspace_id = $2;

-- name: ListOutboundWebhookDeliveries :many
-- Per-subscription listing, newest first, paged by limit/offset. Workspace-
-- scoped defensively even though the caller already authorized the
-- subscription. Slim projection: request_body / response_body are deliberately
-- excluded — a page of 4 KiB+ bodies would be pulled from Postgres just to be
-- dropped in the JSON encoder. Detail views fetch the full row via
-- GetOutboundWebhookDeliveryInWorkspace.
SELECT
    id, workspace_id, subscription_id, event, status, attempt_count,
    response_status, error, redelivered_from_id, created_at
FROM outbound_webhook_delivery
WHERE subscription_id = $1 AND workspace_id = $2
ORDER BY created_at DESC
LIMIT $3 OFFSET $4;

-- name: CountOutboundWebhookDeliveries :one
-- Total for pagination. A separate count (not count(*) OVER() on the list) so
-- the total is correct even when an out-of-range offset returns zero rows, and
-- so Postgres can satisfy it with the (subscription_id, created_at) index.
SELECT count(*) FROM outbound_webhook_delivery
WHERE subscription_id = $1 AND workspace_id = $2;

-- name: PurgeOutboundWebhookDeliveriesOlderThan :exec
-- TTL cleanup: the scheduled OutboundWebhookDeliveryCleanupJob deletes rows
-- past the retention window so history does not grow unbounded. Idempotent.
DELETE FROM outbound_webhook_delivery
WHERE created_at < $1;
