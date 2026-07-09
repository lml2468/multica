-- =====================
-- Webhook subscriptions (outbound)
-- =====================
-- See migration 150. project_id IS NULL => workspace-level; otherwise
-- project-level. All reads/writes are scoped by workspace_id for multi-tenancy.

-- name: ListWebhookSubscriptionsByWorkspace :many
-- Workspace-level listing for the workspace settings UI: workspace-level
-- subscriptions only (project_id IS NULL).
SELECT * FROM webhook_subscription
WHERE workspace_id = $1 AND project_id IS NULL
ORDER BY created_at DESC;

-- name: ListWebhookSubscriptionsByProject :many
-- Project-level listing for the project settings UI.
SELECT * FROM webhook_subscription
WHERE workspace_id = $1 AND project_id = $2
ORDER BY created_at DESC;

-- name: ListEnabledWebhookSubscriptionsForDispatch :many
-- Dispatch hot path: every enabled subscription in the workspace. The
-- dispatcher filters workspace-level (project_id IS NULL) vs project-level
-- (project_id = issue's project) in memory against the event payload.
SELECT * FROM webhook_subscription
WHERE workspace_id = $1 AND enabled = true;

-- name: GetWebhookSubscriptionInWorkspace :one
SELECT * FROM webhook_subscription
WHERE id = $1 AND workspace_id = $2;

-- name: CreateWebhookSubscription :one
INSERT INTO webhook_subscription (
    workspace_id, project_id, url, secret, events, enabled
) VALUES (
    $1, sqlc.narg('project_id'), $2, $3, $4, $5
) RETURNING *;

-- name: UpdateWebhookSubscription :one
UPDATE webhook_subscription SET
    url = COALESCE(sqlc.narg('url'), url),
    events = COALESCE(sqlc.narg('events'), events),
    enabled = COALESCE(sqlc.narg('enabled'), enabled),
    -- Operator re-enabling a subscription clears the system-set disable marker
    -- and zeroes the failure counter, so the next failure window starts fresh
    -- rather than instantly tripping the auto-disable threshold again.
    disabled_reason = CASE
        WHEN sqlc.narg('enabled')::boolean IS TRUE THEN NULL
        ELSE disabled_reason
    END,
    consecutive_failures = CASE
        WHEN sqlc.narg('enabled')::boolean IS TRUE THEN 0
        ELSE consecutive_failures
    END,
    updated_at = now()
WHERE id = $1 AND workspace_id = $2
RETURNING *;

-- name: DeleteWebhookSubscription :exec
DELETE FROM webhook_subscription
WHERE id = $1 AND workspace_id = $2;

-- name: ResetWebhookSubscriptionFailures :exec
-- Called after a successful delivery. WHERE consecutive_failures > 0 keeps the
-- happy path (which always succeeds) from issuing a no-op UPDATE per delivery.
--
-- WHERE enabled = true is a tie-break for the disable-then-in-flight-success
-- race: if delivery A fails to trip the threshold (enabled→false) while
-- delivery B is still in flight and then succeeds, we must NOT reset the
-- counter on B's recovery — that would leave the row in the confusing state
-- enabled=false + disabled_reason='auto_disabled' + consecutive_failures=0
-- where the failure count contradicts the disable reason (Jerry-Xin review).
-- Preserving the count as-is keeps the audit trail honest ("we disabled at
-- exactly N failures"); operator re-enable will zero both via the update
-- query below.
--
-- We also bump updated_at so the row reflects the moment of recovery for
-- the UI/audit (the no-op guard above means this only fires on genuine
-- state change, so the per-row write rate is bounded by recovery events,
-- not delivery volume).
UPDATE webhook_subscription
SET consecutive_failures = 0,
    updated_at = now()
WHERE id = $1 AND consecutive_failures > 0 AND enabled = true;

-- name: IncrementWebhookSubscriptionFailuresAndMaybeDisable :one
-- Called after a terminal failed delivery. Increments the counter and, if the
-- new value crosses @threshold (set 0 to disable auto-disable), flips enabled
-- to false in the same UPDATE so we never publish an interim enabled+counter
-- state another transaction could race against.
--
-- Returns the post-update enabled flag + counter so the caller can log "the
-- threshold tripped" without an extra SELECT.
UPDATE webhook_subscription
SET
    consecutive_failures = consecutive_failures + 1,
    enabled = CASE
        WHEN @threshold::int > 0 AND consecutive_failures + 1 >= @threshold::int
            THEN false
        ELSE enabled
    END,
    disabled_reason = CASE
        WHEN @threshold::int > 0 AND consecutive_failures + 1 >= @threshold::int AND enabled
            THEN 'auto_disabled_failure_threshold'
        ELSE disabled_reason
    END,
    updated_at = now()
WHERE id = $1
RETURNING enabled, consecutive_failures, disabled_reason;
