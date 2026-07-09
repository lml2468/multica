-- Outbound webhook subscriptions: the reverse direction of the existing
-- (inbound) autopilot webhook ingress. A subscription is an external HTTP
-- endpoint that Multica POSTs to when a subscribed event happens — modeled on
-- GitHub's org/repo webhooks:
--
--   * project_id IS NULL  -> workspace-level webhook (GitHub "org" webhook):
--                            fires for every matching issue in the workspace.
--   * project_id IS NOT NULL -> project-level webhook (GitHub "repo" webhook):
--                            fires only for issues belonging to that project.
--
-- `secret` is the HMAC-SHA256 signing key, stored cleartext at rest. This
-- mirrors autopilot_trigger.signing_secret (migration 093): outbound signing
-- needs the cleartext to compute X-Multica-Signature-256, and there is no
-- general secrets-at-rest infrastructure to layer on yet (see MUL-2334).
--
-- `events` is a JSONB array of subscribed event types. v1 only emits
-- 'issue.status_changed'; the column is open-ended so new event types slot in
-- without a schema migration.
CREATE TABLE webhook_subscription (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id  UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    -- NULL = workspace-level. Composite-safe: a project always belongs to a
    -- workspace, so deleting the project (or workspace) cascades the row away.
    project_id    UUID REFERENCES project(id) ON DELETE CASCADE,
    url           TEXT NOT NULL,
    secret        TEXT NOT NULL,
    events        JSONB NOT NULL DEFAULT '["issue.status_changed"]'::jsonb,
    enabled       BOOLEAN NOT NULL DEFAULT true,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dispatch lookup: "all enabled subscriptions in this workspace" (the
-- dispatcher then filters workspace-level vs project-level in memory against
-- the issue's project_id). Partial on enabled keeps the hot path lean.
CREATE INDEX idx_webhook_subscription_ws
    ON webhook_subscription(workspace_id) WHERE enabled;

-- Project-scoped listing for the project settings UI.
CREATE INDEX idx_webhook_subscription_project
    ON webhook_subscription(project_id)
    WHERE project_id IS NOT NULL;
