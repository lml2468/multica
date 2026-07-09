-- Outbound webhook delivery history: one row per dispatcher deliver() call for
-- an outbound webhook subscription (migration 150). The dispatcher
-- (internal/integrations/outwebhook) was fire-and-forget with no persistence;
-- this records the terminal outcome of each delivery so operators can see
-- whether a webhook delivered, inspect the exact signed payload + response, and
-- redeliver after fixing a broken endpoint.
--
-- This is the OUTBOUND mirror of webhook_delivery (migration 093), which is the
-- INBOUND autopilot ingress and is NOT reusable (it keys on autopilot_id /
-- raw_body received / autopilot_run_id — the opposite direction).
--
-- One deliver() call performs up to maxAttempts HTTP tries internally and
-- writes a SINGLE terminal row (no queued->update churn; outbound delivery is
-- fire-and-forget). `status` is the final outcome; `attempt_count` is how many
-- HTTP attempts were made. Bodies are kept for inspection + redeliver: there is
-- no general secrets-at-rest infrastructure yet, but these are our own outbound
-- payloads (already signed and sent), not third-party secrets.
CREATE TABLE outbound_webhook_delivery (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    subscription_id UUID NOT NULL REFERENCES webhook_subscription(id) ON DELETE CASCADE,
    event           TEXT NOT NULL,
    -- Terminal outcome of one deliver() call:
    --   delivered — a 2xx response was received
    --   failed    — a non-retryable 4xx, or retries exhausted (5xx / 429 /
    --               transport error on every attempt)
    status          TEXT NOT NULL CHECK (status IN ('delivered', 'failed')),
    -- Number of HTTP attempts the deliver() call made (1 = delivered first try).
    attempt_count   INTEGER NOT NULL DEFAULT 1,
    -- Last HTTP status code seen. NULL when every attempt was a transport error
    -- (DNS / dial / TLS) and no response was ever received.
    response_status INTEGER,
    -- The exact signed bytes we POSTed. Needed to render "what we sent" and to
    -- redeliver the same payload after the subscriber fixes their endpoint.
    request_body    BYTEA,
    -- Truncated response body (the dispatcher already LimitReaders 4 KiB).
    response_body   TEXT,
    -- Redacted last error (host-only — never the full URL, which carries the
    -- subscriber's path/query tokens).
    error           TEXT,
    -- Set when this row was produced by redelivering an earlier delivery.
    redelivered_from_id UUID REFERENCES outbound_webhook_delivery(id) ON DELETE SET NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-subscription listing, newest first (the deliveries UI).
CREATE INDEX idx_outbound_webhook_delivery_sub
    ON outbound_webhook_delivery(subscription_id, created_at DESC);

-- TTL purge scan (the scheduled OutboundWebhookDeliveryCleanupJob deletes rows
-- older than the retention window).
CREATE INDEX idx_outbound_webhook_delivery_created
    ON outbound_webhook_delivery(created_at);

-- Self-reference lookup for the redelivery lineage FK. Postgres does not
-- auto-index FK columns, and the TTL purge's ON DELETE SET NULL must find rows
-- referencing each deleted parent — without this index that's a sequential
-- scan per purged row. Partial because only redelivered rows carry the link.
CREATE INDEX idx_outbound_webhook_delivery_redelivered_from
    ON outbound_webhook_delivery(redelivered_from_id)
    WHERE redelivered_from_id IS NOT NULL;
