-- Outbound webhook auto-disable on repeated terminal failures (#38).
--
-- The dispatcher's terminal outcomes are "delivered" (2xx) and "failed"
-- (non-retryable 4xx, or retries exhausted). A subscription with a broken
-- endpoint — receiver returns 401 after a token rotation, 404 after the path
-- moves, 400 after schema tightens — stays enabled forever in v1; every
-- subsequent event posts and fails silently until an operator notices via
-- delivery history.
--
-- consecutive_failures counts terminal "failed" outcomes since the last
-- "delivered". On any "delivered" the counter resets to 0. When the counter
-- crosses the configured threshold (env DM_OUTBOUND_WEBHOOK_AUTO_DISABLE_THRESHOLD,
-- default 20), the dispatcher sets enabled=false and stamps disabled_reason so
-- the UI can distinguish "operator disabled" from "system disabled".
--
-- A pure consecutive-count beats time-window heuristics here: a workspace with
-- one status change per day takes weeks to trip a 5-minute window, and a
-- noisy workspace trips it on a brief outage. Count-based is bias-free across
-- traffic levels and resets cleanly on first recovery.
ALTER TABLE webhook_subscription
    ADD COLUMN consecutive_failures INT NOT NULL DEFAULT 0,
    ADD COLUMN disabled_reason      TEXT;
