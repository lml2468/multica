-- Reconcile issue.origin_type CHECK to the full union, unconditionally.
--
-- Why this exists (v0.3.41 upstream sync): the fork's Octo migration was
-- renumbered 120 -> 149 to order after upstream's 127-148, and the
-- runOctoWebhookRenumberHook makes an existing deployment SKIP 149's SQL
-- entirely (it relabels schema_migrations 120->149 so the runner treats 149 as
-- already applied — otherwise 149's bare CREATE TABLE would 42P07). But 149's
-- SQL is also the only place that re-adds 'octo_chat' to the constraint. On the
-- upgrade path, upstream's 131_issue_origin_slack_chat runs first (lexically
-- before 149) and full-replaces the constraint with the slack-aware set WITHOUT
-- 'octo_chat'; then 149 is skipped, so 'octo_chat' is never restored and the
-- upgraded DB silently diverges from a fresh install.
--
-- Fixing this inside 149 is impossible: the hook has to skip 149 wholesale to
-- avoid the duplicate-table crash, which also skips any constraint change there.
-- So the constraint reconciliation lives in its own migration that runs on BOTH
-- fresh and upgraded databases regardless of the 149 skip. It is a full-replace
-- to the complete union, so it is correct no matter which prior state the DB is
-- in (fresh: already 5-value, idempotent; upgraded: restores 'octo_chat').
ALTER TABLE issue DROP CONSTRAINT IF EXISTS issue_origin_type_check;
ALTER TABLE issue ADD CONSTRAINT issue_origin_type_check
    CHECK (origin_type IN ('autopilot', 'quick_create', 'lark_chat', 'slack_chat', 'octo_chat'));
