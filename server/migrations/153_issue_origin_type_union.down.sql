-- Revert the reconciliation to the post-131 state (slack-aware, no octo_chat),
-- matching what 149.down leaves behind. Down-migrating 153 alone should land
-- the constraint where 149.down would — 'octo_chat' is owned by 149.
ALTER TABLE issue DROP CONSTRAINT IF EXISTS issue_origin_type_check;
ALTER TABLE issue ADD CONSTRAINT issue_origin_type_check
    CHECK (origin_type IN ('autopilot', 'quick_create', 'lark_chat', 'slack_chat'));
