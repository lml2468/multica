-- Revert the issue.origin_type CHECK to its pre-Octo state before dropping the
-- Octo tables. Since this migration runs after upstream's 131 (slack_chat), the
-- revert target must keep 'slack_chat' — dropping it would break Slack /issue on
-- a down-migrate. Only 'octo_chat' is removed here.
ALTER TABLE issue DROP CONSTRAINT IF EXISTS issue_origin_type_check;
ALTER TABLE issue ADD CONSTRAINT issue_origin_type_check
    CHECK (origin_type IN ('autopilot', 'quick_create', 'lark_chat', 'slack_chat'));

DROP TABLE IF EXISTS octo_binding_token;
DROP TABLE IF EXISTS octo_outbound_message;
DROP TABLE IF EXISTS octo_inbound_audit;
DROP TABLE IF EXISTS octo_inbound_dedup;
DROP TABLE IF EXISTS octo_chat_session_binding;
DROP TABLE IF EXISTS octo_user_binding;
DROP TABLE IF EXISTS octo_installation;
