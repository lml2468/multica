-- Converge the Octo IM integration onto the generalized channel_* tables so it
-- becomes the third consumer of the shared channel.Channel engine (after Feishu
-- and Slack), retiring its parallel octo_* stack. This folds every octo_* row
-- into the matching channel_* table with channel_type='octo', then DROPs the
-- octo_* tables. The product is not live, so this is a clean cutover with no
-- dual-path / feature flag.
--
-- Ordering vs the fork's renumber machinery: octo_* is ALWAYS present when this
-- migration runs, on both paths, so no pre-migration hook is needed here.
--   * Fresh install: 149_octo_integration's SQL runs (its renumber hook no-ops
--     with nothing recorded to relabel), creating octo_*.
--   * Upgrade: the hook relabels the recorded 120->149 so 149's SQL is skipped,
--     but octo_* already exists from the original migration 120.
-- Either way octo_* exists here; the INSERT ... SELECT copies forward (0 rows on
-- an empty fresh install) and the DROP removes them. issue.origin_type already
-- carries 'octo_chat' via 153_issue_origin_type_union, so it is untouched.
--
-- Field mapping notes:
--   * octo_installation.robot_id -> config->>'app_id' — the routing-key slot the
--     generic GetChannelInstallationByAppID query and the
--     (channel_type, config->>'app_id') unique index already use (same slot Slack
--     uses for its app id). bot_token_encrypted (BYTEA) is carried as unwrapped
--     base64 inside config, exactly as 124 did for Feishu's app_secret_encrypted.
--   * octo_outbound_message stored (octo_message_id, octo_message_seq) for the
--     WuKongIM message-edit streaming path; channel_outbound_card_message has a
--     single channel_card_message_id, so the seq is encoded as
--     "<message_id>:<seq>" (decoded back on the outbound edit path / on down).

-- =====================
-- channel_installation
-- =====================
INSERT INTO channel_installation (
    id, workspace_id, agent_id, channel_type, config, status,
    ws_lease_token, ws_lease_expires_at, installer_user_id,
    installed_at, created_at, updated_at
)
SELECT
    id, workspace_id, agent_id, 'octo',
    jsonb_build_object(
        'app_id',              robot_id,
        'bot_name',            bot_name,
        'owner_uid',           owner_uid,
        'api_url',             api_url,
        'ws_url',              ws_url,
        'bot_token_encrypted', replace(encode(bot_token_encrypted, 'base64'), E'\n', '')
    ),
    status, ws_lease_token, ws_lease_expires_at, installer_user_id,
    installed_at, created_at, updated_at
FROM octo_installation;

-- =====================
-- channel_user_binding
-- =====================
INSERT INTO channel_user_binding (
    id, workspace_id, multica_user_id, installation_id,
    channel_type, channel_user_id, config, bound_at
)
SELECT
    id, workspace_id, multica_user_id, installation_id,
    'octo', octo_uid, '{}'::jsonb, bound_at
FROM octo_user_binding;

-- =====================
-- channel_chat_session_binding
-- =====================
-- octo_channel_type (1=DM, 2=group, 5=topic) collapses to chat_type
-- ('p2p'/'group'); the raw WuKongIM channel_type is preserved in config so the
-- outbound path can still address the platform channel.
INSERT INTO channel_chat_session_binding (
    id, chat_session_id, installation_id, channel_type,
    channel_chat_id, chat_type, last_message_id, last_thread_id, config, created_at
)
SELECT
    id, chat_session_id, installation_id, 'octo',
    octo_channel_id,
    CASE WHEN octo_channel_type = 1 THEN 'p2p' ELSE 'group' END,
    NULL, NULL,
    jsonb_build_object('octo_channel_type', octo_channel_type),
    created_at
FROM octo_chat_session_binding;

-- =====================
-- channel_inbound_message_dedup
-- =====================
INSERT INTO channel_inbound_message_dedup (
    installation_id, message_id, received_at, processed_at, claim_token
)
SELECT installation_id, message_id, received_at, processed_at, claim_token
FROM octo_inbound_dedup;

-- =====================
-- channel_inbound_audit
-- =====================
-- octo_inbound_audit has no event_type column; the channel column is NOT NULL,
-- so folded rows carry ''. octo_message_id maps to channel_message_id.
INSERT INTO channel_inbound_audit (
    id, installation_id, channel_type, channel_chat_id, event_type,
    channel_event_id, channel_message_id, drop_reason, received_at
)
SELECT
    id, installation_id, 'octo', octo_channel_id, '',
    NULL, octo_message_id, drop_reason, received_at
FROM octo_inbound_audit;

-- =====================
-- channel_outbound_card_message
-- =====================
-- Encode the WuKongIM (message_id, message_seq) pair into the single
-- channel_card_message_id slot as "<message_id>:<seq>".
INSERT INTO channel_outbound_card_message (
    id, chat_session_id, task_id, channel_type, channel_chat_id,
    channel_card_message_id, status, last_patched_at, created_at
)
SELECT
    id, chat_session_id, task_id, 'octo', octo_channel_id,
    octo_message_id || ':' || octo_message_seq,
    status, last_edited_at, created_at
FROM octo_outbound_message;

-- =====================
-- channel_binding_token
-- =====================
INSERT INTO channel_binding_token (
    token_hash, workspace_id, installation_id, channel_type,
    channel_user_id, expires_at, consumed_at, created_at
)
SELECT
    token_hash, workspace_id, installation_id, 'octo',
    octo_uid, expires_at, consumed_at, created_at
FROM octo_binding_token;

-- =====================
-- prove the fold before the irreversible DROP
-- =====================
-- Atomicity (the whole file runs as one implicit tx) already rolls back a FAILED
-- insert, but it cannot catch a SILENT logic mismatch — a wrong JOIN/WHERE, a
-- misrouting CASE, or a filter copying fewer rows than exist would commit and
-- then drop the source. Octo migrations have a corruption history (the 120->149
-- renumber saga), so assert the copied row counts match per table and abort the
-- whole migration (before the DROP) on any mismatch. The counts are cheap next
-- to the DROP they guard.
DO $$
DECLARE
    mismatch text;
BEGIN
    SELECT string_agg(t, ', ') INTO mismatch FROM (
        SELECT 'installation' AS t
            WHERE (SELECT count(*) FROM octo_installation)
                <> (SELECT count(*) FROM channel_installation WHERE channel_type = 'octo')
        UNION ALL
        SELECT 'user_binding'
            WHERE (SELECT count(*) FROM octo_user_binding)
                <> (SELECT count(*) FROM channel_user_binding WHERE channel_type = 'octo')
        UNION ALL
        SELECT 'chat_session_binding'
            WHERE (SELECT count(*) FROM octo_chat_session_binding)
                <> (SELECT count(*) FROM channel_chat_session_binding WHERE channel_type = 'octo')
        UNION ALL
        SELECT 'inbound_dedup'
            WHERE (SELECT count(*) FROM octo_inbound_dedup)
                <> (SELECT count(*) FROM channel_inbound_message_dedup d
                    JOIN channel_installation i ON i.id = d.installation_id
                    WHERE i.channel_type = 'octo')
        UNION ALL
        SELECT 'inbound_audit'
            WHERE (SELECT count(*) FROM octo_inbound_audit)
                <> (SELECT count(*) FROM channel_inbound_audit WHERE channel_type = 'octo')
        UNION ALL
        SELECT 'outbound_message'
            WHERE (SELECT count(*) FROM octo_outbound_message)
                <> (SELECT count(*) FROM channel_outbound_card_message WHERE channel_type = 'octo')
        UNION ALL
        SELECT 'binding_token'
            WHERE (SELECT count(*) FROM octo_binding_token)
                <> (SELECT count(*) FROM channel_binding_token WHERE channel_type = 'octo')
    ) m;
    IF mismatch IS NOT NULL THEN
        RAISE EXCEPTION 'octo->channel fold row-count mismatch in: %; aborting before DROP', mismatch;
    END IF;
END $$;

-- =====================
-- drop the parallel octo_* stack
-- =====================
-- CASCADE clears the composite FKs octo_user_binding / octo_chat_session_binding
-- / octo_binding_token held against octo_installation.
DROP TABLE IF EXISTS
    octo_binding_token,
    octo_outbound_message,
    octo_inbound_audit,
    octo_inbound_dedup,
    octo_chat_session_binding,
    octo_user_binding,
    octo_installation
CASCADE;
