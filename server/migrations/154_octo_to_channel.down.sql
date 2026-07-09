-- Revert 154_octo_to_channel: recreate the octo_* tables and move the folded
-- rows (channel_type='octo') back out of the channel_* tables, then delete those
-- octo rows from channel_*. The DDL below is copied verbatim from
-- 149_octo_integration.up.sql so a down-then-up round-trips exactly.
--
-- issue.origin_type is left untouched: 'octo_chat' is owned by
-- 153_issue_origin_type_union, not by this migration.

-- =====================
-- recreate octo_* tables (verbatim from 149)
-- =====================
CREATE TABLE octo_installation (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id          UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    agent_id              UUID NOT NULL REFERENCES agent(id) ON DELETE CASCADE,
    bot_token_encrypted   BYTEA NOT NULL,
    robot_id              TEXT NOT NULL,
    bot_name              TEXT NOT NULL DEFAULT '',
    owner_uid             TEXT NOT NULL DEFAULT '',
    api_url               TEXT NOT NULL,
    ws_url                TEXT NOT NULL DEFAULT '',
    installer_user_id     UUID NOT NULL REFERENCES "user"(id) ON DELETE RESTRICT,
    status                TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'revoked')),
    ws_lease_token        TEXT,
    ws_lease_expires_at   TIMESTAMPTZ,
    installed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, agent_id),
    UNIQUE (robot_id),
    UNIQUE (id, workspace_id)
);

CREATE INDEX idx_octo_installation_workspace ON octo_installation(workspace_id);
CREATE INDEX idx_octo_installation_agent ON octo_installation(agent_id);
CREATE INDEX idx_octo_installation_lease ON octo_installation(ws_lease_expires_at)
    WHERE status = 'active';

CREATE TABLE octo_user_binding (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id     UUID NOT NULL,
    multica_user_id  UUID NOT NULL,
    installation_id  UUID NOT NULL,
    octo_uid         TEXT NOT NULL,
    bound_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (installation_id, octo_uid),
    CONSTRAINT octo_user_binding_installation_fk
        FOREIGN KEY (installation_id, workspace_id)
        REFERENCES octo_installation(id, workspace_id)
        ON DELETE CASCADE,
    CONSTRAINT octo_user_binding_member_fk
        FOREIGN KEY (workspace_id, multica_user_id)
        REFERENCES member(workspace_id, user_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_octo_user_binding_user
    ON octo_user_binding(multica_user_id, workspace_id);

CREATE TABLE octo_chat_session_binding (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chat_session_id    UUID NOT NULL REFERENCES chat_session(id) ON DELETE CASCADE,
    installation_id    UUID NOT NULL REFERENCES octo_installation(id) ON DELETE CASCADE,
    octo_channel_id    TEXT NOT NULL,
    octo_channel_type  SMALLINT NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (installation_id, octo_channel_id),
    UNIQUE (chat_session_id)
);

CREATE TABLE octo_inbound_dedup (
    installation_id  UUID NOT NULL,
    message_id       TEXT NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at     TIMESTAMPTZ,
    claim_token      UUID NOT NULL DEFAULT gen_random_uuid(),
    PRIMARY KEY (installation_id, message_id)
);

CREATE INDEX idx_octo_inbound_dedup_received
    ON octo_inbound_dedup(received_at);

CREATE TABLE octo_inbound_audit (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    installation_id   UUID REFERENCES octo_installation(id) ON DELETE SET NULL,
    octo_channel_id   TEXT,
    octo_message_id   TEXT,
    drop_reason       TEXT NOT NULL,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_octo_inbound_audit_installation
    ON octo_inbound_audit(installation_id, received_at DESC);
CREATE INDEX idx_octo_inbound_audit_reason
    ON octo_inbound_audit(drop_reason, received_at DESC);

CREATE TABLE octo_outbound_message (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chat_session_id    UUID NOT NULL REFERENCES chat_session(id) ON DELETE CASCADE,
    task_id            UUID REFERENCES agent_task_queue(id) ON DELETE SET NULL,
    octo_channel_id    TEXT NOT NULL,
    octo_message_id    TEXT NOT NULL,
    octo_message_seq   BIGINT NOT NULL DEFAULT 0,
    status             TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'streaming', 'final', 'error')),
    last_edited_at     TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_octo_outbound_message_task
    ON octo_outbound_message(task_id)
    WHERE task_id IS NOT NULL;
CREATE INDEX idx_octo_outbound_message_session
    ON octo_outbound_message(chat_session_id, created_at DESC);

CREATE TABLE octo_binding_token (
    token_hash       TEXT PRIMARY KEY,
    workspace_id     UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    installation_id  UUID NOT NULL REFERENCES octo_installation(id) ON DELETE CASCADE,
    octo_uid         TEXT NOT NULL,
    expires_at       TIMESTAMPTZ NOT NULL,
    consumed_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT octo_binding_token_ttl_cap
        CHECK (expires_at <= created_at + INTERVAL '15 minutes')
);

CREATE INDEX idx_octo_binding_token_installation
    ON octo_binding_token(installation_id, expires_at);

-- =====================
-- move folded rows back out of channel_* (channel_type='octo')
-- =====================
-- =====================
-- move folded rows back out of channel_* (channel_type='octo'), orphan-safe
-- =====================
-- The recreated octo_* tables re-impose the FKs that channel_* deliberately
-- dropped (124 §4: no FKs/cascades). While data lived FK-free in channel_*, a
-- parent row (workspace / agent / user / member / chat_session / agent_task_queue)
-- could have been deleted at runtime without cascading the octo row. A naive
-- re-INSERT would then violate the recreated FK (23503) and abort the ENTIRE
-- rollback. So each re-INSERT filters to rows whose required parents still exist
-- (a true orphan cannot be restored — its parent is gone), and nullable FKs whose
-- parent vanished are set NULL rather than dropping the row. A NOTICE logs how
-- many orphans were skipped per table. Every octo row is still removed from
-- channel_* by the DELETEs below, so no octo state lingers after down.

INSERT INTO octo_installation (
    id, workspace_id, agent_id, bot_token_encrypted, robot_id, bot_name,
    owner_uid, api_url, ws_url, installer_user_id, status,
    ws_lease_token, ws_lease_expires_at, installed_at, created_at, updated_at
)
SELECT
    ci.id, ci.workspace_id, ci.agent_id,
    decode(ci.config->>'bot_token_encrypted', 'base64'),
    ci.config->>'app_id',
    COALESCE(ci.config->>'bot_name', ''),
    COALESCE(ci.config->>'owner_uid', ''),
    COALESCE(ci.config->>'api_url', ''),
    COALESCE(ci.config->>'ws_url', ''),
    ci.installer_user_id, ci.status,
    ci.ws_lease_token, ci.ws_lease_expires_at, ci.installed_at, ci.created_at, ci.updated_at
FROM channel_installation ci
WHERE ci.channel_type = 'octo'
  AND EXISTS (SELECT 1 FROM workspace w WHERE w.id = ci.workspace_id)
  AND EXISTS (SELECT 1 FROM agent a WHERE a.id = ci.agent_id)
  AND EXISTS (SELECT 1 FROM "user" u WHERE u.id = ci.installer_user_id);

INSERT INTO octo_user_binding (
    id, workspace_id, multica_user_id, installation_id, octo_uid, bound_at
)
SELECT
    cub.id, cub.workspace_id, cub.multica_user_id, cub.installation_id, cub.channel_user_id, cub.bound_at
FROM channel_user_binding cub
WHERE cub.channel_type = 'octo'
  -- composite FK (installation_id, workspace_id) -> octo_installation(id, workspace_id)
  AND EXISTS (SELECT 1 FROM octo_installation i
              WHERE i.id = cub.installation_id AND i.workspace_id = cub.workspace_id)
  -- composite FK (workspace_id, multica_user_id) -> member(workspace_id, user_id)
  AND EXISTS (SELECT 1 FROM member m
              WHERE m.workspace_id = cub.workspace_id AND m.user_id = cub.multica_user_id);

INSERT INTO octo_chat_session_binding (
    id, chat_session_id, installation_id, octo_channel_id, octo_channel_type, created_at
)
SELECT
    ccsb.id, ccsb.chat_session_id, ccsb.installation_id, ccsb.channel_chat_id,
    COALESCE((ccsb.config->>'octo_channel_type')::smallint,
             CASE WHEN ccsb.chat_type = 'p2p' THEN 1 ELSE 2 END),
    ccsb.created_at
FROM channel_chat_session_binding ccsb
WHERE ccsb.channel_type = 'octo'
  AND EXISTS (SELECT 1 FROM chat_session cs WHERE cs.id = ccsb.chat_session_id)
  AND EXISTS (SELECT 1 FROM octo_installation i WHERE i.id = ccsb.installation_id);

-- octo_inbound_dedup has no FK, but its rows are keyed by installation_id; the
-- JOIN to the just-restored octo_installation drops rows for skipped installs.
INSERT INTO octo_inbound_dedup (
    installation_id, message_id, received_at, processed_at, claim_token
)
SELECT d.installation_id, d.message_id, d.received_at, d.processed_at, d.claim_token
FROM channel_inbound_message_dedup d
JOIN octo_installation i ON i.id = d.installation_id;

-- octo_inbound_audit.installation_id is nullable (ON DELETE SET NULL); when the
-- referenced install was orphan-skipped, keep the audit row with a NULL install
-- rather than dropping it.
INSERT INTO octo_inbound_audit (
    id, installation_id, octo_channel_id, octo_message_id, drop_reason, received_at
)
SELECT
    cia.id,
    CASE WHEN EXISTS (SELECT 1 FROM octo_installation i WHERE i.id = cia.installation_id)
         THEN cia.installation_id ELSE NULL END,
    cia.channel_chat_id, cia.channel_message_id, cia.drop_reason, cia.received_at
FROM channel_inbound_audit cia
WHERE cia.channel_type = 'octo';

-- octo_message_id / seq are packed as "<message_id>:<seq>". Decode seq as the
-- trailing digits after the LAST colon so a message_id containing ':' survives;
-- fall back to the whole string + seq 0 if the pattern does not match.
-- chat_session_id is NOT NULL (must exist); task_id is nullable (ON DELETE SET
-- NULL) so a vanished task becomes NULL rather than dropping the row.
INSERT INTO octo_outbound_message (
    id, chat_session_id, task_id, octo_channel_id, octo_message_id,
    octo_message_seq, status, last_edited_at, created_at
)
SELECT
    cocm.id, cocm.chat_session_id,
    CASE WHEN cocm.task_id IS NOT NULL
              AND EXISTS (SELECT 1 FROM agent_task_queue q WHERE q.id = cocm.task_id)
         THEN cocm.task_id ELSE NULL END,
    cocm.channel_chat_id,
    COALESCE(substring(cocm.channel_card_message_id from '^(.*):[0-9]+$'), cocm.channel_card_message_id),
    COALESCE(substring(cocm.channel_card_message_id from ':([0-9]+)$'), '0')::bigint,
    cocm.status, cocm.last_patched_at, cocm.created_at
FROM channel_outbound_card_message cocm
WHERE cocm.channel_type = 'octo'
  AND EXISTS (SELECT 1 FROM chat_session cs WHERE cs.id = cocm.chat_session_id);

INSERT INTO octo_binding_token (
    token_hash, workspace_id, installation_id, octo_uid, expires_at, consumed_at, created_at
)
SELECT
    cbt.token_hash, cbt.workspace_id, cbt.installation_id, cbt.channel_user_id,
    cbt.expires_at, cbt.consumed_at, cbt.created_at
FROM channel_binding_token cbt
WHERE cbt.channel_type = 'octo'
  AND EXISTS (SELECT 1 FROM workspace w WHERE w.id = cbt.workspace_id)
  AND EXISTS (SELECT 1 FROM octo_installation i WHERE i.id = cbt.installation_id);

-- Log any orphans that could not be restored (parent rows already gone), so a
-- rollback that silently drops rows is at least visible in the migrate output.
DO $$
DECLARE
    dropped text;
BEGIN
    SELECT string_agg(format('%s: %s', t, n), ', ') INTO dropped FROM (
        SELECT 'installation' AS t,
            (SELECT count(*) FROM channel_installation WHERE channel_type = 'octo')
            - (SELECT count(*) FROM octo_installation) AS n
        UNION ALL SELECT 'user_binding',
            (SELECT count(*) FROM channel_user_binding WHERE channel_type = 'octo')
            - (SELECT count(*) FROM octo_user_binding)
        UNION ALL SELECT 'chat_session_binding',
            (SELECT count(*) FROM channel_chat_session_binding WHERE channel_type = 'octo')
            - (SELECT count(*) FROM octo_chat_session_binding)
        UNION ALL SELECT 'binding_token',
            (SELECT count(*) FROM channel_binding_token WHERE channel_type = 'octo')
            - (SELECT count(*) FROM octo_binding_token)
    ) m WHERE n > 0;
    IF dropped IS NOT NULL THEN
        RAISE NOTICE 'octo down: skipped orphaned rows whose FK parents were gone: %', dropped;
    END IF;
END $$;

-- =====================
-- delete the folded rows from channel_*
-- =====================
DELETE FROM channel_binding_token WHERE channel_type = 'octo';
DELETE FROM channel_outbound_card_message WHERE channel_type = 'octo';
DELETE FROM channel_inbound_audit WHERE channel_type = 'octo';
DELETE FROM channel_inbound_message_dedup d
    USING channel_installation i
    WHERE d.installation_id = i.id AND i.channel_type = 'octo';
DELETE FROM channel_chat_session_binding WHERE channel_type = 'octo';
DELETE FROM channel_user_binding WHERE channel_type = 'octo';
DELETE FROM channel_installation WHERE channel_type = 'octo';
