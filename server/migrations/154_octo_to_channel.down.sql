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
INSERT INTO octo_installation (
    id, workspace_id, agent_id, bot_token_encrypted, robot_id, bot_name,
    owner_uid, api_url, ws_url, installer_user_id, status,
    ws_lease_token, ws_lease_expires_at, installed_at, created_at, updated_at
)
SELECT
    id, workspace_id, agent_id,
    decode(config->>'bot_token_encrypted', 'base64'),
    config->>'app_id',
    COALESCE(config->>'bot_name', ''),
    COALESCE(config->>'owner_uid', ''),
    COALESCE(config->>'api_url', ''),
    COALESCE(config->>'ws_url', ''),
    installer_user_id, status,
    ws_lease_token, ws_lease_expires_at, installed_at, created_at, updated_at
FROM channel_installation
WHERE channel_type = 'octo';

INSERT INTO octo_user_binding (
    id, workspace_id, multica_user_id, installation_id, octo_uid, bound_at
)
SELECT
    id, workspace_id, multica_user_id, installation_id, channel_user_id, bound_at
FROM channel_user_binding
WHERE channel_type = 'octo';

INSERT INTO octo_chat_session_binding (
    id, chat_session_id, installation_id, octo_channel_id, octo_channel_type, created_at
)
SELECT
    id, chat_session_id, installation_id, channel_chat_id,
    COALESCE((config->>'octo_channel_type')::smallint,
             CASE WHEN chat_type = 'p2p' THEN 1 ELSE 2 END),
    created_at
FROM channel_chat_session_binding
WHERE channel_type = 'octo';

INSERT INTO octo_inbound_dedup (
    installation_id, message_id, received_at, processed_at, claim_token
)
SELECT d.installation_id, d.message_id, d.received_at, d.processed_at, d.claim_token
FROM channel_inbound_message_dedup d
JOIN octo_installation i ON i.id = d.installation_id;

INSERT INTO octo_inbound_audit (
    id, installation_id, octo_channel_id, octo_message_id, drop_reason, received_at
)
SELECT
    id, installation_id, channel_chat_id, channel_message_id, drop_reason, received_at
FROM channel_inbound_audit
WHERE channel_type = 'octo';

INSERT INTO octo_outbound_message (
    id, chat_session_id, task_id, octo_channel_id, octo_message_id,
    octo_message_seq, status, last_edited_at, created_at
)
SELECT
    id, chat_session_id, task_id, channel_chat_id,
    split_part(channel_card_message_id, ':', 1),
    COALESCE(NULLIF(split_part(channel_card_message_id, ':', 2), ''), '0')::bigint,
    status, last_patched_at, created_at
FROM channel_outbound_card_message
WHERE channel_type = 'octo';

INSERT INTO octo_binding_token (
    token_hash, workspace_id, installation_id, octo_uid, expires_at, consumed_at, created_at
)
SELECT
    token_hash, workspace_id, installation_id, channel_user_id, expires_at, consumed_at, created_at
FROM channel_binding_token
WHERE channel_type = 'octo';

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
