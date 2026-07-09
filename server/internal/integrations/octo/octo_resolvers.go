package octo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// This file is the Octo ResolverSet: the platform-specific seams the channel-
// agnostic engine.Router runs the inbound pipeline through. It mirrors the Slack
// ResolverSet but is built entirely on the generic channel_* queries + the shared
// engine.ChatSession — so "adding Octo" stays "implement Channel + register a
// ResolverSet". Octo has no thread model, no typing indicator, and no cross-app
// identity reuse, so it is a simpler set than Slack's.

// originOctoChat is the issue.origin_type label for issues created via the Octo
// /issue command. Matches the value carried in the issue_origin_type_check union
// (migration 153).
const originOctoChat = "octo_chat"

// NewOctoResolverSet assembles the Octo ResolverSet over the generated queries +
// a tx starter (for the shared session service). replier delivers the binding-
// prompt / agent-unavailable notices; pass nil to disable outbound replies (the
// inbound pipeline is fully functional without it). Octo has no typing notifier.
func NewOctoResolverSet(q *db.Queries, tx engine.TxStarter, replier engine.OutboundReplier) engine.ResolverSet {
	return engine.ResolverSet{
		Installation: &installationResolver{q: q},
		Identity:     &identityResolver{q: q},
		Dedup:        &deduper{q: q},
		Session: &sessionBinder{session: engine.NewChatSession(q, tx, TypeOcto, engine.SessionTitles{
			Group:    "Octo 群聊",
			Direct:   "Octo 私聊",
			Fallback: "Octo 会话",
		})},
		Audit:      &auditor{q: q},
		Replier:    replier,
		OriginType: originOctoChat,
	}
}

var (
	_ engine.InstallationResolver = (*installationResolver)(nil)
	_ engine.IdentityResolver     = (*identityResolver)(nil)
	_ engine.Deduper              = (*deduper)(nil)
	_ engine.SessionBinder        = (*sessionBinder)(nil)
	_ engine.Auditor              = (*auditor)(nil)
)

// decodeOctoRaw reads the octo-specific fields stashed in InboundMessage.Raw by
// the channel adapter (robot_id routing key + WuKongIM channel type).
func decodeOctoRaw(msg channel.InboundMessage) (octoRawEvent, error) {
	var raw octoRawEvent
	if len(msg.Raw) == 0 {
		return octoRawEvent{}, errors.New("octo: inbound message Raw is empty")
	}
	if err := json.Unmarshal(msg.Raw, &raw); err != nil {
		return octoRawEvent{}, fmt.Errorf("decode octo inbound raw: %w", err)
	}
	return raw, nil
}

// ---- installation routing ----

type installationResolver struct{ q *db.Queries }

func (r *installationResolver) ResolveInstallation(ctx context.Context, msg channel.InboundMessage) (engine.ResolvedInstallation, error) {
	raw, err := decodeOctoRaw(msg)
	if err != nil {
		return engine.ResolvedInstallation{}, err
	}
	// Route by robot_id: each installation stores its robot_id in the routing-key
	// slot (config->>'app_id'), and the per-installation socket only ever delivers
	// events for its own bot, so robot_id uniquely identifies the installation.
	inst, err := r.q.GetChannelInstallationByAppID(ctx, db.GetChannelInstallationByAppIDParams{
		ChannelType: string(TypeOcto),
		AppID:       raw.RobotID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return engine.ResolvedInstallation{}, engine.ErrInstallationNotFound
		}
		return engine.ResolvedInstallation{}, err
	}
	return engine.ResolvedInstallation{
		ID:              inst.ID,
		WorkspaceID:     inst.WorkspaceID,
		AgentID:         inst.AgentID,
		InstallerUserID: inst.InstallerUserID,
		Active:          inst.Status == string(InstallationActive),
		Platform:        inst,
	}, nil
}

// ---- identity ----

type identityResolver struct{ q *db.Queries }

func (r *identityResolver) ResolveSender(ctx context.Context, inst engine.ResolvedInstallation, msg channel.InboundMessage) (engine.ResolvedIdentity, error) {
	binding, err := r.q.GetChannelUserBindingByUserID(ctx, db.GetChannelUserBindingByUserIDParams{
		InstallationID: inst.ID,
		ChannelUserID:  msg.Source.SenderID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return engine.ResolvedIdentity{}, engine.ErrSenderUnbound
		}
		return engine.ResolvedIdentity{}, err
	}
	// Binding existence no longer proves membership (no FK on the generic table);
	// re-check that the bound user is still a workspace member.
	if _, err := r.q.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{
		UserID:      binding.MulticaUserID,
		WorkspaceID: inst.WorkspaceID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return engine.ResolvedIdentity{}, engine.ErrSenderNotMember
		}
		return engine.ResolvedIdentity{}, err
	}
	return engine.ResolvedIdentity{UserID: binding.MulticaUserID}, nil
}

// ---- dedup ----

type deduper struct{ q *db.Queries }

func (r *deduper) Claim(ctx context.Context, installationID pgtype.UUID, messageID string) (pgtype.UUID, error) {
	claim, err := r.q.ClaimChannelInboundDedup(ctx, db.ClaimChannelInboundDedupParams{
		InstallationID: installationID,
		MessageID:      messageID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return pgtype.UUID{}, engine.ErrDuplicate
		}
		return pgtype.UUID{}, err
	}
	return claim.ClaimToken, nil
}

func (r *deduper) Mark(ctx context.Context, installationID pgtype.UUID, messageID string, claimToken pgtype.UUID) error {
	_, err := r.q.MarkChannelInboundDedupProcessed(ctx, db.MarkChannelInboundDedupProcessedParams{
		InstallationID: installationID,
		MessageID:      messageID,
		ClaimToken:     claimToken,
	})
	return err
}

func (r *deduper) Release(ctx context.Context, installationID pgtype.UUID, messageID string, claimToken pgtype.UUID) error {
	_, err := r.q.ReleaseChannelInboundDedup(ctx, db.ReleaseChannelInboundDedupParams{
		InstallationID: installationID,
		MessageID:      messageID,
		ClaimToken:     claimToken,
	})
	return err
}

// ---- session bind / append ----

type sessionBinder struct{ session *engine.ChatSession }

func (r *sessionBinder) EnsureSession(ctx context.Context, p engine.EnsureSessionParams) (pgtype.UUID, error) {
	config, _ := octoBindingConfig(p.Message)
	return r.session.EnsureSession(ctx, engine.EnsureSessionInput{
		WorkspaceID:    p.Installation.WorkspaceID,
		AgentID:        p.Installation.AgentID,
		InstallationID: p.Installation.ID,
		Sender:         p.Sender,
		// Octo has no thread model: one channel is one continuous session, so the
		// isolation key is simply the channel id.
		BindingKey:    p.Message.Source.ChatID,
		BindingConfig: config,
		ChatType:      p.Message.Source.ChatType,
	})
}

func (r *sessionBinder) AppendMessage(ctx context.Context, p engine.AppendParams) (engine.AppendResult, error) {
	return r.session.AppendUserMessage(ctx, engine.AppendInput{
		SessionID:      p.SessionID,
		Sender:         p.Sender,
		InstallationID: p.InstallationID,
		Body:           p.Message.Text,
		// Octo text is enriched only by mention-stripping (done in the adapter),
		// so the command source is the body itself.
		CommandText: p.Message.Text,
		MessageID:   p.Message.MessageID,
		// Octo has no thread; the reply target is the channel-level send.
		ClaimToken: p.ClaimToken,
	})
}

// octoBindingConfig persists the WuKongIM channel type on the chat binding's
// config so the outbound path can address the platform channel with the right
// type without re-deriving it from chat_type (topic collapses to group).
func octoBindingConfig(msg channel.InboundMessage) ([]byte, error) {
	raw, err := decodeOctoRaw(msg)
	if err != nil {
		return []byte("{}"), err
	}
	cfg, _ := json.Marshal(octoBindingConfigBlob{OctoChannelType: raw.ChannelType})
	return cfg, nil
}

// octoBindingConfigBlob is the JSON persisted on channel_chat_session_binding
// .config for an Octo binding. octo_channel_type is the raw WuKongIM channel type
// (1/2/5) the outbound sender needs.
type octoBindingConfigBlob struct {
	OctoChannelType int `json:"octo_channel_type"`
}

// ---- audit ----

type auditor struct{ q *db.Queries }

func (r *auditor) RecordDrop(ctx context.Context, instID pgtype.UUID, msg channel.InboundMessage, reason engine.DropReason) error {
	return r.q.RecordChannelInboundDrop(ctx, db.RecordChannelInboundDropParams{
		ChannelType:      string(TypeOcto),
		EventType:        "", // Octo events carry no event_type discriminator.
		DropReason:       string(reason),
		InstallationID:   instID,
		ChannelChatID:    util.StrToText(msg.Source.ChatID),
		ChannelEventID:   util.StrToText(msg.EventID),
		ChannelMessageID: util.StrToText(msg.MessageID),
	})
}
