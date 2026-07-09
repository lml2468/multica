package octo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/integrations/octo/transport"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// The outcome replier is the outbound half of the inbound pipeline the Patcher
// does not own: the Patcher relays the agent's eventual chat reply (chat:done /
// task:failed), while the replier handles the synchronous, pre-agent outcomes the
// Router decides — NeedsBinding (DM the unbound sender a one-shot binding link),
// AgentOffline / AgentArchived (tell the user the agent can't run). Ingested and
// Dropped produce no reply here.
//
// It implements engine.OutboundReplier, so the Router drives it off the ACK
// critical path. Reply is best-effort by design: a transient Octo outage MUST
// NOT fail the inbound pipeline. Errors are logged and swallowed; the next
// inbound message from the same user retries the reply on its own.

// BindingMinter mints a single-use binding token. Satisfied by
// *BindingTokenService; an interface so the replier is unit-testable without a DB.
type BindingMinter interface {
	Mint(ctx context.Context, workspaceID, installationID pgtype.UUID, uid UID) (BindingToken, error)
}

// installationLoader loads a channel_installation row by id so the replier can
// read the bot credentials from its config blob. Satisfied by *db.Queries.
type installationLoader interface {
	GetChannelInstallation(ctx context.Context, arg db.GetChannelInstallationParams) (db.ChannelInstallation, error)
}

// noopReplier is the safe default when the integration is wired without the
// dependencies the production replier needs. It logs each outcome that would have
// produced a reply so the gap is visible in production logs.
type noopReplier struct {
	log *slog.Logger
}

func (n *noopReplier) Reply(_ context.Context, inst engine.ResolvedInstallation, msg channel.InboundMessage, res engine.Result) {
	switch res.Outcome {
	case engine.OutcomeNeedsBinding, engine.OutcomeAgentOffline, engine.OutcomeAgentArchived:
		n.log.Warn("octo outcome replier: outbound reply skipped (replier not wired)",
			"outcome", string(res.Outcome),
			"installation_id", uuidString(inst.ID),
			"channel_id", msg.Source.ChatID,
			"sender", res.Sender,
		)
	}
}

// NewNoopOutcomeReplier returns the no-op replier, used as the fallback when
// production wiring is incomplete.
func NewNoopOutcomeReplier(log *slog.Logger) engine.OutboundReplier {
	if log == nil {
		log = slog.Default()
	}
	return &noopReplier{log: log}
}

// octoOutcomeReplier is the production engine.OutboundReplier. It reuses the same
// MessageSender + TokenDecryptor the Patcher uses, so a binding prompt is just a
// plain-text DM to the sender's uid (Octo renders markdown natively).
type octoOutcomeReplier struct {
	loader      installationLoader
	minter      BindingMinter
	decryptor   TokenDecryptor
	sender      MessageSender
	publicURL   string // e.g. https://multica.example, trailing slash trimmed
	bindingPath string // path component of the binding URL, default "/octo/bind"
	log         *slog.Logger
}

// OutcomeReplierConfig wires the production replier. PublicURL is the Multica HTTP
// host the user clicks into to redeem the binding token; empty means the binding
// flow can only log the uid, not produce a clickable link.
type OutcomeReplierConfig struct {
	Loader      installationLoader
	Minter      BindingMinter
	Decryptor   TokenDecryptor
	Sender      MessageSender
	PublicURL   string
	BindingPath string
	Logger      *slog.Logger
}

// NewOutcomeReplier validates the configuration and returns the production
// replier. Missing dependencies fall back to noop so the boot path stays robust
// on partially-configured deployments.
func NewOutcomeReplier(cfg OutcomeReplierConfig) engine.OutboundReplier {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if cfg.Loader == nil || cfg.Minter == nil || cfg.Decryptor == nil || cfg.Sender == nil {
		return NewNoopOutcomeReplier(log)
	}
	if cfg.PublicURL == "" {
		log.Warn("octo outcome replier: MULTICA_PUBLIC_URL not set; binding link will not work")
	}
	bindingPath := cfg.BindingPath
	if bindingPath == "" {
		bindingPath = "/octo/bind"
	}
	if !strings.HasPrefix(bindingPath, "/") {
		bindingPath = "/" + bindingPath
	}
	return &octoOutcomeReplier{
		loader:      cfg.Loader,
		minter:      cfg.Minter,
		decryptor:   cfg.Decryptor,
		sender:      cfg.Sender,
		publicURL:   strings.TrimRight(cfg.PublicURL, "/"),
		bindingPath: bindingPath,
		log:         log,
	}
}

var _ engine.OutboundReplier = (*octoOutcomeReplier)(nil)

// Reply implements engine.OutboundReplier. The switch is the SOURCE OF TRUTH for
// which outcomes generate a reply; a missing branch silently drops the user-
// visible side effect.
func (r *octoOutcomeReplier) Reply(ctx context.Context, inst engine.ResolvedInstallation, msg channel.InboundMessage, res engine.Result) {
	switch res.Outcome {
	case engine.OutcomeNeedsBinding, engine.OutcomeAgentOffline, engine.OutcomeAgentArchived:
	default:
		// Ingested replies flow through the Patcher; Dropped is silent.
		return
	}

	row, creds, err := r.resolve(ctx, inst.ID)
	if err != nil {
		r.log.Warn("octo outcome replier: load installation failed",
			"installation_id", uuidString(inst.ID), "err", err.Error())
		return
	}

	switch res.Outcome {
	case engine.OutcomeNeedsBinding:
		if err := r.sendBindingPrompt(ctx, row, creds, res.Sender); err != nil {
			r.log.Warn("octo outcome replier: binding prompt failed",
				"installation_id", uuidString(inst.ID), "sender", res.Sender, "err", err.Error())
		}
	case engine.OutcomeAgentOffline:
		if err := r.sendDM(ctx, creds, msg.Source.ChatID, octoChatType(msg), agentOfflineCopy); err != nil {
			r.log.Warn("octo outcome replier: offline notice failed",
				"installation_id", uuidString(inst.ID), "channel_id", msg.Source.ChatID, "err", err.Error())
		}
	case engine.OutcomeAgentArchived:
		if err := r.sendDM(ctx, creds, msg.Source.ChatID, octoChatType(msg), agentArchivedCopy); err != nil {
			r.log.Warn("octo outcome replier: archived notice failed",
				"installation_id", uuidString(inst.ID), "channel_id", msg.Source.ChatID, "err", err.Error())
		}
	}
}

// resolve loads the installation row and decodes its credentials (api_url + bot
// token). The bot token is decrypted via the injected TokenDecryptor.
func (r *octoOutcomeReplier) resolve(ctx context.Context, instID pgtype.UUID) (db.ChannelInstallation, credentials, error) {
	row, err := r.loader.GetChannelInstallation(ctx, db.GetChannelInstallationParams{
		ID:          instID,
		ChannelType: string(TypeOcto),
	})
	if err != nil {
		return db.ChannelInstallation{}, credentials{}, fmt.Errorf("load installation: %w", err)
	}
	creds, err := decodeCredentials(row.Config, nil)
	if err != nil {
		return db.ChannelInstallation{}, credentials{}, fmt.Errorf("decode config: %w", err)
	}
	token, err := r.decryptor.DecryptBotToken(row)
	if err != nil {
		return db.ChannelInstallation{}, credentials{}, fmt.Errorf("decrypt bot token: %w", err)
	}
	creds.BotToken = token
	return row, creds, nil
}

// sendBindingPrompt mints a one-shot token and DMs the unbound sender a link to
// redeem it. The DM goes to the sender's own uid as a 1:1 channel — even when the
// triggering message arrived in a group, the binding prompt is private so a group
// is never spammed with binding links.
func (r *octoOutcomeReplier) sendBindingPrompt(ctx context.Context, row db.ChannelInstallation, creds credentials, sender string) error {
	if sender == "" {
		return errors.New("missing sender uid")
	}
	if r.publicURL == "" {
		return errors.New("public_url not configured")
	}
	token, err := r.minter.Mint(ctx, row.WorkspaceID, row.ID, UID(sender))
	if err != nil {
		return fmt.Errorf("mint binding token: %w", err)
	}
	bindURL := r.publicURL + r.bindingPath + "?token=" + url.QueryEscape(token.Raw)
	return r.sendDM(ctx, creds, sender, transport.ChannelDM, bindingPromptText(bindURL))
}

// sendDM sends content to the given channel with the installation's decrypted bot
// token. Used for the binding prompt (sender's DM channel) and the
// agent-unavailable notices (the originating channel).
func (r *octoOutcomeReplier) sendDM(ctx context.Context, creds credentials, channelID string, channelType transport.ChannelType, content string) error {
	if _, err := r.sender.Send(ctx, creds.APIURL, creds.BotToken, channelID, channelType, content); err != nil {
		return fmt.Errorf("send message: %w", err)
	}
	return nil
}

// octoChatType maps the inbound message back to the WuKongIM channel type for
// the outbound send. It reads the real channel type (1/2/5) the adapter stashed
// in msg.Raw so a community topic (5) is not lost — msg.Source.ChatType alone
// collapses topic and group into "group". Falls back to the p2p/group
// discriminator only when Raw is missing/unparseable.
func octoChatType(msg channel.InboundMessage) transport.ChannelType {
	if raw, err := decodeOctoRaw(msg); err == nil && raw.ChannelType != 0 {
		return transport.ChannelType(raw.ChannelType)
	}
	if msg.Source.ChatType == channel.ChatTypeP2P {
		return transport.ChannelDM
	}
	return transport.ChannelGroup
}

// bindingPromptText is the user-facing copy DMed to an unbound sender. The link
// is on its own line so Octo's auto-linker turns it into a tappable URL.
func bindingPromptText(bindURL string) string {
	return "你还没有绑定 Multica 账号，无法处理你的消息。\n点击下面的链接完成绑定（15 分钟内有效）：\n" + bindURL
}

// agentOfflineCopy and agentArchivedCopy are the user-visible strings for the two
// agent-unavailability outcomes. An offline agent will run when its daemon
// reconnects; an archived agent needs operator action.
const (
	agentOfflineCopy  = "Agent 当前离线，消息已记录。下次 daemon 上线后会自动继续处理。"
	agentArchivedCopy = "这个 Agent 已被归档，无法继续处理消息。请联系工作区管理员恢复或重新绑定。"
)
