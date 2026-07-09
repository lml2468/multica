package octo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/octo/transport"
)

// TypeOcto is the channel discriminator for the Octo IM platform. It equals the
// channel_type value persisted by migration 154 and the routing key the Router
// looks a ResolverSet up under.
const TypeOcto channel.Type = "octo"

// socketConn is the subset of transport.Socket that Connect drives. An interface
// so the terminal-error unwind and graceful-cancel paths are unit-tested without
// a live WuKongIM server.
type socketConn interface {
	Connect(ctx context.Context) error
	Disconnect()
}

// octoChannel is ONE installation's WuKongIM connection. The engine.Supervisor
// builds one per active Octo installation (via the registered Factory) and owns
// the lease / reconnect lifecycle; Connect blocks on the receive loop. Inbound
// messages are normalized to channel.InboundMessage and handed to the engine
// router (cfg.Handler), which resolves the installation by robot_id
// (config->>'app_id'). Outbound replies flow through the Patcher (bus subscriber)
// on chat:done / task:failed; Send satisfies the Channel contract and posts with
// this installation's bot token.
//
// The channel holds only what Connect/Send need (decoded from the per-
// installation config blob). The installation IDENTITY (workspace / agent /
// installer) is resolved per message by the Router, so it is deliberately absent.
type octoChannel struct {
	creds   credentials
	handler channel.InboundHandler
	sender  MessageSender
	logger  *slog.Logger

	// Seams, overridable in tests. Nil means "use the real transport"; the
	// defaults are installed lazily in Connect so a zero-value octoChannel (as
	// the factory builds) works in production.
	register  func(ctx context.Context) (*transport.BotRegisterResp, error)
	newSocket func(opts transport.SocketOptions) socketConn
}

var _ channel.Channel = (*octoChannel)(nil)

func (c *octoChannel) Type() channel.Type { return TypeOcto }

// Capabilities declares what the Octo adapter supports. Octo renders markdown
// natively and WuKongIM supports message-edit; there is no typing indicator.
// Declaration only — the engine performs no degradation.
func (c *octoChannel) Capabilities() channel.Capability {
	return channel.CapText | channel.CapMessageEdit
}

// Disconnect is a no-op: the socket's whole lifetime is scoped to Connect (its
// background manager is torn down when Connect returns and calls
// socket.Disconnect via its defer). Mirrors feishuChannel/slackChannel.
func (c *octoChannel) Disconnect(ctx context.Context) error { return nil }

// Send delivers a plain-text/markdown reply with this installation's bot token.
// It exists to satisfy the Channel contract; Octo's real reply path is the
// Patcher (which reads the stored octo_channel_type from the binding config and
// preserves group/topic routing), and the engine.Router never calls Send for
// Octo. The cross-platform OutboundMessage carries no channel type, so a bare
// Send can only assume a DM — it must NOT be wired for group/topic replies until
// OutboundMessage grows a chat-type field; those go through the Patcher.
func (c *octoChannel) Send(ctx context.Context, out channel.OutboundMessage) (channel.SendResult, error) {
	if c.sender == nil {
		return channel.SendResult{}, errors.New("octo: message sender not configured")
	}
	res, err := c.sender.Send(ctx, c.creds.APIURL, c.creds.BotToken, out.ChatID, transport.ChannelDM, out.Text)
	if err != nil {
		return channel.SendResult{}, err
	}
	msgID := ""
	if res != nil {
		msgID = res.MessageID
	}
	return channel.SendResult{MessageID: msgID}, nil
}

// Connect registers the bot to obtain a fresh im_token + ws_url, opens the
// WuKongIM socket, and BLOCKS running the receive loop until ctx is cancelled or
// the link terminally fails — the contract engine.Supervisor relies on to tie
// lease renewal to connection liveness.
//
// The transport.Socket runs its own connect→serve→backoff→reconnect loop on a
// background goroutine and calls OnError only for TERMINAL conditions (kicked,
// rapid disconnect, stale im_token) after which it STOPS reconnecting. If Connect
// merely waited on ctx.Done, such a terminal error would leave the connection
// silently dead while the Supervisor kept renewing the lease — the bot would be
// dead until redeploy. So Connect selects on a buffered socketErr channel fed by
// OnError and returns it, so the Supervisor rebuilds the channel under backoff
// (which re-runs Register for a fresh im_token). This matches how the Slack
// adapter propagates its terminal socket errors.
func (c *octoChannel) Connect(ctx context.Context) error {
	if c.handler == nil {
		return errors.New("octo: inbound handler not configured")
	}
	if c.creds.BotToken == "" {
		return errors.New("octo: installation has no bot token")
	}
	if c.creds.APIURL == "" {
		return errors.New("octo: installation has no api_url")
	}

	register := c.register
	if register == nil {
		// Register to obtain im_token + ws_url (the bot token alone can't open
		// the WS). A fresh HTTP client per Connect is fine — reconnects are rare.
		// force_refresh=true rotates the im_token: the Supervisor rebuilds this
		// channel precisely because a prior connection died (often on a stale
		// im_token), so a rebuild must not reuse the cached token or a
		// stale-token terminal error would loop forever.
		hc := transport.NewHTTPClient(c.creds.APIURL, c.creds.BotToken)
		register = func(ctx context.Context) (*transport.BotRegisterResp, error) {
			return hc.Register(ctx, true, "Multica", "")
		}
	}
	reg, err := register(ctx)
	if err != nil {
		return fmt.Errorf("octo: register bot: %w", err)
	}

	// Buffered so a terminal OnError delivered from the socket goroutine never
	// blocks even if Connect has already returned on ctx cancellation.
	socketErr := make(chan error, 1)
	opts := transport.SocketOptions{
		WSURL: reg.WSURL,
		UID:   reg.RobotID,
		Token: reg.IMToken,
		OnMessage: func(m transport.BotMessage) {
			c.onMessage(ctx, reg.RobotID, m)
		},
		OnError: func(e error) {
			select {
			case socketErr <- e:
			default:
			}
		},
		Logf: func(format string, args ...any) {
			c.logger.Debug(fmt.Sprintf(format, args...))
		},
	}

	newSocket := c.newSocket
	if newSocket == nil {
		newSocket = func(o transport.SocketOptions) socketConn { return transport.NewSocket(o) }
	}
	sock := newSocket(opts)
	if err := sock.Connect(ctx); err != nil {
		return fmt.Errorf("octo: open socket: %w", err)
	}
	defer sock.Disconnect()

	select {
	case <-ctx.Done():
		return nil
	case err := <-socketErr:
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("octo: socket terminated: %w", err)
	}
}

// onMessage bridges a decoded WuKongIM message to the engine router. It applies
// the same ingress guards the old hub did BEFORE handing off, then normalizes to
// channel.InboundMessage. A handler error is an infrastructure failure; it is
// logged (the socket keeps running — one bad message must not tear the link
// down, unlike a terminal socket error). Product drops are decided inside the
// router.
func (c *octoChannel) onMessage(ctx context.Context, robotID string, m transport.BotMessage) {
	// Ignore traffic that isn't a real inbound user message:
	//   1. The bot's own messages echoed back to its socket (from_uid == robot
	//      id). Without this every outbound reply loops back as a new
	//      unbound-user message and triggers a bogus binding prompt.
	//   2. Non-conversation channels. Octo emits system/command channels (e.g.
	//      channel_type 8 on connect) that aren't DM/group/topic.
	if m.FromUID == robotID {
		return
	}
	if !isConversationChannel(m.ChannelType) {
		return
	}
	if err := c.handler(ctx, c.channelMessage(robotID, m)); err != nil {
		c.logger.Error("octo: inbound handler failed", "robot_id", robotID, "err", err.Error())
	}
}

// channelMessage normalizes a decoded WuKongIM message into the cross-platform
// channel.InboundMessage. The bot's own @mention is stripped so downstream /new
// and /issue parsing sees a clean leading body; the raw platform payload is
// stashed in Raw so the resolvers can read octo-specific fields.
func (c *octoChannel) channelMessage(robotID string, m transport.BotMessage) channel.InboundMessage {
	body := stripBotMentions(m.Payload.Content, robotID, mentionEntities(m.Payload.Mention))
	forceFresh := false
	if cmd, ok := parseFreshSessionCommand(body); ok {
		body = cmd.Body
		forceFresh = true
	}
	raw, _ := json.Marshal(octoRawEvent{
		RobotID:     robotID,
		ChannelType: int(m.ChannelType),
	})
	return channel.InboundMessage{
		EventID:        m.MessageID,
		MessageID:      m.MessageID,
		Type:           channel.MsgTypeText,
		Text:           body,
		AddressedToBot: addressedToBot(robotID, m),
		ForceFresh:     forceFresh,
		Source: channel.Source{
			ChannelType: TypeOcto,
			ChatID:      m.ChannelID,
			ChatType:    chatTypeFromChannel(m.ChannelType),
			SenderID:    m.FromUID,
		},
		Raw: raw,
	}
}

// octoRawEvent is the octo-specific payload stashed in InboundMessage.Raw. The
// resolvers read it back for the routing key (robot_id) and the WuKongIM channel
// type (needed to reconstruct the outbound channel type on the binding config).
type octoRawEvent struct {
	RobotID     string `json:"robot_id"`
	ChannelType int    `json:"channel_type"`
}

// chatTypeFromChannel maps the WuKongIM channel type onto the normalized
// cross-platform ChatType. DM is p2p; group and community topic are both group
// (multi-party) conversations gated by the @bot mention filter.
func chatTypeFromChannel(t transport.ChannelType) channel.ChatType {
	if t == transport.ChannelDM {
		return channel.ChatTypeP2P
	}
	return channel.ChatTypeGroup
}

// isConversationChannel reports whether a channel type is a real user
// conversation (DM, group, or community topic). Octo also emits system/command
// channels that must not be dispatched as user messages.
func isConversationChannel(t transport.ChannelType) bool {
	switch t {
	case transport.ChannelDM, transport.ChannelGroup, transport.ChannelTopic:
		return true
	default:
		return false
	}
}

// addressedToBot reports whether a group message targets the bot (@mention).
// DMs are always addressed; for groups we check the mention uid list.
func addressedToBot(robotID string, m transport.BotMessage) bool {
	if m.ChannelType == transport.ChannelDM {
		return true
	}
	if m.Payload.Mention == nil {
		return false
	}
	return slices.Contains(m.Payload.Mention.UIDs, robotID)
}

// mentionEntities returns the entity list from a mention payload, or nil if the
// payload is nil, so callers pass the result straight into stripBotMentions.
func mentionEntities(m *transport.MentionPayload) []transport.MentionEntity {
	if m == nil {
		return nil
	}
	return m.Entities
}

// ChannelDeps are the shared dependencies the Octo Factory closes over. The
// engine inbound handler is supplied per-build via channel.Config.Handler; the
// Decrypter turns the installation's stored ciphertext bot token into plaintext.
type ChannelDeps struct {
	Decrypt Decrypter
	Sender  MessageSender
	Logger  *slog.Logger
}

// RegisterOcto registers the per-installation Octo Factory so the
// engine.Supervisor builds + supervises one octoChannel per active Octo
// installation. "Adding Octo inbound" is this call plus the adapter — no engine
// edit (the same contract as lark.RegisterFeishu / slack.RegisterSlack).
func RegisterOcto(reg *channel.Registry, deps ChannelDeps) {
	reg.Register(TypeOcto, newOctoFactory(deps))
}

func newOctoFactory(deps ChannelDeps) channel.Factory {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	sender := deps.Sender
	if sender == nil {
		sender = NewMessageSender()
	}
	return func(cfg channel.Config) (channel.Channel, error) {
		creds, err := decodeCredentials(cfg.Raw, deps.Decrypt)
		if err != nil {
			return nil, fmt.Errorf("octo: decode installation config: %w", err)
		}
		if creds.BotToken == "" {
			return nil, errors.New("octo: installation has no bot token")
		}
		return &octoChannel{
			creds:   creds,
			handler: cfg.Handler,
			sender:  sender,
			logger:  logger,
		}, nil
	}
}
