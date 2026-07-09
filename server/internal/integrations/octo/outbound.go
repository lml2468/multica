package octo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/octo/transport"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const outboundEventTimeout = 10 * time.Second

// PatcherQueries is the subset of generated queries the outbound patcher needs.
type PatcherQueries interface {
	GetChannelChatSessionBindingBySession(ctx context.Context, arg db.GetChannelChatSessionBindingBySessionParams) (db.ChannelChatSessionBinding, error)
	GetChannelInstallation(ctx context.Context, arg db.GetChannelInstallationParams) (db.ChannelInstallation, error)
	CreateChannelOutboundCardMessage(ctx context.Context, arg db.CreateChannelOutboundCardMessageParams) (db.ChannelOutboundCardMessage, error)
}

// TokenDecryptor decrypts an installation's stored bot token ciphertext. An
// interface so the patcher can be unit-tested without secretbox.
type TokenDecryptor interface {
	DecryptBotToken(inst db.ChannelInstallation) (string, error)
}

// MessageSender sends an outbound message to Octo for a given installation.
// Production uses octoMessageSender (a thin wrapper over transport.HTTPClient); tests
// provide a fake. Returns the server-assigned message id/seq.
type MessageSender interface {
	Send(ctx context.Context, apiURL, botToken, channelID string, channelType transport.ChannelType, content string) (*transport.SendMessageResult, error)
}

// octoMessageSender is the production MessageSender. It caches one
// transport.HTTPClient per (api_url, token) so repeated replies/edits to the
// same installation reuse the underlying connection pool instead of paying a
// fresh TCP+TLS handshake per message.
type octoMessageSender struct {
	mu      sync.Mutex
	clients map[string]*transport.HTTPClient
}

// NewMessageSender returns the production MessageSender.
func NewMessageSender() MessageSender {
	return &octoMessageSender{clients: make(map[string]*transport.HTTPClient)}
}

func (s *octoMessageSender) client(apiURL, botToken string) *transport.HTTPClient {
	key := apiURL + "\x00" + botToken
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.clients[key]
	if !ok {
		c = transport.NewHTTPClient(apiURL, botToken)
		s.clients[key] = c
	}
	return c
}

func (s *octoMessageSender) Send(ctx context.Context, apiURL, botToken, channelID string, channelType transport.ChannelType, content string) (*transport.SendMessageResult, error) {
	return s.client(apiURL, botToken).SendMessage(ctx, transport.SendMessageParams{
		ChannelID:   channelID,
		ChannelType: channelType,
		Content:     content,
	})
}

// Patcher subscribes to chat task events and relays agent output back to Octo.
// On chat:done it sends the agent's reply; on task:failed it sends a short error
// notice. Octo renders markdown natively, so replies go out as plain text/markdown
// (no interactive-card rendering like Lark).
type Patcher struct {
	queries   PatcherQueries
	decryptor TokenDecryptor
	sender    MessageSender
	logger    *slog.Logger
}

// NewPatcher constructs the outbound Patcher.
func NewPatcher(queries PatcherQueries, decryptor TokenDecryptor, sender MessageSender, logger *slog.Logger) *Patcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Patcher{queries: queries, decryptor: decryptor, sender: sender, logger: logger}
}

// Register subscribes the patcher to the event bus.
func (p *Patcher) Register(bus *events.Bus) {
	bus.Subscribe(protocol.EventChatDone, p.handleEvent)
	bus.Subscribe(protocol.EventTaskFailed, p.handleEvent)
}

// handleEvent runs each event on its own short-lived context. Outbound delivery
// is best-effort: a failure is logged, never propagated (the chat task is
// already durable; the user simply doesn't see this particular reply).
func (p *Patcher) handleEvent(e events.Event) {
	ctx, cancel := context.WithTimeout(context.Background(), outboundEventTimeout)
	defer cancel()
	if err := p.processEvent(ctx, e); err != nil {
		p.logger.Error("octo outbound: process event failed", "type", e.Type, "err", err.Error())
	}
}

func (p *Patcher) processEvent(ctx context.Context, e events.Event) error {
	taskID, chatSessionID, ok := taskAndSessionFromEvent(e)
	if !ok || !chatSessionID.Valid {
		// No task, or an issue/autopilot task with no chat session — not ours.
		return nil
	}

	binding, err := p.queries.GetChannelChatSessionBindingBySession(ctx, db.GetChannelChatSessionBindingBySessionParams{
		ChatSessionID: chatSessionID,
		ChannelType:   string(TypeOcto),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Web-only or another platform's chat session — not an Octo target.
			return nil
		}
		return fmt.Errorf("lookup chat session binding: %w", err)
	}

	inst, err := p.queries.GetChannelInstallation(ctx, db.GetChannelInstallationParams{
		ID:          binding.InstallationID,
		ChannelType: string(TypeOcto),
	})
	if err != nil {
		return fmt.Errorf("load installation: %w", err)
	}
	if InstallationStatus(inst.Status) != InstallationActive {
		// Revoked between trigger and event; nothing to send.
		return nil
	}

	creds, err := decodeCredentials(inst.Config, nil)
	if err != nil {
		return fmt.Errorf("decode installation config: %w", err)
	}
	token, err := p.decryptor.DecryptBotToken(inst)
	if err != nil {
		return fmt.Errorf("decrypt bot token: %w", err)
	}

	switch e.Type {
	case protocol.EventChatDone:
		return p.sendReply(ctx, creds.APIURL, token, binding, taskID, chatDoneContent(e.Payload))
	case protocol.EventTaskFailed:
		return p.sendReply(ctx, creds.APIURL, token, binding, taskID, "⚠️ "+failureMessageFromPayload(e.Payload))
	}
	return nil
}

// sendReply sends content to the bound Octo channel and records the sent message
// (keyed by task) so a later streaming edit can target it. The WuKongIM
// (message_id, message_seq) pair is encoded into the single card-message-id slot
// as "<message_id>:<seq>". Empty content is dropped — better to show nothing than
// a bare "Done.".
func (p *Patcher) sendReply(ctx context.Context, apiURL, token string, binding db.ChannelChatSessionBinding, taskID pgtype.UUID, content string) error {
	if content == "" {
		return nil
	}
	res, err := p.sender.Send(ctx, apiURL, token, binding.ChannelChatID, octoChannelType(binding), content)
	if err != nil {
		return fmt.Errorf("send message: %w", err)
	}

	// Record the sent message so future streaming edits can find it. Best-effort:
	// a failure here only loses the edit anchor, not the delivered message.
	var seq uint32
	msgID := ""
	if res != nil {
		seq = res.MessageSeq
		msgID = res.MessageID
	}
	if _, err := p.queries.CreateChannelOutboundCardMessage(ctx, db.CreateChannelOutboundCardMessageParams{
		ChatSessionID:        binding.ChatSessionID,
		TaskID:               taskID,
		ChannelType:          string(TypeOcto),
		ChannelChatID:        binding.ChannelChatID,
		ChannelCardMessageID: encodeCardMessageID(msgID, seq),
		Status:               "final",
	}); err != nil {
		p.logger.Warn("octo outbound: record sent message failed",
			"task_id", uuidString(taskID), "err", err.Error())
	}
	return nil
}

// octoChannelType reads the raw WuKongIM channel type (1=DM, 2=group, 5=topic)
// persisted on the binding config (octo_channel_type). Migration 154 backfills it
// for every folded row and octoBindingConfig writes it for every new binding, so
// the config value is authoritative. The p2p/group default is defense against a
// corrupt JSONB blob only, not a compatibility path.
func octoChannelType(binding db.ChannelChatSessionBinding) transport.ChannelType {
	var cfg octoBindingConfigBlob
	if err := json.Unmarshal(binding.Config, &cfg); err == nil && cfg.OctoChannelType != 0 {
		return transport.ChannelType(cfg.OctoChannelType)
	}
	return chatTypeToChannel(channel.ChatType(binding.ChatType))
}

// encodeCardMessageID packs the WuKongIM (message_id, seq) pair into the single
// channel_card_message_id slot as "<message_id>:<seq>", the encoding migration
// 154 uses.
func encodeCardMessageID(messageID string, seq uint32) string {
	return fmt.Sprintf("%s:%d", messageID, seq)
}

// taskAndSessionFromEvent extracts task_id + chat_session_id from the event,
// handling both the map payload (task events) and the ChatDonePayload struct.
func taskAndSessionFromEvent(e events.Event) (taskID, chatSessionID pgtype.UUID, ok bool) {
	if e.TaskID != "" {
		_ = taskID.Scan(e.TaskID)
	}
	if e.ChatSessionID != "" {
		_ = chatSessionID.Scan(e.ChatSessionID)
	}
	switch pl := e.Payload.(type) {
	case map[string]any:
		if !taskID.Valid {
			if s, _ := pl["task_id"].(string); s != "" {
				_ = taskID.Scan(s)
			}
		}
		if !chatSessionID.Valid {
			if s, _ := pl["chat_session_id"].(string); s != "" {
				_ = chatSessionID.Scan(s)
			}
		}
	case protocol.ChatDonePayload:
		if !taskID.Valid {
			_ = taskID.Scan(pl.TaskID)
		}
		if !chatSessionID.Valid {
			_ = chatSessionID.Scan(pl.ChatSessionID)
		}
	}
	return taskID, chatSessionID, taskID.Valid
}

func chatDoneContent(payload any) string {
	switch pl := payload.(type) {
	case protocol.ChatDonePayload:
		return pl.Content
	case map[string]any:
		if s, ok := pl["content"].(string); ok {
			return s
		}
	}
	return ""
}

// failureMessageFromPayload builds the user-facing text for a task:failed
// event. Precedence:
//  1. The explicit error / error_message string (the redacted detail the
//     daemon reported) — most actionable.
//  2. A friendly Chinese description of the coarse failure_reason classifier.
//  3. A generic fallback when neither is present.
//
// The IM user should never be left with a bare "运行失败" when the backend
// actually knows what went wrong.
func failureMessageFromPayload(payload any) string {
	m, ok := payload.(map[string]any)
	if !ok {
		return defaultFailureMessage
	}
	if s, ok := m["error"].(string); ok && s != "" {
		return s
	}
	if s, ok := m["error_message"].(string); ok && s != "" {
		return s
	}
	if reason, ok := m["failure_reason"].(string); ok && reason != "" {
		if desc, ok := failureReasonText[reason]; ok {
			return desc
		}
		// Unknown reason (a classifier value added server-side later):
		// downgrade to the generic message rather than leaking a raw enum.
		return defaultFailureMessage
	}
	return defaultFailureMessage
}

const defaultFailureMessage = "Agent 运行失败，请稍后重试或联系工作区管理员。"

// failureReasonText maps the taskfailure.Reason string values to friendly
// Chinese copy. Keep the keys in sync with server/pkg/taskfailure/failure.go;
// a missing key falls back to defaultFailureMessage, so drift downgrades
// gracefully rather than crashing.
var failureReasonText = map[string]string{
	"queued_expired":                              "任务排队超时，未被任何 runtime 领取。请确认 Agent 的 daemon 在线。",
	"runtime_offline":                             "Agent 的 runtime 当前离线，消息已记录。请确认 daemon 在线后重试。",
	"runtime_recovery":                            "Agent 的 runtime 正在恢复中，请稍后重试。",
	"timeout":                                     "Agent 运行超时，请稍后重试。",
	"iteration_limit":                             "Agent 达到迭代上限，未能完成。请简化请求或重试。",
	"agent_blocked":                               "Agent 被阻塞，无法继续。请联系工作区管理员。",
	"api_invalid_request":                         "请求无效，Agent 无法处理。请调整后重试。",
	"agent_error.provider_auth_or_access":         "模型服务认证失败，请检查 Agent runtime 的 API Key 配置。",
	"agent_error.provider_quota_limit":            "模型服务额度已用尽，请检查账户额度。",
	"agent_error.provider_capacity_or_rate_limit": "模型服务繁忙或触发限流，请稍后重试。",
	"agent_error.provider_server_error":           "模型服务返回错误，请稍后重试。",
	"agent_error.provider_network":                "连接模型服务失败，请检查网络后重试。",
	"agent_error.process_failure":                 "Agent 进程异常退出，请联系工作区管理员。",
	"agent_error.empty_or_unparseable_output":     "Agent 未返回有效结果，请重试。",
	"agent_error.agent_timeout":                   "Agent 运行超时，请稍后重试。",
	"agent_error.context_overflow":                "对话上下文过长，Agent 无法处理。请精简内容后重试。",
	"agent_error.missing_config":                  "Agent runtime 缺少必要配置（如环境变量），请联系工作区管理员。",
	"agent_error.model_not_found_or_unavailable":  "指定的模型不存在或不可用，请检查 Agent 的模型配置。",
	"agent_error.runtime_version_unsupported":     "Agent runtime 版本不受支持，请升级后重试。",
	"agent_error.runtime_missing_executable":      "Agent runtime 缺少所需的可执行文件，请检查安装。",
	"agent_error.unknown":                         defaultFailureMessage,
}
