package octo

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/integrations/octo/transport"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// octoRawJSON builds the octo-specific InboundMessage.Raw the adapter stashes, so
// replier tests can exercise the real channel-type-from-Raw path.
func octoRawJSON(robotID string, channelType int) json.RawMessage {
	raw, _ := json.Marshal(octoRawEvent{RobotID: robotID, ChannelType: channelType})
	return raw
}

// capturingSender records every Send call so a test can assert on the channel,
// type, and content of the outbound reply.
type capturingSender struct {
	calls []capturedSend
	err   error
}

type capturedSend struct {
	channelID   string
	channelType transport.ChannelType
	content     string
}

func (s *capturingSender) Send(_ context.Context, _, _, channelID string, channelType transport.ChannelType, content string) (*transport.SendMessageResult, error) {
	s.calls = append(s.calls, capturedSend{channelID: channelID, channelType: channelType, content: content})
	if s.err != nil {
		return nil, s.err
	}
	return &transport.SendMessageResult{MessageID: "m1"}, nil
}

// fakeMinter records the Mint call and returns a fixed raw token.
type fakeMinter struct {
	calls    int
	gotWS    pgtype.UUID
	gotInst  pgtype.UUID
	gotUID   UID
	rawToken string
	err      error
}

func (m *fakeMinter) Mint(_ context.Context, workspaceID, installationID pgtype.UUID, uid UID) (BindingToken, error) {
	m.calls++
	m.gotWS = workspaceID
	m.gotInst = installationID
	m.gotUID = uid
	if m.err != nil {
		return BindingToken{}, m.err
	}
	return BindingToken{Raw: m.rawToken}, nil
}

// fakeLoader returns a fixed installation row for the replier to read credentials
// from. It mirrors the row the InstallationResolver would have stashed.
type fakeLoader struct {
	row db.ChannelInstallation
	err error
}

func (l fakeLoader) GetChannelInstallation(_ context.Context, _ db.GetChannelInstallationParams) (db.ChannelInstallation, error) {
	return l.row, l.err
}

// replierInst is the resolved installation the Router passes to Reply.
func replierInst() engine.ResolvedInstallation {
	return engine.ResolvedInstallation{
		ID:          validUUID(0xAA),
		WorkspaceID: validUUID(0xBB),
		Active:      true,
	}
}

// replierLoader returns the matching channel_installation row for replierInst.
func replierLoader() fakeLoader {
	return fakeLoader{row: db.ChannelInstallation{
		ID:          validUUID(0xAA),
		WorkspaceID: validUUID(0xBB),
		Status:      "active",
		ChannelType: string(TypeOcto),
		Config:      octoConfigJSON("robot-1", "https://im.example/api", "bf_x"),
	}}
}

func TestOutcomeReplier_NeedsBinding_DMsSenderWithLink(t *testing.T) {
	minter := &fakeMinter{rawToken: "raw-token-123"}
	sender := &capturingSender{}
	r := NewOutcomeReplier(OutcomeReplierConfig{
		Loader:    replierLoader(),
		Minter:    minter,
		Decryptor: fakeDecryptor{token: "bot-token"},
		Sender:    sender,
		PublicURL: "https://multica.example/",
	})

	inst := replierInst()
	msg := channel.InboundMessage{Source: channel.Source{ChatID: "grp_1", ChatType: channel.ChatTypeGroup, SenderID: "uid_42"}}
	res := engine.Result{Outcome: engine.OutcomeNeedsBinding, InstallationID: inst.ID, Sender: "uid_42"}

	r.Reply(context.Background(), inst, msg, res)

	if minter.calls != 1 {
		t.Fatalf("Mint called %d times, want 1", minter.calls)
	}
	if minter.gotUID != "uid_42" {
		t.Errorf("Mint uid = %q, want uid_42", minter.gotUID)
	}
	if minter.gotWS != inst.WorkspaceID || minter.gotInst != inst.ID {
		t.Errorf("Mint got wrong workspace/installation ids")
	}
	if len(sender.calls) != 1 {
		t.Fatalf("Send called %d times, want 1", len(sender.calls))
	}
	got := sender.calls[0]
	// The binding prompt must be a private DM to the SENDER, never the group.
	if got.channelID != "uid_42" {
		t.Errorf("DM channel = %q, want uid_42 (sender), not the group", got.channelID)
	}
	if got.channelType != transport.ChannelDM {
		t.Errorf("DM channel type = %d, want %d (DM)", got.channelType, transport.ChannelDM)
	}
	wantURL := "https://multica.example/octo/bind?token=raw-token-123"
	if !strings.Contains(got.content, wantURL) {
		t.Errorf("binding prompt %q does not contain link %q", got.content, wantURL)
	}
}

func TestOutcomeReplier_NeedsBinding_NoPublicURL_Downgrades(t *testing.T) {
	minter := &fakeMinter{rawToken: "x"}
	sender := &capturingSender{}
	r := NewOutcomeReplier(OutcomeReplierConfig{
		Loader:    replierLoader(),
		Minter:    minter,
		Decryptor: fakeDecryptor{token: "bot-token"},
		Sender:    sender,
	})

	inst := replierInst()
	res := engine.Result{Outcome: engine.OutcomeNeedsBinding, InstallationID: inst.ID, Sender: "uid_42"}
	r.Reply(context.Background(), inst, channel.InboundMessage{}, res)

	if minter.calls != 0 {
		t.Errorf("Mint should not be called without a public URL, got %d calls", minter.calls)
	}
	if len(sender.calls) != 0 {
		t.Errorf("no DM should be sent without a public URL, got %d", len(sender.calls))
	}
}

func TestOutcomeReplier_AgentOffline_NotifiesChannel(t *testing.T) {
	sender := &capturingSender{}
	r := NewOutcomeReplier(OutcomeReplierConfig{
		Loader:    replierLoader(),
		Minter:    &fakeMinter{},
		Decryptor: fakeDecryptor{token: "bot-token"},
		Sender:    sender,
		PublicURL: "https://multica.example",
	})

	inst := replierInst()
	msg := channel.InboundMessage{
		Source: channel.Source{ChatID: "grp_1", ChatType: channel.ChatTypeGroup, SenderID: "uid_42"},
		Raw:    octoRawJSON("robot-1", int(transport.ChannelGroup)),
	}
	r.Reply(context.Background(), inst, msg, engine.Result{Outcome: engine.OutcomeAgentOffline})

	if len(sender.calls) != 1 {
		t.Fatalf("Send called %d times, want 1", len(sender.calls))
	}
	got := sender.calls[0]
	// The offline notice goes back to the originating channel (the group).
	if got.channelID != "grp_1" {
		t.Errorf("offline notice channel = %q, want grp_1 (originating channel)", got.channelID)
	}
	if got.channelType != transport.ChannelGroup {
		t.Errorf("group notice channel type = %d, want %d (group)", got.channelType, transport.ChannelGroup)
	}
	if got.content != agentOfflineCopy {
		t.Errorf("offline notice = %q, want %q", got.content, agentOfflineCopy)
	}
}

// TestOutcomeReplier_TopicPreservesChannelType guards that an offline notice
// triggered by a community-topic message (WuKongIM type 5) is addressed with
// ChannelTopic, not collapsed to ChannelGroup — the real type is read from
// msg.Raw, which Source.ChatType (p2p/group only) cannot carry.
func TestOutcomeReplier_TopicPreservesChannelType(t *testing.T) {
	sender := &capturingSender{}
	r := NewOutcomeReplier(OutcomeReplierConfig{
		Loader:    replierLoader(),
		Minter:    &fakeMinter{},
		Decryptor: fakeDecryptor{token: "bot-token"},
		Sender:    sender,
		PublicURL: "https://multica.example",
	})

	msg := channel.InboundMessage{
		Source: channel.Source{ChatID: "topic_1", ChatType: channel.ChatTypeGroup, SenderID: "uid_42"},
		Raw:    octoRawJSON("robot-1", int(transport.ChannelTopic)),
	}
	r.Reply(context.Background(), replierInst(), msg, engine.Result{Outcome: engine.OutcomeAgentOffline})

	if len(sender.calls) != 1 {
		t.Fatalf("Send called %d times, want 1", len(sender.calls))
	}
	if got := sender.calls[0]; got.channelType != transport.ChannelTopic {
		t.Errorf("topic notice channel type = %d, want %d (topic); type was collapsed", got.channelType, transport.ChannelTopic)
	}
}

func TestOutcomeReplier_AgentArchived_NotifiesChannel(t *testing.T) {
	sender := &capturingSender{}
	r := NewOutcomeReplier(OutcomeReplierConfig{
		Loader:    replierLoader(),
		Minter:    &fakeMinter{},
		Decryptor: fakeDecryptor{token: "bot-token"},
		Sender:    sender,
		PublicURL: "https://multica.example",
	})

	msg := channel.InboundMessage{Source: channel.Source{ChatID: "dm_1", ChatType: channel.ChatTypeP2P}}
	r.Reply(context.Background(), replierInst(), msg,
		engine.Result{Outcome: engine.OutcomeAgentArchived})

	if len(sender.calls) != 1 || sender.calls[0].content != agentArchivedCopy {
		t.Fatalf("expected one archived notice with archived copy, got %+v", sender.calls)
	}
}

func TestOutcomeReplier_IngestedAndDropped_Silent(t *testing.T) {
	sender := &capturingSender{}
	minter := &fakeMinter{}
	r := NewOutcomeReplier(OutcomeReplierConfig{
		Loader:    replierLoader(),
		Minter:    minter,
		Decryptor: fakeDecryptor{token: "bot-token"},
		Sender:    sender,
		PublicURL: "https://multica.example",
	})

	for _, oc := range []engine.Outcome{engine.OutcomeIngested, engine.OutcomeDropped} {
		msg := channel.InboundMessage{Source: channel.Source{ChatID: "c"}}
		r.Reply(context.Background(), replierInst(), msg, engine.Result{Outcome: oc})
	}
	if len(sender.calls) != 0 || minter.calls != 0 {
		t.Errorf("ingested/dropped must produce no reply, got %d sends %d mints", len(sender.calls), minter.calls)
	}
}

func TestOutcomeReplier_MissingDeps_Noop(t *testing.T) {
	// A nil minter forces the noop replier even with the other deps present.
	r := NewOutcomeReplier(OutcomeReplierConfig{
		Loader:    replierLoader(),
		Decryptor: fakeDecryptor{token: "t"},
		Sender:    &capturingSender{},
		PublicURL: "https://multica.example",
	})
	if _, ok := r.(*noopReplier); !ok {
		t.Fatalf("expected noop replier when minter is nil, got %T", r)
	}
}

func TestOutcomeReplier_BestEffort_SwallowsSendError(t *testing.T) {
	// A send failure must not panic or propagate — Reply is best-effort.
	sender := &capturingSender{err: errors.New("octo down")}
	r := NewOutcomeReplier(OutcomeReplierConfig{
		Loader:    replierLoader(),
		Minter:    &fakeMinter{rawToken: "x"},
		Decryptor: fakeDecryptor{token: "bot-token"},
		Sender:    sender,
		PublicURL: "https://multica.example",
	})
	// Should not panic.
	r.Reply(context.Background(), replierInst(), channel.InboundMessage{},
		engine.Result{Outcome: engine.OutcomeNeedsBinding, Sender: "uid_42"})
}
