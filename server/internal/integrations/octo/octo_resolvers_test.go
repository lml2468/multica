package octo_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/integrations/octo"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// These tests exercise the Octo ResolverSet through its public constructor
// against a real Postgres, mirroring the coverage Slack/Feishu have for their
// resolver seams. They pin the two mechanism changes convergence introduced that
// the FK previously enforced for free: routing by robot_id (config->>'app_id')
// and the explicit workspace-membership gate that replaced the dropped member FK.

// octoInboundRaw builds the octo-specific InboundMessage.Raw the adapter stashes
// (robot_id routing key + WuKongIM channel type), so a resolver test can feed the
// same shape the live channel produces.
func octoInboundRaw(t *testing.T, robotID string, channelType int) []byte {
	t.Helper()
	raw, err := json.Marshal(struct {
		RobotID     string `json:"robot_id"`
		ChannelType int    `json:"channel_type"`
	}{robotID, channelType})
	if err != nil {
		t.Fatalf("marshal raw: %v", err)
	}
	return raw
}

// seedInstallation upserts an active Octo installation and returns it.
func seedInstallation(t *testing.T, q *db.Queries, wsID, userID, agentID pgtype.UUID, robotID string) db.ChannelInstallation {
	t.Helper()
	svc, err := octo.NewInstallationService(q, newBox(t))
	if err != nil {
		t.Fatalf("NewInstallationService: %v", err)
	}
	inst, err := svc.Upsert(context.Background(), octo.InstallationParams{
		WorkspaceID:     wsID,
		AgentID:         agentID,
		BotToken:        "bf_x",
		RobotID:         robotID,
		APIURL:          "https://im.example/api",
		InstallerUserID: userID,
	})
	if err != nil {
		t.Fatalf("seed installation: %v", err)
	}
	return inst
}

// resolvedFromRow adapts a db.ChannelInstallation into the engine.ResolvedInstallation
// the Router hands the identity/session resolvers after routing.
func resolvedFromRow(inst db.ChannelInstallation) engine.ResolvedInstallation {
	return engine.ResolvedInstallation{
		ID:              inst.ID,
		WorkspaceID:     inst.WorkspaceID,
		AgentID:         inst.AgentID,
		InstallerUserID: inst.InstallerUserID,
		Active:          inst.Status == "active",
		Platform:        inst,
	}
}

func TestResolver_Installation_RoutesByRobotID(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	robotID := "robot_" + randToken()
	inst := seedInstallation(t, q, wsID, userID, agentID, robotID)

	set := octo.NewOctoResolverSet(q, testPool, nil)
	msg := channel.InboundMessage{
		Source: channel.Source{ChannelType: octo.TypeOcto, ChatID: "chan-1"},
		Raw:    octoInboundRaw(t, robotID, 1),
	}
	got, err := set.Installation.ResolveInstallation(context.Background(), msg)
	if err != nil {
		t.Fatalf("ResolveInstallation: %v", err)
	}
	if got.ID != inst.ID || got.WorkspaceID != wsID || got.AgentID != agentID {
		t.Errorf("resolved wrong installation: %+v (want id=%v ws=%v agent=%v)", got, inst.ID, wsID, agentID)
	}
	if !got.Active {
		t.Error("active installation resolved as inactive")
	}
}

func TestResolver_Installation_UnknownRobotID(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	set := octo.NewOctoResolverSet(q, testPool, nil)
	msg := channel.InboundMessage{
		Source: channel.Source{ChannelType: octo.TypeOcto, ChatID: "chan-x"},
		Raw:    octoInboundRaw(t, "robot_"+randToken(), 1),
	}
	if _, err := set.Installation.ResolveInstallation(context.Background(), msg); !errors.Is(err, engine.ErrInstallationNotFound) {
		t.Errorf("unknown robot_id err = %v, want ErrInstallationNotFound", err)
	}
}

func TestResolver_Identity_UnboundSender(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	inst := seedInstallation(t, q, wsID, userID, agentID, "robot_"+randToken())

	set := octo.NewOctoResolverSet(q, testPool, nil)
	msg := channel.InboundMessage{Source: channel.Source{ChannelType: octo.TypeOcto, SenderID: "uid-unbound"}}
	if _, err := set.Identity.ResolveSender(context.Background(), resolvedFromRow(inst), msg); !errors.Is(err, engine.ErrSenderUnbound) {
		t.Errorf("unbound sender err = %v, want ErrSenderUnbound", err)
	}
}

func TestResolver_Identity_BoundMemberResolves(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	inst := seedInstallation(t, q, wsID, userID, agentID, "robot_"+randToken())

	// Bind uid-abc -> the workspace member via the real redemption path.
	bindSvc := octo.NewBindingTokenService(q, testPool)
	tok, err := bindSvc.Mint(context.Background(), wsID, inst.ID, "uid-abc")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if _, err := bindSvc.RedeemAndBind(context.Background(), tok.Raw, userID); err != nil {
		t.Fatalf("redeem: %v", err)
	}

	set := octo.NewOctoResolverSet(q, testPool, nil)
	msg := channel.InboundMessage{Source: channel.Source{ChannelType: octo.TypeOcto, SenderID: "uid-abc"}}
	got, err := set.Identity.ResolveSender(context.Background(), resolvedFromRow(inst), msg)
	if err != nil {
		t.Fatalf("ResolveSender: %v", err)
	}
	if got.UserID != userID {
		t.Errorf("resolved user = %v, want %v", got.UserID, userID)
	}
}

// TestResolver_Identity_BoundNonMember is the per-message counterpart of the
// binding-side gate: a binding whose user was removed from the workspace after
// binding must resolve to ErrSenderNotMember (the FK that used to enforce this
// was dropped from channel_user_binding; the resolver re-checks explicitly).
func TestResolver_Identity_BoundNonMember(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	inst := seedInstallation(t, q, wsID, userID, agentID, "robot_"+randToken())

	bindSvc := octo.NewBindingTokenService(q, testPool)
	tok, err := bindSvc.Mint(context.Background(), wsID, inst.ID, "uid-gone")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if _, err := bindSvc.RedeemAndBind(context.Background(), tok.Raw, userID); err != nil {
		t.Fatalf("redeem: %v", err)
	}
	// Remove the member while the binding row survives (channel_user_binding has
	// no member FK, so it is not cascaded).
	if _, err := testPool.Exec(context.Background(),
		`DELETE FROM member WHERE workspace_id=$1 AND user_id=$2`, wsID, userID); err != nil {
		t.Fatalf("delete member: %v", err)
	}

	set := octo.NewOctoResolverSet(q, testPool, nil)
	msg := channel.InboundMessage{Source: channel.Source{ChannelType: octo.TypeOcto, SenderID: "uid-gone"}}
	if _, err := set.Identity.ResolveSender(context.Background(), resolvedFromRow(inst), msg); !errors.Is(err, engine.ErrSenderNotMember) {
		t.Errorf("non-member sender err = %v, want ErrSenderNotMember", err)
	}
}

// TestResolver_Dedup_TwoPhase pins the claim/mark/release owner-fence semantics
// over the generic channel_inbound_message_dedup table.
func TestResolver_Dedup_TwoPhase(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	inst := seedInstallation(t, q, wsID, userID, agentID, "robot_"+randToken())
	ctx := context.Background()
	set := octo.NewOctoResolverSet(q, testPool, nil)
	msgID := "msg-" + randToken()

	// First claim mints a token.
	tok, err := set.Dedup.Claim(ctx, inst.ID, msgID)
	if err != nil {
		t.Fatalf("first Claim: %v", err)
	}
	// A second claim of the same in-flight message is a duplicate.
	if _, err := set.Dedup.Claim(ctx, inst.ID, msgID); !errors.Is(err, engine.ErrDuplicate) {
		t.Errorf("second Claim err = %v, want ErrDuplicate", err)
	}
	// Mark finalizes; a later claim still sees it as a duplicate (terminal).
	if err := set.Dedup.Mark(ctx, inst.ID, msgID, tok); err != nil {
		t.Fatalf("Mark: %v", err)
	}
	if _, err := set.Dedup.Claim(ctx, inst.ID, msgID); !errors.Is(err, engine.ErrDuplicate) {
		t.Errorf("post-Mark Claim err = %v, want ErrDuplicate", err)
	}

	// Release on a fresh message allows a re-claim (the crash-recovery path).
	msgID2 := "msg-" + randToken()
	tok2, err := set.Dedup.Claim(ctx, inst.ID, msgID2)
	if err != nil {
		t.Fatalf("Claim msg2: %v", err)
	}
	if err := set.Dedup.Release(ctx, inst.ID, msgID2, tok2); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := set.Dedup.Claim(ctx, inst.ID, msgID2); err != nil {
		t.Errorf("re-Claim after Release err = %v, want nil", err)
	}
}

// TestResolver_Session_BindsChannelAndPersistsType exercises EnsureSession +
// AppendMessage on the shared engine.ChatSession and confirms the WuKongIM
// channel type (topic=5) is persisted into the binding config so the outbound
// path can recover it (the topic→group collapse is lossless).
func TestResolver_Session_BindsChannelAndPersistsType(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	inst := seedInstallation(t, q, wsID, userID, agentID, "robot_"+randToken())
	ctx := context.Background()
	set := octo.NewOctoResolverSet(q, testPool, nil)

	msg := channel.InboundMessage{
		MessageID: "m1",
		Text:      "hello",
		Source: channel.Source{
			ChannelType: octo.TypeOcto,
			ChatID:      "chan-topic",
			ChatType:    channel.ChatTypeGroup,
			SenderID:    "uid-abc",
		},
		Raw: octoInboundRaw(t, "robot-x", 5), // community topic
	}
	sessionID, err := set.Session.EnsureSession(ctx, engine.EnsureSessionParams{
		Installation: resolvedFromRow(inst),
		Sender:       userID,
		Message:      msg,
	})
	if err != nil {
		t.Fatalf("EnsureSession: %v", err)
	}
	if !sessionID.Valid {
		t.Fatal("EnsureSession returned an invalid session id")
	}
	// A second EnsureSession for the same channel id is idempotent (one session
	// per (installation, channel)).
	again, err := set.Session.EnsureSession(ctx, engine.EnsureSessionParams{
		Installation: resolvedFromRow(inst),
		Sender:       userID,
		Message:      msg,
	})
	if err != nil {
		t.Fatalf("EnsureSession (2nd): %v", err)
	}
	if again != sessionID {
		t.Errorf("EnsureSession not idempotent: %v != %v", again, sessionID)
	}

	// The binding persisted octo_channel_type=5 so outbound can address the topic.
	var cfg []byte
	if err := testPool.QueryRow(ctx,
		`SELECT config FROM channel_chat_session_binding WHERE chat_session_id=$1 AND channel_type='octo'`,
		sessionID).Scan(&cfg); err != nil {
		t.Fatalf("read binding config: %v", err)
	}
	var blob struct {
		OctoChannelType int `json:"octo_channel_type"`
	}
	if err := json.Unmarshal(cfg, &blob); err != nil {
		t.Fatalf("decode binding config %s: %v", cfg, err)
	}
	if blob.OctoChannelType != 5 {
		t.Errorf("persisted octo_channel_type = %d, want 5 (topic must survive the chat_type collapse)", blob.OctoChannelType)
	}

	// AppendMessage writes the user message under the dedup owner-fence.
	claim, err := set.Dedup.Claim(ctx, inst.ID, msg.MessageID)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if _, err := set.Session.AppendMessage(ctx, engine.AppendParams{
		SessionID:      sessionID,
		Sender:         userID,
		InstallationID: inst.ID,
		Message:        msg,
		ClaimToken:     claim,
	}); err != nil {
		t.Fatalf("AppendMessage: %v", err)
	}
}
