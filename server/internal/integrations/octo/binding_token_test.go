package octo_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/octo"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// newInstallation creates an active Octo channel_installation via the service, so
// the binding tests have a real installation id + workspace to bind against.
func newInstallation(t *testing.T, box *secretbox.Box, q *db.Queries, wsID, userID, agentID pgtype.UUID) db.ChannelInstallation {
	t.Helper()
	svc, err := octo.NewInstallationService(q, box)
	if err != nil {
		t.Fatalf("NewInstallationService: %v", err)
	}
	inst, err := svc.Upsert(context.Background(), octo.InstallationParams{
		WorkspaceID:     wsID,
		AgentID:         agentID,
		BotToken:        "bf_x",
		RobotID:         "robot_" + randToken(),
		BotName:         "Octo-Z",
		APIURL:          "https://im.example/api",
		InstallerUserID: userID,
	})
	if err != nil {
		t.Fatalf("newInstallation Upsert: %v", err)
	}
	return inst
}

func TestBindingToken_MintRedeemRoundTrip(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	inst := newInstallation(t, newBox(t), q, wsID, userID, agentID)
	ctx := context.Background()

	svc := octo.NewBindingTokenService(q, testPool)
	tok, err := svc.Mint(ctx, wsID, inst.ID, "octo_uid_1")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if tok.Raw == "" {
		t.Fatal("Mint returned empty raw token")
	}

	got, err := svc.RedeemAndBind(ctx, tok.Raw, userID)
	if err != nil {
		t.Fatalf("RedeemAndBind: %v", err)
	}
	if got.OctoUID != "octo_uid_1" {
		t.Errorf("OctoUID = %q, want octo_uid_1", got.OctoUID)
	}

	// The binding now resolves via the generic channel identity query.
	binding, err := q.GetChannelUserBindingByUserID(ctx, db.GetChannelUserBindingByUserIDParams{
		InstallationID: inst.ID, ChannelUserID: "octo_uid_1",
	})
	if err != nil {
		t.Fatalf("GetChannelUserBindingByUserID after bind: %v", err)
	}
	if binding.MulticaUserID != userID {
		t.Errorf("binding points at wrong user")
	}
	if binding.ChannelType != "octo" {
		t.Errorf("binding channel_type = %q, want octo", binding.ChannelType)
	}
}

func TestBindingToken_SingleUse(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	inst := newInstallation(t, newBox(t), q, wsID, userID, agentID)
	ctx := context.Background()

	svc := octo.NewBindingTokenService(q, testPool)
	tok, err := svc.Mint(ctx, wsID, inst.ID, "octo_uid_2")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if _, err := svc.RedeemAndBind(ctx, tok.Raw, userID); err != nil {
		t.Fatalf("first redeem: %v", err)
	}
	// Second redemption of the same token must fail (consumed).
	if _, err := svc.RedeemAndBind(ctx, tok.Raw, userID); !errors.Is(err, octo.ErrBindingTokenInvalid) {
		t.Errorf("second redeem err = %v, want ErrBindingTokenInvalid", err)
	}
}

func TestBindingToken_Expired(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	inst := newInstallation(t, newBox(t), q, wsID, userID, agentID)
	ctx := context.Background()

	// Clock 20 minutes in the past so the 15-min token is already expired.
	past := func() time.Time { return time.Now().Add(-20 * time.Minute) }
	svc := octo.NewBindingTokenServiceWithClock(q, testPool, past)
	tok, err := svc.Mint(ctx, wsID, inst.ID, "octo_uid_3")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if _, err := svc.RedeemAndBind(ctx, tok.Raw, userID); !errors.Is(err, octo.ErrBindingTokenInvalid) {
		t.Errorf("expired redeem err = %v, want ErrBindingTokenInvalid", err)
	}
}

func TestBindingToken_InvalidToken(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	_, userID, _ := fixture(t)
	ctx := context.Background()
	svc := octo.NewBindingTokenService(q, testPool)
	if _, err := svc.RedeemAndBind(ctx, "not-a-real-token", userID); !errors.Is(err, octo.ErrBindingTokenInvalid) {
		t.Errorf("bogus token err = %v, want ErrBindingTokenInvalid", err)
	}
}

// TestBindingToken_NonMemberStillBinds documents the intentional behavior change
// after convergence: the generalized channel_user_binding has NO member foreign
// key (MUL-3515 §4 dropped cascades), so redeeming a token binds the uid even for
// a user who is not a workspace member. Membership is instead enforced at inbound
// time by the identity resolver (ResolveSender → ErrSenderNotMember). This test
// pins that RedeemAndBind no longer returns ErrBindingNotWorkspaceMember, so the
// gate must live in the resolver.
func TestBindingToken_NonMemberStillBinds(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	inst := newInstallation(t, newBox(t), q, wsID, userID, agentID)
	ctx := context.Background()

	// A second user who is NOT a member of this workspace.
	var outsiderID pgtype.UUID
	if err := testPool.QueryRow(ctx,
		`INSERT INTO "user" (email, name) VALUES ($1, 'Outsider') RETURNING id`,
		"outsider-"+randToken()[:8]+"@example.com").Scan(&outsiderID); err != nil {
		t.Fatalf("create outsider: %v", err)
	}
	t.Cleanup(func() { _, _ = testPool.Exec(context.Background(), `DELETE FROM "user" WHERE id=$1`, outsiderID) })

	svc := octo.NewBindingTokenService(q, testPool)
	tok, err := svc.Mint(ctx, wsID, inst.ID, "octo_uid_4")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if _, err := svc.RedeemAndBind(ctx, tok.Raw, outsiderID); err != nil {
		t.Fatalf("non-member redeem should succeed now (membership gate moved to resolver): %v", err)
	}
}
