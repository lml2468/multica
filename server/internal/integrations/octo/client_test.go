package octo_test

import (
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/internal/integrations/octo"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func newBox(t *testing.T) *secretbox.Box {
	t.Helper()
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatalf("rand key: %v", err)
	}
	box, err := secretbox.New(key[:])
	if err != nil {
		t.Fatalf("secretbox.New: %v", err)
	}
	return box
}

func TestInstallationService_UpsertDecryptRoundTrip(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	ctx := context.Background()

	svc, err := octo.NewInstallationService(q, newBox(t))
	if err != nil {
		t.Fatalf("NewInstallationService: %v", err)
	}

	const token = "bf_secret_token_value"
	inst, err := svc.Upsert(ctx, octo.InstallationParams{
		WorkspaceID:     wsID,
		AgentID:         agentID,
		BotToken:        token,
		RobotID:         "robot_" + randToken(),
		BotName:         "Octo-Z",
		OwnerUID:        "owner_x",
		APIURL:          "https://im.example/api",
		WSURL:           "wss://im.example/ws",
		InstallerUserID: userID,
	})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// The config blob must not contain the plaintext token anywhere.
	if strings.Contains(string(inst.Config), token) {
		t.Fatalf("bot token stored in plaintext inside config: %s", inst.Config)
	}
	// robot_id is stored in the routing-key slot so the generic query resolves it.
	if pub := octo.DecodePublicConfig(inst.Config); pub.BotName != "Octo-Z" {
		t.Errorf("DecodePublicConfig bot_name = %q, want Octo-Z", pub.BotName)
	}

	// DecryptBotToken round-trips to the original.
	got, err := svc.DecryptBotToken(inst)
	if err != nil {
		t.Fatalf("DecryptBotToken: %v", err)
	}
	if got != token {
		t.Errorf("decrypted token = %q, want %q", got, token)
	}
}

func TestInstallationService_NilBoxRejected(t *testing.T) {
	if _, err := octo.NewInstallationService(nil, nil); err == nil {
		t.Error("expected error for nil secretbox.Box")
	}
}

// TestInstallationService_RobotAlreadyBound guards the 409 an admin must get when
// binding a bot whose robot_id is already in use by a different agent. The
// deployment-wide (channel_type, config->>'app_id') unique index must surface as
// the typed ErrRobotAlreadyBound, not a raw DB error (→ 500).
func TestInstallationService_RobotAlreadyBound(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	ctx := context.Background()
	svc, _ := octo.NewInstallationService(q, newBox(t))

	robotID := "robot_" + randToken()
	if _, err := svc.Upsert(ctx, octo.InstallationParams{
		WorkspaceID: wsID, AgentID: agentID, BotToken: "bf_a",
		RobotID: robotID, APIURL: "https://im.example/api", InstallerUserID: userID,
	}); err != nil {
		t.Fatalf("first Upsert: %v", err)
	}

	// A second agent binding the SAME bot (same robot_id) must be rejected: the
	// upsert conflict target is (channel_type, app_id), so a different agent with
	// the same robot_id trips the unique index.
	agentID2 := secondAgent(t, wsID, agentID)
	_, err := svc.Upsert(ctx, octo.InstallationParams{
		WorkspaceID: wsID, AgentID: agentID2, BotToken: "bf_b",
		RobotID: robotID, APIURL: "https://im.example/api", InstallerUserID: userID,
	})
	if !errors.Is(err, octo.ErrRobotAlreadyBound) {
		t.Fatalf("second Upsert error = %v, want ErrRobotAlreadyBound", err)
	}
}

// TestInstallationService_ReconfigureSameAgentSucceeds confirms the legitimate
// re-configure path still works: re-binding the SAME robot_id hits ON CONFLICT DO
// UPDATE and must succeed in place.
func TestInstallationService_ReconfigureSameAgentSucceeds(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	ctx := context.Background()
	svc, _ := octo.NewInstallationService(q, newBox(t))

	robotID := "robot_" + randToken()
	first, err := svc.Upsert(ctx, octo.InstallationParams{
		WorkspaceID: wsID, AgentID: agentID, BotToken: "bf_a",
		RobotID: robotID, APIURL: "https://im.example/api", InstallerUserID: userID,
	})
	if err != nil {
		t.Fatalf("first Upsert: %v", err)
	}
	second, err := svc.Upsert(ctx, octo.InstallationParams{
		WorkspaceID: wsID, AgentID: agentID, BotToken: "bf_rotated",
		RobotID: robotID, APIURL: "https://im.example/api", InstallerUserID: userID,
	})
	if err != nil {
		t.Fatalf("reconfigure Upsert: %v", err)
	}
	if first.ID != second.ID {
		t.Errorf("reconfigure created a new row (%v → %v); expected in-place update", first.ID, second.ID)
	}
	if got, _ := svc.DecryptBotToken(second); got != "bf_rotated" {
		t.Errorf("reconfigure did not rotate token, got %q", got)
	}
}

func TestInstallationService_Revoke(t *testing.T) {
	requireDB(t)
	q := db.New(testPool)
	wsID, userID, agentID := fixture(t)
	ctx := context.Background()
	svc, _ := octo.NewInstallationService(q, newBox(t))

	inst, err := svc.Upsert(ctx, octo.InstallationParams{
		WorkspaceID: wsID, AgentID: agentID, BotToken: "bf_x",
		RobotID: "robot_" + randToken(), APIURL: "https://im.example/api",
		InstallerUserID: userID,
	})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := svc.Revoke(ctx, inst.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	active, _ := q.ListAllActiveChannelInstallations(ctx)
	for _, row := range active {
		if row.ID == inst.ID {
			t.Error("revoked installation still active")
		}
	}
}
