package octo_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests exercise the Octo services against a real PostgreSQL instance via
// the generalized channel_* tables. They follow the repo convention: read
// DATABASE_URL, and skip — never fail — when no database is reachable, so the
// suite is a no-op locally without a DB but runs for real in CI.

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://multica:multica@localhost:5432/multica?sslmode=disable"
	}
	if pool, err := pgxpool.New(ctx, dbURL); err == nil {
		if perr := pool.Ping(ctx); perr == nil {
			testPool = pool
		} else {
			fmt.Printf("octo DB tests will skip: database not reachable: %v\n", perr)
			pool.Close()
		}
	} else {
		fmt.Printf("octo DB tests will skip: cannot connect: %v\n", err)
	}
	code := m.Run()
	if testPool != nil {
		testPool.Close()
	}
	os.Exit(code)
}

// requireDB skips a test when no database is configured, so mock-only tests in
// the package still run locally without a database.
func requireDB(t *testing.T) {
	t.Helper()
	if testPool == nil {
		t.Skip("no database available (set DATABASE_URL)")
	}
}

func randToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// fixture creates a throwaway workspace + user + member + agent and returns their
// IDs, registering cleanup that cascades everything away.
func fixture(t *testing.T) (workspaceID, userID, agentID pgtype.UUID) {
	t.Helper()
	ctx := context.Background()

	slug := "octo-test-" + randToken()[:8]
	email := "octo-test-" + randToken()[:8] + "@example.com"

	if err := testPool.QueryRow(ctx,
		`INSERT INTO workspace (name, slug) VALUES ($1, $2) RETURNING id`,
		"Octo Test WS", slug).Scan(&workspaceID); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := testPool.QueryRow(ctx,
		`INSERT INTO "user" (email, name) VALUES ($1, $2) RETURNING id`,
		email, "Octo Tester").Scan(&userID); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if _, err := testPool.Exec(ctx,
		`INSERT INTO member (workspace_id, user_id, role) VALUES ($1, $2, 'owner')`,
		workspaceID, userID); err != nil {
		t.Fatalf("create member: %v", err)
	}
	// agent requires runtime_mode and a NOT NULL runtime_id (migration 004), so
	// create an agent_runtime first.
	var runtimeID pgtype.UUID
	if err := testPool.QueryRow(ctx,
		`INSERT INTO agent_runtime (workspace_id, name, runtime_mode, provider)
		 VALUES ($1, 'Octo Runtime', 'local', 'octo_test') RETURNING id`,
		workspaceID).Scan(&runtimeID); err != nil {
		t.Fatalf("create agent_runtime: %v", err)
	}
	if err := testPool.QueryRow(ctx,
		`INSERT INTO agent (workspace_id, name, runtime_mode, runtime_id)
		 VALUES ($1, $2, 'local', $3) RETURNING id`,
		workspaceID, "Octo Agent", runtimeID).Scan(&agentID); err != nil {
		t.Fatalf("create agent: %v", err)
	}

	wsID, uID := workspaceID, userID
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM workspace WHERE id = $1`, wsID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM "user" WHERE id = $1`, uID)
	})
	return workspaceID, userID, agentID
}

// secondAgent creates another agent in the same workspace, reusing the first
// agent's runtime, and returns its id. Used to test the deployment-wide robot_id
// uniqueness (a second agent binding the same bot).
func secondAgent(t *testing.T, wsID, firstAgentID pgtype.UUID) pgtype.UUID {
	t.Helper()
	var id pgtype.UUID
	if err := testPool.QueryRow(context.Background(),
		`INSERT INTO agent (workspace_id, name, runtime_mode, runtime_id)
		 SELECT $1, 'Octo Agent 2', 'local', runtime_id FROM agent WHERE id = $2
		 RETURNING id`,
		wsID, firstAgentID).Scan(&id); err != nil {
		t.Fatalf("create second agent: %v", err)
	}
	return id
}
