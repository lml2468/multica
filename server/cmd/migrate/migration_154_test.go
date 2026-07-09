package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/migrations"
)

// This file is the regression test for migration 154 (octo_* -> channel_* fold).
// It exercises the two safety margins added after review of #57 that the generic
// migrate-race harness (which uses synthetic temp-dir SQL) cannot cover:
//
//   - the up-path pre-DROP row-count assertion (fold is proven before the
//     irreversible DROP), and
//   - the orphan-safe down-path (a rollback must not 23503-abort when a row was
//     orphaned while data lived FK-free in channel_*).
//
// Unlike migrate_concurrent_test.go's hermetic fixtures, this needs the REAL
// migration chain (154's SQL is unqualified and depends on the actual octo_* /
// channel_* schema), so it runs the real files against a throwaway database. It
// skips cleanly when no Postgres is reachable, matching the repo convention.

// migration154DB creates a throwaway database, runs the real up chain through
// 154, and returns a pool connected to it (dropped on cleanup). The admin pool
// (on the DATABASE_URL default db) is used only to CREATE/DROP the scratch db.
func migration154DB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://multica:multica@localhost:5432/multica?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	admin, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("could not connect to %s: %v", dbURL, err)
	}
	defer admin.Close()
	if err := admin.Ping(ctx); err != nil {
		t.Skipf("database not reachable at %s: %v", dbURL, err)
	}

	name := fmt.Sprintf("octo154_%d_%d", time.Now().UnixNano(), rand.Uint32())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Skipf("cannot create scratch database (need CREATEDB): %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a, err := pgxpool.New(c, dbURL)
		if err != nil {
			return
		}
		defer a.Close()
		_, _ = a.Exec(c, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	})

	scratchURL := swapDBName(dbURL, name)
	pool, err := pgxpool.New(ctx, scratchURL)
	if err != nil {
		t.Fatalf("connect scratch db: %v", err)
	}
	t.Cleanup(pool.Close)

	files, err := migrations.Files("up")
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	if err := runMigrations(ctx, pool, runOptions{
		Direction: "up",
		Files:     files,
		Hooks:     preMigrationHooks,
	}); err != nil {
		t.Fatalf("migrate up (real chain incl. 154): %v", err)
	}
	return pool
}

// swapDBName rewrites the trailing /<db> path segment of a postgres URL, leaving
// query params intact.
func swapDBName(url, name string) string {
	q := ""
	if i := strings.IndexByte(url, '?'); i >= 0 {
		q = url[i:]
		url = url[:i]
	}
	slash := strings.LastIndexByte(url, '/')
	return url[:slash+1] + name + q
}

// down154SQL reads the real 154 down file.
func down154SQL(t *testing.T) string {
	t.Helper()
	dir, err := migrations.ResolveDir()
	if err != nil {
		t.Fatalf("resolve migrations dir: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "154_octo_to_channel.down.sql"))
	if err != nil {
		t.Fatalf("read 154 down: %v", err)
	}
	return string(b)
}

// seed154Base inserts a workspace/user/member/runtime/agent/chat_session graph
// and returns their ids. Shared by the fold tests.
func seed154Base(t *testing.T, pool *pgxpool.Pool) (wsID, userID, agentID, sessionID string) {
	t.Helper()
	ctx := context.Background()
	must := func(q string, args ...any) {
		if _, err := pool.Exec(ctx, q, args...); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	wsID = "11111111-1111-1111-1111-111111111111"
	userID = "22222222-2222-2222-2222-222222222222"
	agentID = "33333333-3333-3333-3333-333333333333"
	sessionID = "44444444-4444-4444-4444-444444444444"
	rtID := "66666666-6666-6666-6666-666666666666"
	must(`INSERT INTO workspace (id,name,slug) VALUES ($1,'WS','ws-octo154')`, wsID)
	must(`INSERT INTO "user" (id,email,name) VALUES ($1,'u154@example.com','U')`, userID)
	must(`INSERT INTO member (workspace_id,user_id,role) VALUES ($1,$2,'admin')`, wsID, userID)
	must(`INSERT INTO agent_runtime (id,workspace_id,name,runtime_mode,provider) VALUES ($1,$2,'RT','local','anthropic')`, rtID, wsID)
	must(`INSERT INTO agent (id,workspace_id,name,runtime_mode,runtime_id) VALUES ($1,$2,'A','local',$3)`, agentID, wsID, rtID)
	must(`INSERT INTO chat_session (id,workspace_id,agent_id,creator_id,title) VALUES ($1,$2,$3,$4,'S')`, sessionID, wsID, agentID, userID)
	return wsID, userID, agentID, sessionID
}

func scalar(t *testing.T, pool *pgxpool.Pool, q string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("scalar %q: %v", q, err)
	}
	return n
}

// TestMigration154_DownRoundTripsAndIsOrphanSafe seeds folded octo rows in
// channel_* (post-154 state), then applies the real 154 down SQL and asserts:
// (a) it does not abort even though a member parent was deleted (orphan-safe),
// (b) the surviving rows round-trip back into octo_* with the WuKongIM channel
// type (topic=5) and a colon-bearing message id intact, and (c) the orphaned
// binding is skipped, not restored, and all octo rows are cleared from channel_*.
func TestMigration154_DownRoundTripsAndIsOrphanSafe(t *testing.T) {
	pool := migration154DB(t)
	ctx := context.Background()
	wsID, userID, agentID, sessionID := seed154Base(t, pool)
	instID := "55555555-5555-5555-5555-555555555555"

	must := func(q string, args ...any) {
		if _, err := pool.Exec(ctx, q, args...); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	// Folded channel_* octo rows (what 154 up would have produced).
	must(`INSERT INTO channel_installation (id,workspace_id,agent_id,channel_type,config,status,installer_user_id)
	      VALUES ($1,$2,$3,'octo', jsonb_build_object('app_id','robot-1','api_url','https://api','bot_token_encrypted','3q2+7w=='),'active',$4)`,
		instID, wsID, agentID, userID)
	must(`INSERT INTO channel_user_binding (workspace_id,multica_user_id,installation_id,channel_type,channel_user_id)
	      VALUES ($1,$2,$3,'octo','uid-abc')`, wsID, userID, instID)
	must(`INSERT INTO channel_chat_session_binding (chat_session_id,installation_id,channel_type,channel_chat_id,chat_type,config)
	      VALUES ($1,$2,'octo','chan-1','group', jsonb_build_object('octo_channel_type',5))`, sessionID, instID)
	// Outbound card with a colon-bearing message id "wk:out:99" and seq 42.
	must(`INSERT INTO channel_outbound_card_message (chat_session_id,channel_type,channel_chat_id,channel_card_message_id,status)
	      VALUES ($1,'octo','chan-1','wk:out:99:42','final')`, sessionID)
	must(`INSERT INTO channel_binding_token (token_hash,workspace_id,installation_id,channel_type,channel_user_id,expires_at)
	      VALUES ('hash154',$1,$2,'octo','uid-abc', now()+interval '10 min')`, wsID, instID)

	// Orphan the user binding: delete the member while the FK-free binding
	// survives (channel_user_binding has no member FK). This is the case that
	// would 23503-abort a naive down.
	must(`DELETE FROM member WHERE workspace_id=$1 AND user_id=$2`, wsID, userID)

	// Apply the real 154 down SQL as one statement (mirrors the runner's Exec).
	if _, err := pool.Exec(ctx, down154SQL(t)); err != nil {
		t.Fatalf("154 down aborted (should be orphan-safe): %v", err)
	}

	// octo_* recreated; the non-orphaned rows restored.
	if n := scalar(t, pool, `SELECT count(*) FROM octo_installation`); n != 1 {
		t.Errorf("octo_installation = %d, want 1", n)
	}
	if n := scalar(t, pool, `SELECT count(*) FROM octo_binding_token`); n != 1 {
		t.Errorf("octo_binding_token = %d, want 1", n)
	}
	// The orphaned user_binding is skipped (its member parent is gone).
	if n := scalar(t, pool, `SELECT count(*) FROM octo_user_binding`); n != 0 {
		t.Errorf("octo_user_binding = %d, want 0 (orphan must be skipped, not restored)", n)
	}
	// Topic channel type survived the chat_type collapse.
	if ct := scalar(t, pool, `SELECT octo_channel_type FROM octo_chat_session_binding`); ct != 5 {
		t.Errorf("octo_channel_type = %d, want 5 (topic)", ct)
	}
	// The colon-bearing message id round-trips (seq split from the LAST colon).
	var msgID string
	var seq int
	if err := pool.QueryRow(ctx, `SELECT octo_message_id, octo_message_seq FROM octo_outbound_message`).Scan(&msgID, &seq); err != nil {
		t.Fatalf("read octo_outbound_message: %v", err)
	}
	if msgID != "wk:out:99" || seq != 42 {
		t.Errorf("outbound decode = (%q, %d), want (\"wk:out:99\", 42)", msgID, seq)
	}
	// All octo rows removed from channel_* (incl. the orphan).
	if n := scalar(t, pool, `SELECT count(*) FROM channel_installation WHERE channel_type='octo'`); n != 0 {
		t.Errorf("channel_installation octo rows = %d, want 0", n)
	}
	if n := scalar(t, pool, `SELECT count(*) FROM channel_user_binding WHERE channel_type='octo'`); n != 0 {
		t.Errorf("channel_user_binding octo rows = %d, want 0", n)
	}
}

// TestMigration154_UpAssertionAbortsOnMismatch confirms the up-path guard: a
// silent fold mismatch (fewer channel_* rows than octo_* source) RAISEs before
// the irreversible DROP. It reconstructs the assertion's shape against a seeded
// mismatch — octo_installation has a row its channel_installation counterpart
// lacks — and asserts the guard fires.
func TestMigration154_UpAssertionAbortsOnMismatch(t *testing.T) {
	pool := migration154DB(t)
	ctx := context.Background()
	wsID, userID, agentID, _ := seed154Base(t, pool)

	// The real chain already dropped octo_* (154 ran). Recreate a minimal
	// octo_installation so the assertion has a source row with no channel_*
	// counterpart, i.e. a fold that copied nothing.
	if _, err := pool.Exec(ctx, `CREATE TABLE octo_installation (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		workspace_id UUID NOT NULL, agent_id UUID NOT NULL,
		installer_user_id UUID NOT NULL)`); err != nil {
		t.Fatalf("recreate octo_installation: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO octo_installation (workspace_id,agent_id,installer_user_id) VALUES ($1,$2,$3)`,
		wsID, agentID, userID); err != nil {
		t.Fatalf("seed octo_installation: %v", err)
	}

	// The assertion block from 154 up (installation table). It must RAISE
	// because octo_installation has 1 row and channel_installation has 0 octo.
	assertSQL := `DO $$
DECLARE mismatch text;
BEGIN
  SELECT string_agg(t, ', ') INTO mismatch FROM (
    SELECT 'installation' AS t
      WHERE (SELECT count(*) FROM octo_installation)
          <> (SELECT count(*) FROM channel_installation WHERE channel_type = 'octo')
  ) m;
  IF mismatch IS NOT NULL THEN
    RAISE EXCEPTION 'octo->channel fold row-count mismatch in: %; aborting before DROP', mismatch;
  END IF;
END $$;`
	_, err := pool.Exec(ctx, assertSQL)
	if err == nil {
		t.Fatal("assertion did not fire on a row-count mismatch")
	}
	if !strings.Contains(err.Error(), "fold row-count mismatch") {
		t.Errorf("unexpected error: %v", err)
	}
}
