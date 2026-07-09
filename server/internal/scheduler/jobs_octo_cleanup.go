package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// JobNameOctoCleanup is the canonical name used in audit rows. Stable across
// releases — do not rename without a migration. (It now purges the shared
// channel_* tables, covering Feishu/Slack/Octo, but the audit name is kept for
// continuity.)
const JobNameOctoCleanup = "octo_cleanup"

// octoDedupRetention is how long processed inbound-dedup rows are kept before
// purge. The dedup gate only needs to remember a message id long enough to
// reject a WS replay; 24h is comfortably beyond any reconnect window. Matches
// the cutoff documented on the PurgeChannelInboundDedup query.
const octoDedupRetention = 24 * time.Hour

// OctoCleanupJob returns the JobSpec that purges expired channel binding tokens
// and stale inbound-dedup rows from the shared channel_* tables. Both would
// otherwise grow unbounded: binding tokens are single-use with a 15m TTL but the
// consumed/expired rows linger, and dedup rows accumulate one per inbound message
// forever. This purges channel_binding_token / channel_inbound_message_dedup, so
// it covers every channel adapter (Feishu, Slack, Octo) — the octo_* tables it
// used to purge were folded into channel_* by migration 154.
func OctoCleanupJob(pool *pgxpool.Pool) JobSpec {
	return JobSpec{
		Name:              JobNameOctoCleanup,
		Cadence:           1 * time.Hour,
		ScheduleDelay:     1 * time.Hour,
		CatchUpMode:       CatchUpLatestOnly,
		CatchUpWindow:     24 * time.Hour,
		RunTimeout:        5 * time.Minute,
		StaleTimeout:      10 * time.Minute,
		HeartbeatInterval: 30 * time.Second,
		AllowStaleReentry: true,
		MaxAttempts:       3,
		RetryBackoff: []time.Duration{
			1 * time.Minute,
			5 * time.Minute,
			15 * time.Minute,
		},
		Scopes:  StaticScopes(ScopeGlobal),
		Handler: makeOctoCleanupHandler(pool),
	}
}

// makeOctoCleanupHandler deletes expired channel binding tokens (expires_at <
// now) and dedup rows older than the retention window. Both deletes are
// idempotent and safe to re-run, so a retry after a partial failure simply
// removes whatever remains.
func makeOctoCleanupHandler(pool *pgxpool.Pool) Handler {
	return func(ctx context.Context, in HandlerInput) (HandlerResult, error) {
		q := db.New(pool)
		now := pgtype.Timestamptz{Time: in.PlanTime, Valid: true}
		if err := q.PurgeExpiredChannelBindingTokens(ctx, now); err != nil {
			return HandlerResult{}, fmt.Errorf("purge expired channel binding tokens: %w", err)
		}
		dedupCutoff := pgtype.Timestamptz{Time: in.PlanTime.Add(-octoDedupRetention), Valid: true}
		if err := q.PurgeChannelInboundDedup(ctx, dedupCutoff); err != nil {
			return HandlerResult{}, fmt.Errorf("purge channel inbound dedup: %w", err)
		}
		if in.Heartbeat != nil {
			_ = in.Heartbeat(ctx)
		}
		return HandlerResult{
			Result: map[string]any{
				"dedup_retention": octoDedupRetention.String(),
			},
		}, nil
	}
}
