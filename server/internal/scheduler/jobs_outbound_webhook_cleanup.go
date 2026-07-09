package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// JobNameOutboundWebhookCleanup is the canonical name used in audit rows.
// Stable across releases — do not rename without a migration.
const JobNameOutboundWebhookCleanup = "outbound_webhook_cleanup"

// outboundWebhookDeliveryRetention is how long outbound webhook delivery-history
// rows are kept before purge. Outbound webhooks fire on every issue status
// change × subscription, so the table grows quickly; 30 days is long enough to
// debug a misbehaving endpoint while bounding storage. There is intentionally
// no per-subscription cap — time-based retention is simpler and predictable.
const outboundWebhookDeliveryRetention = 30 * 24 * time.Hour

// OutboundWebhookDeliveryCleanupJob returns the JobSpec that purges outbound
// webhook delivery-history rows older than the retention window. Without it the
// outbound_webhook_delivery table (migration 151) would grow unbounded — the
// dispatcher writes one row per delivery and never deletes.
func OutboundWebhookDeliveryCleanupJob(pool *pgxpool.Pool) JobSpec {
	return JobSpec{
		Name:              JobNameOutboundWebhookCleanup,
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
		Handler: makeOutboundWebhookCleanupHandler(pool),
	}
}

// makeOutboundWebhookCleanupHandler deletes delivery rows older than the
// retention window. The delete is idempotent and safe to re-run, so a retry
// after a partial failure simply removes whatever remains.
func makeOutboundWebhookCleanupHandler(pool *pgxpool.Pool) Handler {
	return func(ctx context.Context, in HandlerInput) (HandlerResult, error) {
		q := db.New(pool)
		cutoff := pgtype.Timestamptz{Time: in.PlanTime.Add(-outboundWebhookDeliveryRetention), Valid: true}
		if err := q.PurgeOutboundWebhookDeliveriesOlderThan(ctx, cutoff); err != nil {
			return HandlerResult{}, fmt.Errorf("purge outbound webhook deliveries: %w", err)
		}
		if in.Heartbeat != nil {
			_ = in.Heartbeat(ctx)
		}
		return HandlerResult{
			Result: map[string]any{
				"retention": outboundWebhookDeliveryRetention.String(),
			},
		}, nil
	}
}
