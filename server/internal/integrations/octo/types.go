package octo

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
)

// UID is an Octo user's identifier within a deployment. Typed alias rather than
// a plain string so callers can't accidentally pass a Multica user UUID where an
// Octo uid is expected.
type UID string

// InstallationStatus mirrors the channel_installation.status CHECK constraint.
type InstallationStatus string

const (
	InstallationActive  InstallationStatus = "active"
	InstallationRevoked InstallationStatus = "revoked"
)

// BindingTokenTTL caps the lifetime of a member-binding token. The DB CHECK on
// channel_binding_token enforces the same bound at the storage layer. Keep these
// in sync if the product value changes.
const BindingTokenTTL = 15 * time.Minute

// TxStarter begins a database transaction. Satisfied by *pgxpool.Pool; an
// interface so the services that need a tx (binding redemption) are unit-testable
// without a live pool.
type TxStarter interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// uuidString renders a pgtype.UUID for logs. Empty string for the zero UUID.
func uuidString(u pgtype.UUID) string { return util.UUIDToString(u) }
