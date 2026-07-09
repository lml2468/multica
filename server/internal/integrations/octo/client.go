package octo

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// InstallationParams carries the inputs to create or update an Octo bot
// installation. BotToken is the plaintext bf_* token; it is encrypted at rest
// via secretbox inside the config blob before storage and never persisted clear.
type InstallationParams struct {
	WorkspaceID     pgtype.UUID
	AgentID         pgtype.UUID
	BotToken        string
	RobotID         string
	BotName         string
	OwnerUID        string
	APIURL          string
	WSURL           string
	InstallerUserID pgtype.UUID
}

// InstallationService manages Octo rows on the generalized channel_installation
// table (channel_type='octo'), sealing the bot token at rest inside the config
// blob with a secretbox.Box. It also satisfies the outbound TokenDecryptor
// interface (DecryptBotToken).
type InstallationService struct {
	queries *db.Queries
	box     *secretbox.Box
}

// NewInstallationService constructs the service. The box MUST be non-nil; the
// whole Octo integration is gated on a configured MULTICA_OCTO_SECRET_KEY, so a
// nil box is a programming error rather than a degraded mode.
func NewInstallationService(queries *db.Queries, box *secretbox.Box) (*InstallationService, error) {
	if box == nil {
		return nil, errors.New("octo: InstallationService requires a non-nil secretbox.Box")
	}
	return &InstallationService{queries: queries, box: box}, nil
}

// Upsert creates or refreshes the installation, keyed by robot_id in the routing
// slot (config->>'app_id'). The (channel_type, config->>'app_id') unique index
// guarantees one Octo bot maps to exactly one row deployment-wide.
func (s *InstallationService) Upsert(ctx context.Context, p InstallationParams) (db.ChannelInstallation, error) {
	if err := validateInstallationParams(p); err != nil {
		return db.ChannelInstallation{}, err
	}
	config, err := encodeConfig(s.box, p)
	if err != nil {
		return db.ChannelInstallation{}, err
	}
	inst, err := s.queries.UpsertChannelInstallation(ctx, db.UpsertChannelInstallationParams{
		WorkspaceID:     p.WorkspaceID,
		AgentID:         p.AgentID,
		ChannelType:     string(TypeOcto),
		Config:          config,
		InstallerUserID: p.InstallerUserID,
	})
	if err != nil {
		// Conflict target is (workspace_id, agent_id, channel_type), so
		// re-configuring the SAME agent updates in place. Binding a bot whose
		// robot_id is already in use by a DIFFERENT agent falls through to an
		// INSERT that trips the (channel_type, config->>'app_id') unique index
		// (23505); surface it as a typed error so the handler returns 409.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return db.ChannelInstallation{}, ErrRobotAlreadyBound
		}
		return db.ChannelInstallation{}, err
	}
	return inst, nil
}

// ErrRobotAlreadyBound is returned by Upsert when the bot's robot_id is already
// bound to a different agent (the routing-key unique index is deployment-wide:
// one Octo bot maps to exactly one Multica agent). Translated to 409 at the HTTP
// boundary. The fix is to revoke the existing installation first.
var ErrRobotAlreadyBound = errors.New("octo bot is already bound to another agent")

// Revoke marks an installation revoked; the supervisor tears down its WS on the
// next sweep.
func (s *InstallationService) Revoke(ctx context.Context, id pgtype.UUID) error {
	return s.queries.SetChannelInstallationStatus(ctx, db.SetChannelInstallationStatusParams{
		ID:     id,
		Status: string(InstallationRevoked),
	})
}

// DecryptBotToken returns the plaintext bot token for an installation, reading it
// from the config blob. It satisfies the outbound TokenDecryptor interface.
func (s *InstallationService) DecryptBotToken(inst db.ChannelInstallation) (string, error) {
	creds, err := decodeCredentials(inst.Config, boxDecrypter(s.box))
	if err != nil {
		return "", err
	}
	return creds.BotToken, nil
}

// GetInWorkspace loads a workspace-scoped Octo installation (HTTP handler path).
// Returns ErrInstallationNotFound when no matching row exists.
func (s *InstallationService) GetInWorkspace(ctx context.Context, id, workspaceID pgtype.UUID) (db.ChannelInstallation, error) {
	inst, err := s.queries.GetChannelInstallationInWorkspace(ctx, db.GetChannelInstallationInWorkspaceParams{
		ID:          id,
		WorkspaceID: workspaceID,
		ChannelType: string(TypeOcto),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.ChannelInstallation{}, ErrInstallationNotFound
	}
	return inst, err
}

// ErrInstallationNotFound is returned by GetInWorkspace when no matching
// installation row exists for the (id, workspace) pair.
var ErrInstallationNotFound = errors.New("octo installation not found")

// ListByWorkspace lists a workspace's Octo installations (HTTP handler path).
func (s *InstallationService) ListByWorkspace(ctx context.Context, workspaceID pgtype.UUID) ([]db.ChannelInstallation, error) {
	return s.queries.ListChannelInstallationsByWorkspace(ctx, db.ListChannelInstallationsByWorkspaceParams{
		WorkspaceID: workspaceID,
		ChannelType: string(TypeOcto),
	})
}

func validateInstallationParams(p InstallationParams) error {
	switch {
	case !p.WorkspaceID.Valid:
		return errors.New("octo: installation requires workspace_id")
	case !p.AgentID.Valid:
		return errors.New("octo: installation requires agent_id")
	case p.BotToken == "":
		return errors.New("octo: installation requires a bot token")
	case p.RobotID == "":
		return errors.New("octo: installation requires robot_id")
	case p.APIURL == "":
		return errors.New("octo: installation requires api_url")
	case !p.InstallerUserID.Valid:
		return errors.New("octo: installation requires installer_user_id")
	}
	return nil
}
