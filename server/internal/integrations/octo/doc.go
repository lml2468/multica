// Package octo implements the Octo IM integration as a consumer of the shared
// channel.Channel engine (internal/integrations/channel + .../channel/engine),
// the third IM adapter after Feishu (lark) and Slack. It provides per-agent bot
// installations, inbound message dispatch into chat sessions, outbound replies,
// and identity binding — all over the generalized channel_* tables
// (channel_type='octo'), with no octo_* tables of its own (migration 154 folded
// them in).
//
// It builds on the WuKongIM transport layer in the transport subpackage
// (github.com/multica-ai/multica/server/internal/integrations/octo/transport).
// The layering is one-directional: transport knows the WuKongIM binary protocol
// and REST API but no business types; this package depends on transport and the
// generated DB queries, never the reverse.
//
// The moving parts, all wired onto the shared engine in cmd/server/router.go:
//
//   - octoChannel (octo_channel.go) implements channel.Channel. Connect registers
//     the bot, opens the WuKongIM socket, and blocks running the receive loop;
//     each decoded message is normalized to a channel.InboundMessage (bot @mention
//     stripped, /new parsed) and handed to the engine Router. The engine.Supervisor
//     builds one per active installation and owns the WS lease / reconnect
//     lifecycle. RegisterOcto registers the Factory on the shared channel.Registry.
//   - NewOctoResolverSet (octo_resolvers.go) wires the installation / identity /
//     dedup / session / audit seams the engine.Router runs the inbound pipeline
//     through, plus the outbound replier. Octo inherits /issue commands and the
//     debounced run trigger from the engine for free.
//   - Patcher (outbound.go) relays the agent's reply back to Octo on chat:done /
//     task:failed events.
//   - octoOutcomeReplier (outcome_replier.go) handles the synchronous pre-agent
//     outcomes: DM an unbound sender a binding link, or notify the user when the
//     agent is offline/archived. Implements engine.OutboundReplier.
//   - BindingTokenService (binding_token.go) mints and redeems the one-shot tokens
//     behind the {PublicURL}/octo/bind?token= flow.
//   - InstallationService (client.go) creates/revokes installations and encrypts
//     the bot token at rest inside the config blob.
//
// Expired binding tokens and stale inbound-dedup rows are purged by the
// octo_cleanup scheduler job (internal/scheduler/jobs_octo_cleanup.go), which now
// purges the shared channel_* dedup/token tables (covering Feishu/Slack too). The
// generic channel_* queries this package depends on live in
// pkg/db/queries/channel.sql (migration 124; Octo folded in by migration 154).
package octo
