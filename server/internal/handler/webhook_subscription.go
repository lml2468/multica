package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/outwebhook"
	"github.com/multica-ai/multica/server/internal/netguard"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// ── Outbound webhook subscriptions ──────────────────────────────────────────
//
// CRUD for webhook_subscription (see migration 150): external HTTP endpoints
// Multica POSTs to when subscribed issue events fire. project_id IS NULL is a
// workspace-level webhook (GitHub "org" webhook); a set project_id is a
// project-level webhook (GitHub "repo" webhook). All endpoints are gated to
// workspace owner/admin — webhooks can exfiltrate issue data, so creating one
// is a privileged action.

// webhookSecretPrefix marks a Multica webhook signing secret so a leaked value
// is recognisable. "whsec_" follows the convention used by Stripe/GitHub for
// signing secrets. 32 random bytes => 43 chars of URL-safe base64.
const webhookSecretPrefix = "whsec_"

// supportedWebhookEvents is the v1 event allow-list. It is derived from the
// dispatcher's SupportedEventTypes — the single source of truth for the events
// Multica can actually deliver. Adding an event there requires a matching bus
// subscription in cmd/server/webhook_listeners.go, otherwise subscriptions for
// it would be accepted but never fire. Unknown event types in a create/update
// request are rejected so a typo doesn't silently subscribe to nothing.
var supportedWebhookEvents = func() map[string]bool {
	m := make(map[string]bool, len(outwebhook.SupportedEventTypes))
	for _, e := range outwebhook.SupportedEventTypes {
		m[e] = true
	}
	return m
}()

// WebhookSubscriptionResponse is the API shape. The signing secret is returned
// ONLY once, on create (SecretOnce); list/get never echo it. SecretHint carries
// the last 4 chars so operators can tell two secrets apart in the UI.
type WebhookSubscriptionResponse struct {
	ID          string   `json:"id"`
	WorkspaceID string   `json:"workspace_id"`
	ProjectID   *string  `json:"project_id"`
	URL         string   `json:"url"`
	Events      []string `json:"events"`
	Enabled     bool     `json:"enabled"`
	SecretHint  string   `json:"secret_hint"`
	// SecretOnce is the full signing secret, populated only in the create
	// response. Empty on list/get.
	SecretOnce string `json:"secret,omitempty"`
	// ConsecutiveFailures is the number of terminal failed deliveries since
	// the last delivered one. Auto-disable trips at the configured threshold
	// (#38); exposed so the UI can render a "this subscription is at N/M
	// failures" hint before it flips. Resets to 0 on a successful delivery,
	// also reset when an operator re-enables.
	ConsecutiveFailures int32 `json:"consecutive_failures"`
	// DisabledReason is "auto_disabled_failure_threshold" when the system
	// flipped enabled→false; empty when the operator disabled it themselves
	// (or it has never been disabled). UI uses this to render "Disabled by
	// system" vs "Disabled".
	DisabledReason *string `json:"disabled_reason"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
}

func webhookSubscriptionToResponse(s db.WebhookSubscription) WebhookSubscriptionResponse {
	var events []string
	if err := json.Unmarshal(s.Events, &events); err != nil {
		events = []string{}
	}
	resp := WebhookSubscriptionResponse{
		ID:                  uuidToString(s.ID),
		WorkspaceID:         uuidToString(s.WorkspaceID),
		ProjectID:           uuidToPtr(s.ProjectID),
		URL:                 s.Url,
		Events:              events,
		Enabled:             s.Enabled,
		SecretHint:          secretHint(s.Secret),
		ConsecutiveFailures: s.ConsecutiveFailures,
		DisabledReason:      textToPtr(s.DisabledReason),
		CreatedAt:           timestampToString(s.CreatedAt),
		UpdatedAt:           timestampToString(s.UpdatedAt),
	}
	return resp
}

// secretHint returns the last 4 characters of a secret (or "" if too short).
func secretHint(secret string) string {
	if len(secret) < 4 {
		return ""
	}
	return secret[len(secret)-4:]
}

// generateWebhookSecret returns a cryptographically random signing secret,
// reusing the shared credential generator so token and secret entropy stay in
// lockstep.
func generateWebhookSecret() (string, error) {
	return generateCredential(webhookSecretPrefix)
}

// validateWebhookURL enforces an absolute http(s) URL and fast-rejects endpoints
// that obviously point at the server's own network — "localhost" and IP-literal
// hosts in loopback/link-local (incl. the 169.254.169.254 cloud metadata
// endpoint)/private/unspecified ranges. This is a create-time UX check only:
// the authoritative SSRF guard runs at delivery time in netguard's restricted
// HTTP client, which resolves DNS and re-checks every dial (including redirect
// hops), closing the hostname-resolves-to-internal and DNS-rebinding gaps this
// literal check cannot.
func validateWebhookURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return errors.New("url is not a valid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("url must be http or https")
	}
	host := u.Hostname()
	if host == "" {
		return errors.New("url must include a host")
	}
	if strings.EqualFold(host, "localhost") {
		return errors.New("url must not target localhost")
	}
	if ip := net.ParseIP(host); ip != nil && netguard.IsBlockedIP(ip) {
		return errors.New("url must not target an internal or loopback address")
	}
	return nil
}

// validateWebhookEvents checks every requested event against the allow-list and
// returns the JSONB bytes to persist. An empty list is rejected — callers that
// want the default must apply it before calling (create does; update does not,
// so an explicit empty array is a 400 rather than a silent reset to default).
func validateWebhookEvents(events []string) ([]byte, error) {
	if len(events) == 0 {
		return nil, errors.New("events must not be empty")
	}
	for _, e := range events {
		if !supportedWebhookEvents[e] {
			return nil, fmt.Errorf("unsupported event: %q", e)
		}
	}
	b, err := json.Marshal(events)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// ── Requests ────────────────────────────────────────────────────────────────

type CreateWebhookSubscriptionRequest struct {
	URL string `json:"url"`
	// ProjectID, when set, scopes the webhook to one project (project-level /
	// "repo" webhook). Omit for a workspace-level / "org" webhook.
	ProjectID *string  `json:"project_id"`
	Events    []string `json:"events"`
}

type UpdateWebhookSubscriptionRequest struct {
	URL     *string   `json:"url"`
	Events  *[]string `json:"events"`
	Enabled *bool     `json:"enabled"`
}

// ── Handlers ────────────────────────────────────────────────────────────────

// ListWebhookSubscriptions returns subscriptions for the workspace. With a
// `project_id` query param it returns that project's webhooks; without it,
// workspace-level webhooks only.
func (h *Handler) ListWebhookSubscriptions(w http.ResponseWriter, r *http.Request) {
	workspaceID := h.resolveWorkspaceID(r)
	if _, ok := h.requireWorkspaceRole(w, r, workspaceID, "workspace not found", "owner", "admin"); !ok {
		return
	}
	wsUUID, ok := parseUUIDOrBadRequest(w, workspaceID, "workspace id")
	if !ok {
		return
	}

	var rows []db.WebhookSubscription
	var err error
	if projectIDParam := strings.TrimSpace(r.URL.Query().Get("project_id")); projectIDParam != "" {
		projectUUID, ok := parseUUIDOrBadRequest(w, projectIDParam, "project_id")
		if !ok {
			return
		}
		rows, err = h.Queries.ListWebhookSubscriptionsByProject(r.Context(), db.ListWebhookSubscriptionsByProjectParams{
			WorkspaceID: wsUUID,
			ProjectID:   projectUUID,
		})
	} else {
		rows, err = h.Queries.ListWebhookSubscriptionsByWorkspace(r.Context(), wsUUID)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list webhook subscriptions")
		return
	}

	out := make([]WebhookSubscriptionResponse, 0, len(rows))
	for _, row := range rows {
		out = append(out, webhookSubscriptionToResponse(row))
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": out})
}

// CreateWebhookSubscription registers a new outbound webhook. The generated
// signing secret is returned once in the response body.
func (h *Handler) CreateWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	var req CreateWebhookSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Authorize before validating input so an unauthorized caller can't probe
	// validation behavior (URL/event fingerprinting).
	workspaceID := h.resolveWorkspaceID(r)
	if _, ok := h.requireWorkspaceRole(w, r, workspaceID, "workspace not found", "owner", "admin"); !ok {
		return
	}

	if err := validateWebhookURL(req.URL); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Default the event list on create only; validateWebhookEvents rejects an
	// empty list so an explicit empty array on update is a 400, not a reset.
	if len(req.Events) == 0 {
		req.Events = []string{outwebhook.EventIssueStatusChanged}
	}
	eventsJSON, err := validateWebhookEvents(req.Events)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	wsUUID, ok := parseUUIDOrBadRequest(w, workspaceID, "workspace id")
	if !ok {
		return
	}

	// Resolve and validate project scope (project-level webhook).
	var projectUUID pgtype.UUID
	if req.ProjectID != nil && *req.ProjectID != "" {
		projectUUID, ok = parseUUIDOrBadRequest(w, *req.ProjectID, "project_id")
		if !ok {
			return
		}
		if _, err := h.Queries.GetProjectInWorkspace(r.Context(), db.GetProjectInWorkspaceParams{
			ID:          projectUUID,
			WorkspaceID: wsUUID,
		}); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				writeError(w, http.StatusNotFound, "project not found")
				return
			}
			writeError(w, http.StatusInternalServerError, "failed to load project")
			return
		}
	}

	secret, err := generateWebhookSecret()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate secret")
		return
	}

	row, err := h.Queries.CreateWebhookSubscription(r.Context(), db.CreateWebhookSubscriptionParams{
		WorkspaceID: wsUUID,
		ProjectID:   projectUUID,
		Url:         strings.TrimSpace(req.URL),
		Secret:      secret,
		Events:      eventsJSON,
		Enabled:     true,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create webhook subscription")
		return
	}

	resp := webhookSubscriptionToResponse(row)
	resp.SecretOnce = secret // shown once on create
	writeJSON(w, http.StatusCreated, resp)
}

// UpdateWebhookSubscription patches url / events / enabled on an existing
// subscription. Per the UUID-parsing convention, the {id} path param is
// validated and scoped by workspace_id in the query — never round-tripped raw.
func (h *Handler) UpdateWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	sub, ok := h.loadWebhookSubscription(w, r)
	if !ok {
		return
	}

	var req UpdateWebhookSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	params := db.UpdateWebhookSubscriptionParams{
		ID:          sub.ID,
		WorkspaceID: sub.WorkspaceID,
	}
	if req.URL != nil {
		if err := validateWebhookURL(*req.URL); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		params.Url = pgtype.Text{String: strings.TrimSpace(*req.URL), Valid: true}
	}
	if req.Events != nil {
		eventsJSON, err := validateWebhookEvents(*req.Events)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		params.Events = eventsJSON
	}
	if req.Enabled != nil {
		params.Enabled = pgtype.Bool{Bool: *req.Enabled, Valid: true}
	}

	row, err := h.Queries.UpdateWebhookSubscription(r.Context(), params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update webhook subscription")
		return
	}
	writeJSON(w, http.StatusOK, webhookSubscriptionToResponse(row))
}

// DeleteWebhookSubscription removes a subscription, scoped by workspace_id.
func (h *Handler) DeleteWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	sub, ok := h.loadWebhookSubscription(w, r)
	if !ok {
		return
	}
	if err := h.Queries.DeleteWebhookSubscription(r.Context(), db.DeleteWebhookSubscriptionParams{
		ID:          sub.ID,
		WorkspaceID: sub.WorkspaceID,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete webhook subscription")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// loadWebhookSubscription resolves the {id} path param to a subscription owned
// by the caller's workspace, gating on owner/admin. Returns the resolved row so
// callers use row.ID / row.WorkspaceID for writes (never the raw URL string).
func (h *Handler) loadWebhookSubscription(w http.ResponseWriter, r *http.Request) (db.WebhookSubscription, bool) {
	workspaceID := h.resolveWorkspaceID(r)
	if _, ok := h.requireWorkspaceRole(w, r, workspaceID, "workspace not found", "owner", "admin"); !ok {
		return db.WebhookSubscription{}, false
	}
	wsUUID, ok := parseUUIDOrBadRequest(w, workspaceID, "workspace id")
	if !ok {
		return db.WebhookSubscription{}, false
	}
	idUUID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "id")
	if !ok {
		return db.WebhookSubscription{}, false
	}
	sub, err := h.Queries.GetWebhookSubscriptionInWorkspace(r.Context(), db.GetWebhookSubscriptionInWorkspaceParams{
		ID:          idUUID,
		WorkspaceID: wsUUID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "webhook subscription not found")
			return db.WebhookSubscription{}, false
		}
		writeError(w, http.StatusInternalServerError, "failed to load webhook subscription")
		return db.WebhookSubscription{}, false
	}
	return sub, true
}

// TestWebhookSubscription enqueues a one-off synthetic delivery against the
// subscription so an operator can dry-run a freshly configured webhook against
// its receiver without waiting for a real issue status change. The synthetic
// payload uses identifier "TEST-0" / title "Multica webhook test push" so the
// receiver can clearly tell test traffic from real events; the delivery flows
// through the normal dispatcher (signing, retries, history row), so any
// problem surfaces the same way it would in production.
//
// Mirrors RedeliverWebhookSubscriptionDelivery's contract: owner/admin gated,
// rejects a disabled subscription (kill switch is authoritative), returns 202
// with the new delivery's lineage left unset (it's not a redelivery).
func (h *Handler) TestWebhookSubscription(w http.ResponseWriter, r *http.Request) {
	sub, ok := h.loadWebhookSubscription(w, r)
	if !ok {
		return
	}
	if !sub.Enabled {
		writeError(w, http.StatusConflict, "subscription is disabled; enable it before sending a test push")
		return
	}
	if h.WebhookDispatcher == nil {
		writeError(w, http.StatusServiceUnavailable, "webhook delivery is not available")
		return
	}
	if !h.WebhookDispatcher.TestPush(sub) {
		writeError(w, http.StatusServiceUnavailable, "delivery queue is full, try again shortly")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "queued"})
}
