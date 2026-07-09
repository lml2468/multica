package octo

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/multica-ai/multica/server/internal/util/secretbox"
)

// installConfig is the JSON shape stored in channel_installation.config for an
// Octo installation. The cross-platform columns (workspace/agent/installer/
// status) stay flat on the row; everything Octo-specific lives in this opaque
// blob — the documented config boundary shared by every channel adapter.
//
// app_id holds the Octo robot_id. It is the per-installation routing key: the
// generic GetChannelInstallationByAppID query (config->>'app_id') and the
// (channel_type, app_id) unique index map an inbound event's robot_id to its
// installation, the same slot Feishu uses for app_id and Slack for its app id.
//
// bot_token_encrypted is the base64-encoded secretbox ciphertext of the bf_*
// bot token, never plaintext (mirroring Feishu's app_secret_encrypted). bot_name
// / owner_uid / api_url / ws_url are cached from the register response so the
// channel can open its socket without an extra round-trip and the management API
// can render the installation without decrypting anything.
type installConfig struct {
	AppID             string `json:"app_id"`
	BotName           string `json:"bot_name,omitempty"`
	OwnerUID          string `json:"owner_uid,omitempty"`
	APIURL            string `json:"api_url"`
	WSURL             string `json:"ws_url,omitempty"`
	BotTokenEncrypted string `json:"bot_token_encrypted"`
}

// credentials is the decoded, decrypted form the channel/outbound paths run on:
// just the API base and the plaintext bot token needed to Register/Send. The
// installation IDENTITY (workspace / agent / installer) is resolved per message
// by the Router's InstallationResolver, and display fields (robot_id, bot_name)
// live in PublicConfig — neither belongs here.
type credentials struct {
	APIURL   string
	BotToken string
}

// Decrypter turns stored ciphertext into plaintext. Production injects a
// secretbox-backed implementation; tests inject an identity decrypter (or nil,
// which treats the stored bytes as plaintext).
type Decrypter func(ciphertext []byte) (plaintext []byte, err error)

// boxDecrypter adapts a *secretbox.Box to the Decrypter signature.
func boxDecrypter(box *secretbox.Box) Decrypter {
	if box == nil {
		return nil
	}
	return box.Open
}

// decodeCredentials parses the per-installation config blob and decrypts the bot
// token. It is the single place the Octo config JSON is interpreted for the
// runtime (connect / send) paths.
func decodeCredentials(raw json.RawMessage, decrypt Decrypter) (credentials, error) {
	if len(raw) == 0 {
		return credentials{}, errors.New("octo: empty installation config")
	}
	var cfg installConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return credentials{}, fmt.Errorf("decode octo installation config: %w", err)
	}
	botToken, err := decryptToken(cfg.BotTokenEncrypted, decrypt)
	if err != nil {
		return credentials{}, fmt.Errorf("decrypt bot token: %w", err)
	}
	return credentials{
		APIURL:   cfg.APIURL,
		BotToken: botToken,
	}, nil
}

// PublicConfig is the non-secret subset of an installation config, safe to
// surface on the management API (the encrypted bot token is never included).
type PublicConfig struct {
	RobotID  string
	BotName  string
	OwnerUID string
	APIURL   string
	WSURL    string
}

// DecodePublicConfig extracts the display-safe fields from a stored config blob.
// A decode miss yields a zero-value PublicConfig rather than an error, so the
// management list still renders the row's flat identity columns even if the blob
// is malformed (the API-compatibility rule: degrade, do not 500).
func DecodePublicConfig(raw json.RawMessage) PublicConfig {
	var cfg installConfig
	_ = json.Unmarshal(raw, &cfg)
	return PublicConfig{
		RobotID:  cfg.AppID,
		BotName:  cfg.BotName,
		OwnerUID: cfg.OwnerUID,
		APIURL:   cfg.APIURL,
		WSURL:    cfg.WSURL,
	}
}

// encodeConfig seals the bot token and marshals the installation config blob for
// storage in channel_installation.config. The token is sealed via the injected
// secretbox and stored as base64, matching how migration 154 folded existing
// rows and how decodeCredentials reads them back.
func encodeConfig(box *secretbox.Box, p InstallationParams) ([]byte, error) {
	if box == nil {
		return nil, errors.New("octo: encodeConfig requires a non-nil secretbox.Box")
	}
	sealed, err := box.Seal([]byte(p.BotToken))
	if err != nil {
		return nil, fmt.Errorf("seal bot token: %w", err)
	}
	return json.Marshal(installConfig{
		AppID:             p.RobotID,
		BotName:           p.BotName,
		OwnerUID:          p.OwnerUID,
		APIURL:            p.APIURL,
		WSURL:             p.WSURL,
		BotTokenEncrypted: base64.StdEncoding.EncodeToString(sealed),
	})
}

// decryptToken base64-decodes the stored ciphertext (tolerating the MIME newline
// wrapping PostgreSQL's encode(...,'base64') emits) and runs it through the
// injected Decrypter. An empty stored value decodes to an empty token; a nil
// Decrypter treats the decoded bytes as plaintext (test convenience). Mirrors
// the Slack adapter's helper.
func decryptToken(enc string, decrypt Decrypter) (string, error) {
	if enc == "" {
		return "", nil
	}
	ciphertext, err := base64.StdEncoding.DecodeString(stripWhitespace(enc))
	if err != nil {
		return "", fmt.Errorf("base64 decode: %w", err)
	}
	if decrypt == nil {
		return string(ciphertext), nil
	}
	plaintext, err := decrypt(ciphertext)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

// stripWhitespace removes ASCII whitespace so a MIME-wrapped base64 string
// (newlines every 64/76 chars) and an unwrapped one decode identically.
func stripWhitespace(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
