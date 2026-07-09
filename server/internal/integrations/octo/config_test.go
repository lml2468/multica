package octo

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/internal/util/secretbox"
)

func testBox(t *testing.T) *secretbox.Box {
	t.Helper()
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	box, err := secretbox.New(key[:])
	if err != nil {
		t.Fatalf("secretbox.New: %v", err)
	}
	return box
}

// TestEncodeDecodeConfig_RoundTrips confirms encodeConfig seals the bot token and
// decodeCredentials recovers it, and that the routing key (robot_id → app_id) and
// display fields survive the round-trip.
func TestEncodeDecodeConfig_RoundTrips(t *testing.T) {
	box := testBox(t)
	raw, err := encodeConfig(box, InstallationParams{
		RobotID:  "robot_9",
		BotName:  "Octo-Z",
		OwnerUID: "owner_1",
		APIURL:   "https://im.example/api",
		WSURL:    "wss://im.example/ws",
		BotToken: "bf_secret",
	})
	if err != nil {
		t.Fatalf("encodeConfig: %v", err)
	}
	// Plaintext token must never appear in the stored blob.
	if got := string(raw); strings.Contains(got, "bf_secret") {
		t.Fatalf("plaintext token leaked into config: %s", got)
	}

	creds, err := decodeCredentials(raw, box.Open)
	if err != nil {
		t.Fatalf("decodeCredentials: %v", err)
	}
	if creds.APIURL != "https://im.example/api" {
		t.Errorf("creds api_url wrong: %+v", creds)
	}
	if creds.BotToken != "bf_secret" {
		t.Errorf("decrypted token = %q, want bf_secret", creds.BotToken)
	}

	pub := DecodePublicConfig(raw)
	if pub.RobotID != "robot_9" || pub.BotName != "Octo-Z" || pub.OwnerUID != "owner_1" {
		t.Errorf("public config wrong: %+v", pub)
	}
}

// TestDecodePublicConfig_Malformed enforces the API-compatibility rule: a
// malformed / unexpected config blob must degrade to a zero-value PublicConfig,
// never panic, so the management list still renders the row's flat columns.
func TestDecodePublicConfig_Malformed(t *testing.T) {
	cases := [][]byte{
		nil,
		[]byte(""),
		[]byte("not json"),
		[]byte(`{"app_id": 123}`),        // wrong type
		[]byte(`["unexpected","array"]`), // wrong shape
		[]byte(`{}`),                     // empty object
	}
	for i, raw := range cases {
		pub := DecodePublicConfig(raw) // must not panic
		if pub.RobotID != "" && i != 3 {
			t.Errorf("case %d: expected empty RobotID on malformed config, got %q", i, pub.RobotID)
		}
	}
}

// TestDecodeCredentials_EmptyConfig returns an error rather than panicking on an
// empty config blob (the runtime path, unlike the display path).
func TestDecodeCredentials_EmptyConfig(t *testing.T) {
	if _, err := decodeCredentials(nil, nil); err == nil {
		t.Error("expected error for empty config")
	}
}

// TestDecodeCredentials_NilDecrypterTreatsAsPlaintext confirms the test-only
// convenience path: a nil Decrypter treats the base64-decoded bytes as plaintext.
func TestDecodeCredentials_NilDecrypterTreatsAsPlaintext(t *testing.T) {
	raw, _ := json.Marshal(installConfig{
		AppID:             "robot_1",
		APIURL:            "https://im.example/api",
		BotTokenEncrypted: base64.StdEncoding.EncodeToString([]byte("plain_token")),
	})
	creds, err := decodeCredentials(raw, nil)
	if err != nil {
		t.Fatalf("decodeCredentials: %v", err)
	}
	if creds.BotToken != "plain_token" {
		t.Errorf("token = %q, want plain_token", creds.BotToken)
	}
}
