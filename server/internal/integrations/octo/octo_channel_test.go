package octo

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/octo/transport"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeSocket is a transport-less socketConn the Connect tests drive: it captures
// the OnError hook it was built with so a test can fire a terminal error. Access
// is mutex-guarded because Connect builds the socket on its own goroutine while
// the test goroutine waits to invoke the hook.
type fakeSocket struct {
	mu          sync.Mutex
	onError     func(error)
	connectErr  error
	disconnects int
}

func (f *fakeSocket) Connect(ctx context.Context) error { return f.connectErr }

func (f *fakeSocket) Disconnect() {
	f.mu.Lock()
	f.disconnects++
	f.mu.Unlock()
}

func (f *fakeSocket) setOnError(fn func(error)) {
	f.mu.Lock()
	f.onError = fn
	f.mu.Unlock()
}

func (f *fakeSocket) fireError(err error) bool {
	f.mu.Lock()
	fn := f.onError
	f.mu.Unlock()
	if fn == nil {
		return false
	}
	fn(err)
	return true
}

func (f *fakeSocket) ready() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.onError != nil
}

func (f *fakeSocket) disconnected() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.disconnects
}

// newTestChannel builds an octoChannel whose socket + register seams are fakes,
// returning the channel and the socket the next Connect will use.
func newTestChannel(t *testing.T, handler channel.InboundHandler) (*octoChannel, *fakeSocket) {
	t.Helper()
	sock := &fakeSocket{}
	c := &octoChannel{
		creds:   credentials{RobotID: "robot_1", APIURL: "https://im.example/api", BotToken: "bf_x"},
		handler: handler,
		logger:  testLogger(),
	}
	c.newSocket = func(opts transport.SocketOptions) socketConn {
		sock.setOnError(opts.OnError)
		return sock
	}
	c.register = func(ctx context.Context) (*transport.BotRegisterResp, error) {
		return &transport.BotRegisterResp{RobotID: "robot_1", IMToken: "im_x", WSURL: "wss://im.example/ws"}, nil
	}
	return c, sock
}

// TestConnect_TerminalSocketError_Unwinds is the regression guard for the bug
// where Connect blocked on ctx.Done() forever, ignoring a terminal socket error
// (kicked / rapid disconnect / stale token). The Supervisor relies on Connect
// returning so it can rebuild the channel under backoff; without this the bot
// goes silently dead until redeploy.
func TestConnect_TerminalSocketError_Unwinds(t *testing.T) {
	c, sock := newTestChannel(t, func(context.Context, channel.InboundMessage) error { return nil })

	done := make(chan error, 1)
	go func() { done <- c.Connect(context.Background()) }()

	// Wait for the socket to be built, then fire a terminal error.
	waitFor(t, sock.ready)
	sock.fireError(errors.New("kicked by server"))

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Connect returned nil on a terminal socket error; want non-nil so the Supervisor reconnects")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Connect did not unwind after a terminal socket error (blocked on ctx.Done forever)")
	}
	if sock.disconnected() == 0 {
		t.Error("Connect did not Disconnect the socket on unwind")
	}
}

// TestConnect_CtxCancel_GracefulNil confirms a context cancellation is a clean
// stop (nil), not a failure the Supervisor would retry.
func TestConnect_CtxCancel_GracefulNil(t *testing.T) {
	c, sock := newTestChannel(t, func(context.Context, channel.InboundMessage) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Connect(ctx) }()

	waitFor(t, sock.ready)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Connect returned %v on ctx cancel; want nil (graceful stop)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Connect did not return after ctx cancel")
	}
}

// TestChannelMessage_StripsMentionAndParsesFresh confirms the inbound
// normalization: the bot's own @mention is removed and a leading /new sets
// ForceFresh with the directive stripped from the body.
func TestChannelMessage_StripsMentionAndParsesFresh(t *testing.T) {
	c := &octoChannel{logger: testLogger()}
	m := transport.BotMessage{
		MessageID:   "msg_1",
		FromUID:     "user_9",
		ChannelID:   "grp_1",
		ChannelType: transport.ChannelGroup,
		Payload: transport.MessagePayload{
			Content: "@bot /new build the thing",
			Mention: &transport.MentionPayload{
				UIDs:     []string{"robot_1"},
				Entities: []transport.MentionEntity{{UID: "robot_1", Offset: 0, Length: 4}},
			},
		},
	}
	msg := c.channelMessage("robot_1", m)
	if !msg.ForceFresh {
		t.Error("expected ForceFresh from leading /new")
	}
	if msg.Text != "build the thing" {
		t.Errorf("normalized text = %q, want %q", msg.Text, "build the thing")
	}
	if !msg.AddressedToBot {
		t.Error("group message mentioning the bot should be AddressedToBot")
	}
	if msg.Source.ChatType != channel.ChatTypeGroup {
		t.Errorf("group channel should map to ChatTypeGroup, got %q", msg.Source.ChatType)
	}
	if msg.Source.SenderID != "user_9" || msg.Source.ChatID != "grp_1" {
		t.Errorf("source routing = %+v", msg.Source)
	}
}

// TestChannelMessage_DMAlwaysAddressed confirms a DM is always addressed to the
// bot even without an @mention.
func TestChannelMessage_DMAlwaysAddressed(t *testing.T) {
	c := &octoChannel{logger: testLogger()}
	m := transport.BotMessage{
		MessageID:   "msg_2",
		FromUID:     "user_9",
		ChannelID:   "dm_1",
		ChannelType: transport.ChannelDM,
		Payload:     transport.MessagePayload{Content: "hello"},
	}
	msg := c.channelMessage("robot_1", m)
	if !msg.AddressedToBot {
		t.Error("DM should always be AddressedToBot")
	}
	if msg.Source.ChatType != channel.ChatTypeP2P {
		t.Errorf("DM should map to ChatTypeP2P, got %q", msg.Source.ChatType)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}
