package agent

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// bridgeWebSockets had no test while it was buried in the terminal handler.
// Now that it is a shared primitive - the thing every future node-scoped
// streaming endpoint will proxy through - the properties are worth pinning,
// particularly the goroutine cleanup, which leaks silently once per session.

// wsEcho stands up a server that echoes frames back verbatim and returns a
// client connection to it.
func wsEcho(t *testing.T) (*websocket.Conn, func()) {
	t.Helper()
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			mt, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			if err := c.WriteMessage(mt, data); err != nil {
				return
			}
		}
	}))
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		srv.Close()
		t.Fatalf("dial echo server: %v", err)
	}
	return conn, func() { conn.Close(); srv.Close() }
}

// wsBridgedPair returns a client connection whose traffic bridgeWebSockets is
// forwarding to an echo server - the same topology as the host-shell proxy.
func wsBridgedPair(t *testing.T) (*websocket.Conn, chan struct{}, func()) {
	t.Helper()
	peer, closePeer := wsEcho(t)

	bridged := make(chan struct{})
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		bridgeWebSockets(c, peer)
		close(bridged)
	}))

	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		srv.Close()
		closePeer()
		t.Fatalf("dial bridge: %v", err)
	}
	return client, bridged, func() { client.Close(); srv.Close(); closePeer() }
}

func TestBridgeForwardsMessageTypesVerbatim(t *testing.T) {
	// The terminal protocol distinguishes text from binary frames. A bridge
	// that normalised everything to one type would break it in a way that
	// only shows up on a real terminal session.
	client, _, cleanup := wsBridgedPair(t)
	defer cleanup()

	for _, tc := range []struct {
		mt   int
		data string
	}{
		{websocket.TextMessage, "resize:80x24"},
		{websocket.BinaryMessage, "\x00\x01\xff raw bytes"},
		{websocket.TextMessage, ""},
	} {
		if err := client.WriteMessage(tc.mt, []byte(tc.data)); err != nil {
			t.Fatalf("write: %v", err)
		}
		client.SetReadDeadline(time.Now().Add(5 * time.Second))
		gotType, gotData, err := client.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if gotType != tc.mt {
			t.Errorf("message type %d came back as %d", tc.mt, gotType)
		}
		if string(gotData) != tc.data {
			t.Errorf("payload %q came back as %q", tc.data, gotData)
		}
	}
}

func TestBridgeReturnsWhenEitherSideCloses(t *testing.T) {
	client, bridged, cleanup := wsBridgedPair(t)
	defer cleanup()

	client.Close()

	select {
	case <-bridged:
	case <-time.After(5 * time.Second):
		t.Fatal("bridgeWebSockets did not return after the client closed - " +
			"the handler's deferred Close never runs and the session is pinned")
	}
}

func TestBridgeLeavesNoGoroutineBehind(t *testing.T) {
	// The reason both connections are closed before the second <-done. If the
	// still-blocked pump is never woken, one goroutine leaks per proxied
	// session and the agent bleeds until restart.
	settle := func() int {
		for i := 0; i < 40; i++ {
			time.Sleep(50 * time.Millisecond)
			runtime.GC()
		}
		return runtime.NumGoroutine()
	}
	before := settle()

	for i := 0; i < 25; i++ {
		client, bridged, cleanup := wsBridgedPair(t)
		client.WriteMessage(websocket.TextMessage, []byte("ping"))
		client.SetReadDeadline(time.Now().Add(5 * time.Second))
		client.ReadMessage()
		client.Close()
		select {
		case <-bridged:
		case <-time.After(5 * time.Second):
			cleanup()
			t.Fatal("bridge did not return")
		}
		cleanup()
	}

	after := settle()
	// 25 sessions leaking a pump each would show up as ~25 extra goroutines;
	// the slack absorbs httptest/transport teardown noise.
	if after-before > 10 {
		t.Errorf("goroutines went %d -> %d across 25 bridged sessions - "+
			"a pump is not being woken when its peer closes", before, after)
	}
}
