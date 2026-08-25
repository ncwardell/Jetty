package agent

import (
	"github.com/gorilla/websocket"
)

// =============================================================================
// WebSocket bridging
// =============================================================================
//
// This lives here rather than in handlers_terminal.go because it is not about
// terminals. It forwards message type and payload verbatim and never inspects
// either, so it is a transport primitive: any request that has to be served by
// a peer rather than by us can be proxied with it.
//
// That matters for what comes next. Node-scoped endpoints - a shell, a log
// stream, a metrics feed - are all the same shape: figure out which node owns
// the request, dial that node, and get out of the way. Leaving the bridge
// buried in the terminal handler made it look terminal-specific and invited
// the next such endpoint to grow its own copy.

// bridgeWebSockets shuffles messages between two WebSockets until either side
// closes. Forwards message types (binary/text/control) verbatim so whatever
// frame protocol the endpoints agreed on passes through unchanged.
//
// The two ReadMessage loops can race against each other on the writes;
// gorilla/websocket's NextWriter is not safe for concurrent use, but each
// goroutine writes to a different connection so this is fine.
//
// Both connections are closed before returning. That is not tidiness: it is
// what wakes the still-blocked pump's ReadMessage so the second goroutine
// exits. Without it that goroutine leaks for the life of the process, once per
// proxied session.
func bridgeWebSockets(a, b *websocket.Conn) {
	done := make(chan struct{}, 2)

	pump := func(src, dst *websocket.Conn) {
		defer func() { done <- struct{}{} }()
		for {
			msgType, data, err := src.ReadMessage()
			if err != nil {
				return
			}
			if err := dst.WriteMessage(msgType, data); err != nil {
				return
			}
		}
	}

	go pump(a, b)
	go pump(b, a)

	<-done
	a.Close()
	b.Close()
	<-done
}
