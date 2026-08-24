package agent

import (
	"testing"
	"time"
)

// Flow control for the userspace tunnel's TCP proxy. These tests pin the
// behaviour that fixes the bulk-transfer deadlock: the proxy must stop
// sending once in-flight bytes reach the peer's advertised window, and must
// release a blocked sender on teardown rather than leaking the goroutine.

func TestInitFlowSeedsWindowFromSYN(t *testing.T) {
	c := &tcpProxyConn{}
	c.initFlow(4096, 1000)

	if c.peerWindow != 4096 {
		t.Errorf("peerWindow = %d, want 4096", c.peerWindow)
	}
	if c.lastAck != 1000 {
		t.Errorf("lastAck = %d, want 1000", c.lastAck)
	}
	if c.windowCh == nil {
		t.Error("windowCh not initialised")
	}
}

func TestInitFlowZeroWindowFallsBackToDefault(t *testing.T) {
	// A SYN advertising a zero window is unusual but legal. Treating it
	// literally would wedge the flow before it started, so we fall back.
	c := &tcpProxyConn{}
	c.initFlow(0, 0)

	if c.peerWindow != defaultPeerWindow {
		t.Errorf("peerWindow = %d, want defaultPeerWindow (%d)", c.peerWindow, defaultPeerWindow)
	}
}

func TestAwaitWindowAllowsSegmentThatFits(t *testing.T) {
	c := &tcpProxyConn{localSeq: 1000}
	c.initFlow(4096, 1000)

	if !c.awaitWindow(1240) {
		t.Error("awaitWindow(1240) blocked with 4096 window and nothing in flight")
	}
}

func TestAwaitWindowUngatedWhenFlowNeverInitialised(t *testing.T) {
	// initFlow only runs on the SYN path. A flow that somehow skipped it has
	// a nil windowCh and must degrade to the old unthrottled behaviour rather
	// than blocking forever.
	c := &tcpProxyConn{localSeq: 5000, lastAck: 0, peerWindow: 0}

	if !c.awaitWindow(1240) {
		t.Error("awaitWindow blocked on an uninitialised flow; should degrade open")
	}
}

func TestAwaitWindowBlocksWhenWindowFullThenUnblocksOnAck(t *testing.T) {
	c := &tcpProxyConn{localSeq: 1000}
	c.initFlow(1000, 1000)

	// Put the full window in flight: localSeq - lastAck == 1000 == peerWindow.
	c.localSeq = 2000

	done := make(chan bool, 1)
	go func() { done <- c.awaitWindow(500) }()

	select {
	case <-done:
		t.Fatal("awaitWindow returned while the window was full")
	case <-time.After(50 * time.Millisecond):
	}

	// Peer acks everything and re-advertises. The sender must wake on the
	// channel signal, well inside windowProbeInterval - if this only passes
	// because of the poll timer, the wakeup path is broken.
	c.noteAck(2000, 1000)

	select {
	case ok := <-done:
		if !ok {
			t.Error("awaitWindow returned false after the window opened")
		}
	case <-time.After(windowProbeInterval - 50*time.Millisecond):
		t.Fatal("awaitWindow did not wake on the window-update signal")
	}
}

func TestAwaitWindowWakesOnProbeTimerAfterLostUpdate(t *testing.T) {
	// A window update that never arrives (or a signal lost to the 1-slot
	// channel) must not wedge the flow permanently - the probe timer re-checks.
	c := &tcpProxyConn{localSeq: 2000}
	c.initFlow(1000, 1000)

	done := make(chan bool, 1)
	go func() { done <- c.awaitWindow(500) }()

	// Mutate the window directly, with no channel signal at all.
	time.Sleep(20 * time.Millisecond)
	c.mu.Lock()
	c.peerWindow = 65535
	c.mu.Unlock()

	select {
	case ok := <-done:
		if !ok {
			t.Error("awaitWindow returned false after the window opened")
		}
	case <-time.After(2 * windowProbeInterval):
		t.Fatal("probe timer did not re-check the window")
	}
}

func TestCloseFlowReleasesBlockedSender(t *testing.T) {
	// This is the goroutine-leak fix: every teardown path must call
	// closeFlow, or a sender parked here is stranded for the process
	// lifetime.
	c := &tcpProxyConn{localSeq: 2000}
	c.initFlow(1000, 1000)

	done := make(chan bool, 1)
	go func() { done <- c.awaitWindow(500) }()

	select {
	case <-done:
		t.Fatal("awaitWindow returned while the window was full")
	case <-time.After(50 * time.Millisecond):
	}

	c.closeFlow()

	select {
	case ok := <-done:
		if ok {
			t.Error("awaitWindow returned true after closeFlow; sender should stop")
		}
	case <-time.After(windowProbeInterval - 50*time.Millisecond):
		t.Fatal("closeFlow did not release the blocked sender")
	}
}

func TestAwaitWindowReturnsFalseWhenAlreadyClosed(t *testing.T) {
	c := &tcpProxyConn{localSeq: 1000}
	c.initFlow(65535, 1000)
	c.closeFlow()

	if c.awaitWindow(1) {
		t.Error("awaitWindow returned true on a closed flow")
	}
}

func TestNoteAckIgnoresStaleAcks(t *testing.T) {
	// Duplicate and reordered ACKs must not rewind lastAck, which would
	// understate bytes in flight and let the sender overrun the receiver.
	c := &tcpProxyConn{}
	c.initFlow(65535, 1000)
	c.noteAck(5000, 65535)
	c.noteAck(3000, 65535)

	c.mu.Lock()
	got := c.lastAck
	c.mu.Unlock()

	if got != 5000 {
		t.Errorf("lastAck = %d after a stale ACK, want 5000", got)
	}
}

func TestNoteAckHandlesSequenceWraparound(t *testing.T) {
	// Sequence numbers wrap at 2^32. A naive `ack > lastAck` comparison
	// treats the wrap as a stale ACK and stalls the flow permanently; the
	// serial-arithmetic rule int32(ack-lastAck) > 0 handles it.
	const nearMax = uint32(1<<32 - 100)

	c := &tcpProxyConn{}
	c.initFlow(65535, nearMax)
	c.noteAck(50, 65535) // wrapped past zero

	c.mu.Lock()
	got := c.lastAck
	c.mu.Unlock()

	if got != 50 {
		t.Errorf("lastAck = %d, want 50 (wrapped ACK must advance)", got)
	}
}

func TestAwaitWindowAccountsForWrappedInFlight(t *testing.T) {
	// In-flight is localSeq - lastAck in wrapping arithmetic. Straddling the
	// wrap must not be read as ~4GB in flight (which would block forever) nor
	// as zero (which would let the sender overrun).
	const nearMax = uint32(1<<32 - 100)

	c := &tcpProxyConn{localSeq: 100} // wrapped: 200 bytes in flight
	c.initFlow(1000, nearMax)

	if !c.awaitWindow(500) {
		t.Error("awaitWindow blocked with 200 bytes in flight against a 1000 window")
	}

	// Advance past the wrap to 1000 bytes in flight; +500 exceeds the window.
	// Computed at runtime -- as a constant expression this overflows uint32.
	base := nearMax
	c.localSeq = base + 1000
	done := make(chan bool, 1)
	go func() { done <- c.awaitWindow(500) }()

	select {
	case <-done:
		t.Fatal("awaitWindow allowed a send past the window across a wrap")
	case <-time.After(50 * time.Millisecond):
		c.closeFlow()
		<-done
	}
}
