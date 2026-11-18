package agent

import (
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
)

// The helpers in command.go are the backstop for the 2026-08 / 2026-11-18
// control-plane freezes: a child that never exits must cost its caller one
// timeout, not forever.

func TestRunCmdTimesOutOnAHungChild(t *testing.T) {
	start := time.Now()
	err := runCmd(100*time.Millisecond, "sleep", "5")
	if err == nil {
		t.Fatal("a command that outran its timeout returned no error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("error = %v, want it to say the command timed out", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("took %s to give up on a 100ms timeout", elapsed)
	}
}

func TestRunCmdCombinedIsNotHeldOpenByAGrandchildsPipes(t *testing.T) {
	// The case WaitDelay exists for. `sh` is killed at the deadline, but the
	// backgrounded sleep inherited stdout/stderr and keeps the pipe open.
	// Without WaitDelay, CombinedOutput waits for that pipe to close - the
	// full 30s - even though the context fired, silently defeating the
	// timeout. With it, we are back within timeout + cmdWaitDelay.
	start := time.Now()
	_, err := runCmdCombined(100*time.Millisecond, "sh", "-c", "sleep 30 & sleep 30")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected an error from a killed command")
	}
	if limit := cmdWaitDelay + 3*time.Second; elapsed > limit {
		t.Fatalf("took %s; a grandchild holding the output pipe kept Wait "+
			"blocked past the deadline (limit %s)", elapsed, limit)
	}
}

func TestRunCmdOutputReturnsStdoutOnly(t *testing.T) {
	out, err := runCmdOutput(5*time.Second, "sh", "-c", "echo out; echo err >&2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != "out" {
		t.Errorf("stdout = %q, want %q - parsed output must not include stderr", got, "out")
	}
}

func TestRunCmdPreservesExitError(t *testing.T) {
	err := runCmd(5*time.Second, "false")
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("err = %#v, want an *exec.ExitError for a non-zero exit", err)
	}
	if err := runCmd(5*time.Second, "true"); err != nil {
		t.Errorf("a successful command returned %v", err)
	}
}

func TestBoundedCmdErrDoesNotLeakArgv(t *testing.T) {
	// Callers pass secrets on the command line (warp-cli connector new
	// <token>, docker create -e JETTY_SECRET=...) and log these errors.
	err := runCmd(100*time.Millisecond, "sh", "-c", "sleep 5 # SECRET-TOKEN-xyz")
	if err == nil {
		t.Fatal("expected a timeout")
	}
	if strings.Contains(err.Error(), "SECRET-TOKEN-xyz") {
		t.Errorf("timeout error echoed argv into a loggable string: %v", err)
	}
}

func TestTunnelEndpointsMatch(t *testing.T) {
	show := "tun_ab12cd34: ip/ip remote 100.96.0.2 local 100.96.0.1 ttl inherit\n"
	cases := []struct {
		local, remote string
		want          bool
	}{
		{"100.96.0.1", "100.96.0.2", true},
		// The local end is in the encapsulation header; a WARP renumber
		// must cause a rebuild even though the remote is unchanged.
		{"100.96.0.9", "100.96.0.2", false},
		// Prefix of the real remote - the old strings.Contains accepted it.
		{"100.96.0.1", "100.96.0.", false},
		{"100.96.0.1", "100.96.0.21", false},
	}
	for _, c := range cases {
		if got := tunnelEndpointsMatch(show, c.local, c.remote); got != c.want {
			t.Errorf("tunnelEndpointsMatch(local=%s, remote=%s) = %v, want %v", c.local, c.remote, got, c.want)
		}
	}
	if tunnelEndpointsMatch("", "100.96.0.1", "100.96.0.2") {
		t.Error("an absent tunnel (empty show output) matched")
	}
}

// stubTunnelCommands replaces the route/tunnel exec indirection for one test.
// show returns the `ip tunnel show` output; every call is recorded.
type stubTunnelCommands struct {
	mu    sync.Mutex
	calls []string
	show  string
	block chan struct{} // if non-nil, every command waits on it
}

func (s *stubTunnelCommands) install(t *testing.T) {
	t.Helper()
	restoreRun, restoreOut := routeCommandRunner, routeCommandOutput
	routeCommandRunner = func(_ time.Duration, name string, args ...string) error {
		_, err := s.run(name, args...)
		return err
	}
	routeCommandOutput = func(_ time.Duration, name string, args ...string) ([]byte, error) {
		return s.run(name, args...)
	}
	t.Cleanup(func() { routeCommandRunner, routeCommandOutput = restoreRun, restoreOut })
}

func (s *stubTunnelCommands) run(name string, args ...string) ([]byte, error) {
	if s.block != nil {
		<-s.block
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	line := name + " " + strings.Join(args, " ")
	s.calls = append(s.calls, line)
	if strings.HasPrefix(line, "ip tunnel show") {
		return []byte(s.show), nil
	}
	return nil, nil
}

func (s *stubTunnelCommands) joined() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.calls, "\n")
}

func TestEnsurePeerTunnelUsesCurrentAddressNotCallersSnapshot(t *testing.T) {
	// Callers snapshot a peer's address and release stateMu before calling.
	// If gossip renumbered the peer in between, the tunnel must follow the
	// current address - otherwise the stale caller wins the race and the
	// tunnel points at an address the peer no longer has.
	stub := &stubTunnelCommands{}
	stub.install(t)

	a := newTestAgentWithDir(t)
	a.tunnelMode = "ipip"
	a.setWarpIP("100.96.0.1")
	a.state.Peers["peer"] = &Peer{ID: "peer", Name: "peer", IP: "100.96.0.3"}

	if err := a.ensurePeerTunnel("peer", "100.96.0.2" /* stale */); err != nil {
		t.Fatalf("ensurePeerTunnel: %v", err)
	}
	calls := stub.joined()
	if !strings.Contains(calls, "remote 100.96.0.3") {
		t.Errorf("tunnel not built to the peer's current address:\n%s", calls)
	}
	if strings.Contains(calls, "remote 100.96.0.2") {
		t.Errorf("tunnel built to the caller's stale address:\n%s", calls)
	}
}

func TestEnsurePeerTunnelSkipsRemovedPeer(t *testing.T) {
	// A reconcile snapshot taken before a node removal must not resurrect the
	// tunnel that removePeerTunnel just tore down.
	stub := &stubTunnelCommands{}
	stub.install(t)

	a := newTestAgentWithDir(t)
	a.tunnelMode = "ipip"
	a.setWarpIP("100.96.0.1")

	if err := a.ensurePeerTunnel("gone", "100.96.0.2"); err != nil {
		t.Fatalf("ensurePeerTunnel: %v", err)
	}
	if calls := stub.joined(); calls != "" {
		t.Errorf("ran commands for a peer no longer in state:\n%s", calls)
	}
}

func TestEnsurePeerTunnelRebuildsAfterLocalRenumber(t *testing.T) {
	stub := &stubTunnelCommands{
		show: "tun_peer: ip/ip remote 100.96.0.2 local 100.96.0.1 ttl inherit",
	}
	stub.install(t)

	a := newTestAgentWithDir(t)
	a.tunnelMode = "ipip"
	a.setWarpIP("100.96.0.7") // our WARP address changed
	a.state.Peers["peer"] = &Peer{ID: "peer", Name: "peer", IP: "100.96.0.2"}

	if err := a.ensurePeerTunnel("peer", "100.96.0.2"); err != nil {
		t.Fatalf("ensurePeerTunnel: %v", err)
	}
	if calls := stub.joined(); !strings.Contains(calls, "local 100.96.0.7 remote 100.96.0.2") {
		t.Errorf("tunnel with a stale local endpoint was left in place:\n%s", calls)
	}
}

func TestNotifyUpdateDoesNotHoldStateLockAcrossTunnelExec(t *testing.T) {
	// The remaining lock-across-exec site after route reconciliation moved
	// off stateMu: a peer IP change in NotifyUpdate called ensurePeerTunnel
	// while holding stateMu for writing. A WARP renumber is exactly when `ip`
	// is most likely to hang, and a stuck writer blocks every API reader.
	//
	// The stub blocks every command until released, standing in for an `ip`
	// that does not return.
	stub := &stubTunnelCommands{block: make(chan struct{})}
	stub.install(t)

	a := newTestAgentWithDir(t)
	a.hwid = "us"
	a.tunnelMode = "ipip"
	a.setWarpIP("100.96.0.1")
	a.state.Peers["peer-b"] = &Peer{ID: "peer-b", Name: "node-b", IP: "100.96.0.2"}

	e := &jettyEventDelegate{agent: a}
	meta, _ := json.Marshal(NodeMeta{ID: "peer-b", Name: "node-b", IP: "100.96.0.9"})

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.NotifyUpdate(&memberlist.Node{Name: "peer-b", Meta: meta})
	}()

	// Wait until the update is actually stuck in a command.
	deadline := time.After(3 * time.Second)
	for {
		a.stateMu.RLock()
		ip := a.state.Peers["peer-b"].IP
		a.stateMu.RUnlock()
		if ip == "100.96.0.9" {
			break
		}
		select {
		case <-deadline:
			close(stub.block)
			<-done
			t.Fatal("NotifyUpdate never applied the new IP")
		case <-time.After(10 * time.Millisecond):
		}
	}
	time.Sleep(50 * time.Millisecond)

	// Both a reader (every API request) and a writer (gossip, handlers) must
	// get the lock while the tunnel command is hung.
	got := make(chan struct{})
	go func() {
		defer close(got)
		a.stateMu.Lock()
		a.stateMu.Unlock()
		a.stateMu.RLock()
		a.stateMu.RUnlock()
	}()
	select {
	case <-got:
	case <-time.After(3 * time.Second):
		close(stub.block)
		<-done
		t.Fatal("stateMu was unavailable while a tunnel command was hung - " +
			"NotifyUpdate is exec'ing under the state lock again")
	}

	close(stub.block)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("NotifyUpdate did not finish after the command was released")
	}
}
