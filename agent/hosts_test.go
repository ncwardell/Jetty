package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// updateHosts held stateMu.RLock across os.ReadFile and os.WriteFile. A read
// lock does not block other readers directly, but a writer queuing behind it
// does - and Go's RWMutex then excludes every subsequent reader, including
// apiKeyMiddleware. So a slow /etc/hosts write wedged the control plane the
// same way route programming did, one hop further round.

func hostsTestAgent(t *testing.T) *Agent {
	t.Helper()
	a := newTestAgentWithDir(t)
	a.hwid = "us"
	a.hostname = "node-a"
	a.setWarpIP("100.96.0.1")
	a.hostsFile = filepath.Join(t.TempDir(), "hosts")
	a.workloadRoutes = make(map[string]string)
	os.WriteFile(a.hostsFile, []byte("127.0.0.1\tlocalhost\n"), 0644)
	return a
}

func TestSnapshotHostsReleasesTheStateLock(t *testing.T) {
	a := hostsTestAgent(t)
	a.snapshotHosts()

	acquired := make(chan struct{})
	go func() {
		a.stateMu.Lock()
		a.stateMu.Unlock()
		close(acquired)
	}()
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshotHosts did not release stateMu")
	}
}

func TestUpdateHostsDoesNotHoldTheStateLockDuringFileIO(t *testing.T) {
	// The regression test. While updateHosts is doing its read-modify-write,
	// an API-path read lock must still be served.
	a := hostsTestAgent(t)

	// A directory in place of the file makes WriteFile fail slowly-ish and,
	// more importantly, proves the I/O path runs at all.
	a.stateMu.Lock()
	a.state.Peers["p"] = &Peer{ID: "p", Name: "peer-1", IP: "100.96.0.2", Healthy: true}
	a.state.Workloads["10.100.0.1"] = &Workload{Name: "app", IP: "10.100.0.1", Owner: "us"}
	a.stateMu.Unlock()

	done := make(chan struct{})
	go func() { defer close(done); a.updateHosts() }()

	lockOK := make(chan struct{})
	go func() {
		defer close(lockOK)
		for i := 0; i < 100; i++ {
			a.stateMu.RLock()
			a.stateMu.RUnlock()
		}
	}()

	select {
	case <-lockOK:
	case <-time.After(5 * time.Second):
		t.Fatal("API-path read locks were starved during /etc/hosts I/O")
	}
	<-done
}

// TestHostsBlockIsStableAcrossCalls covers a bug the split exposed: the block
// was built by ranging over maps, and Go randomises map iteration order. So
// the rendered block differed between calls, its hash never matched the
// previous one, and the "skip the write if nothing changed" optimisation
// never fired - /etc/hosts was rewritten on every gossip tick regardless.
func TestHostsBlockIsStableAcrossCalls(t *testing.T) {
	a := hostsTestAgent(t)

	a.stateMu.Lock()
	for _, n := range []string{"alpha", "bravo", "charlie", "delta", "echo"} {
		a.state.Peers[n] = &Peer{ID: n, Name: n, IP: "100.96.0.9", Healthy: true}
		a.state.Workloads["10.100.0."+n[:1]] = &Workload{
			Name: "wl-" + n, IP: "10.100.0." + n[:1], Owner: "us",
		}
	}
	a.stateMu.Unlock()

	first := renderHostsFor(t, a)
	for i := 0; i < 20; i++ {
		if got := renderHostsFor(t, a); got != first {
			t.Fatalf("hosts block changed between identical calls - map iteration "+
				"order is leaking into the output, so the content hash never "+
				"matches and the file is rewritten every tick.\nfirst:\n%s\ngot:\n%s",
				first, got)
		}
	}
}

// renderHostsFor runs updateHosts and returns the managed block.
func renderHostsFor(t *testing.T, a *Agent) string {
	t.Helper()
	a.updateHosts()
	data, err := os.ReadFile(a.hostsFile)
	if err != nil {
		t.Fatalf("read hosts: %v", err)
	}
	out := string(data)
	start := strings.Index(out, "# JETTY START")
	end := strings.Index(out, "# JETTY END")
	if start == -1 || end == -1 {
		t.Fatalf("no JETTY block in:\n%s", out)
	}
	return out[start : end+len("# JETTY END")]
}

func TestUpdateHostsSkipsTheWriteWhenNothingChanged(t *testing.T) {
	// The point of the hash. With unstable ordering this never held.
	a := hostsTestAgent(t)
	a.stateMu.Lock()
	a.state.Peers["p"] = &Peer{ID: "p", Name: "peer-1", IP: "100.96.0.2", Healthy: true}
	a.stateMu.Unlock()

	a.updateHosts()
	info, err := os.Stat(a.hostsFile)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	firstMod := info.ModTime()

	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 5; i++ {
		a.updateHosts()
	}

	info, _ = os.Stat(a.hostsFile)
	if !info.ModTime().Equal(firstMod) {
		t.Error("/etc/hosts was rewritten despite no state change - the content " +
			"hash is not preventing write amplification")
	}
}

func TestUpdateHostsPreservesEntriesOutsideTheBlock(t *testing.T) {
	a := hostsTestAgent(t)
	os.WriteFile(a.hostsFile, []byte("127.0.0.1\tlocalhost\n1.2.3.4\thand-written\n"), 0644)

	a.updateHosts()

	data, _ := os.ReadFile(a.hostsFile)
	if !strings.Contains(string(data), "hand-written") {
		t.Error("a hand-written /etc/hosts entry was destroyed")
	}
	if !strings.Contains(string(data), "localhost") {
		t.Error("localhost was destroyed")
	}
}

func TestSnapshotHostsSortsPeersAndWorkloads(t *testing.T) {
	a := hostsTestAgent(t)
	a.stateMu.Lock()
	a.state.Peers["z"] = &Peer{ID: "z", Name: "zulu", IP: "100.96.0.9", Healthy: true}
	a.state.Peers["a"] = &Peer{ID: "a", Name: "alpha", IP: "100.96.0.8", Healthy: true}
	a.state.Workloads["10.100.0.2"] = &Workload{Name: "zeta", IP: "10.100.0.2", Owner: "us"}
	a.state.Workloads["10.100.0.1"] = &Workload{Name: "aardvark", IP: "10.100.0.1", Owner: "us"}
	a.stateMu.Unlock()

	snap := a.snapshotHosts()
	if len(snap.entries) != 5 { // self + 2 peers + 2 workloads
		t.Fatalf("got %d entries, want 5", len(snap.entries))
	}
	if snap.entries[0].note != "this node" {
		t.Errorf("first entry is %q, want self", snap.entries[0].note)
	}
	if snap.entries[1].name != "alpha" || snap.entries[2].name != "zulu" {
		t.Errorf("peers not sorted: %q, %q", snap.entries[1].name, snap.entries[2].name)
	}
	if snap.entries[3].name != "aardvark" || snap.entries[4].name != "zeta" {
		t.Errorf("workloads not sorted: %q, %q", snap.entries[3].name, snap.entries[4].name)
	}
}
