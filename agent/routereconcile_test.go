package agent

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Route updates used to run with stateMu held, and they fork/exec: three `ip`
// calls plus five per peer inside ensurePeerTunnel, none of them bounded.
// apiKeyMiddleware takes stateMu.RLock() on every request, and Go's RWMutex
// excludes new readers once a writer is queued - so one slow `ip` child froze
// the entire control plane, including /api/health, with the listener still
// open and answering nothing.
//
// These pin the properties that make that impossible rather than unlikely.

func TestTriggerRouteReconcileNeverBlocksUnderTheStateLock(t *testing.T) {
	// The property the whole fix rests on. Every call site is an event handler
	// holding stateMu; if the trigger could block, the deadlock would simply
	// move rather than go away.
	a := newTestAgentWithDir(t)
	a.routeReconcileCh = make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		a.stateMu.Lock()
		defer a.stateMu.Unlock()
		// Far more calls than the channel can hold.
		for i := 0; i < 1000; i++ {
			a.triggerRouteReconcile()
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("triggerRouteReconcile blocked while stateMu was held - " +
			"this is exactly the deadlock the change is meant to remove")
	}
}

func TestTriggerRouteReconcileCoalesces(t *testing.T) {
	// Reconciliation is idempotent and level-triggered: a pending request
	// already covers anything written before it runs. Queueing one per event
	// would apply stale snapshots in sequence instead.
	a := newTestAgentWithDir(t)
	a.routeReconcileCh = make(chan struct{}, 1)

	for i := 0; i < 50; i++ {
		a.triggerRouteReconcile()
	}
	if got := len(a.routeReconcileCh); got != 1 {
		t.Errorf("%d reconciles queued after 50 triggers, want 1", got)
	}
}

func TestTriggerRouteReconcileIsSafeBeforeStart(t *testing.T) {
	// Tests and partially constructed agents have no channel; triggering must
	// be a no-op rather than a nil-channel block (which blocks forever).
	a := newTestAgentWithDir(t)
	a.routeReconcileCh = nil

	done := make(chan struct{})
	go func() { defer close(done); a.triggerRouteReconcile() }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("triggerRouteReconcile blocked on a nil channel")
	}
}

func TestSnapshotRoutesReleasesTheStateLock(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.setWarpIP("100.96.0.1")
	a.hwid = "us"

	if _, ok := a.snapshotRoutes(); !ok {
		t.Fatal("snapshot refused with a WARP IP set")
	}

	// If the read lock leaked, this write lock would never be granted.
	acquired := make(chan struct{})
	go func() {
		a.stateMu.Lock()
		a.stateMu.Unlock()
		close(acquired)
	}()
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshotRoutes did not release stateMu")
	}
}

func TestSnapshotRoutesSkipsWhenWarpIsDown(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.setWarpIP("")
	if _, ok := a.snapshotRoutes(); ok {
		t.Error("snapshot succeeded with no WARP IP; there is nothing to route through")
	}
}

func TestApplyRoutesRunsWithoutHoldingTheStateLock(t *testing.T) {
	// The regression test for the outage.
	//
	// A slow command is essential here: the real `ip` fails in milliseconds
	// without root, so holding stateMu across it starves nobody and the test
	// passes either way. The production failure was specifically a command
	// that did NOT return promptly, so that is what has to be simulated.
	slow := make(chan struct{})
	restore := routeCommandRunner
	routeCommandRunner = func(_ time.Duration, _ string, _ ...string) error {
		<-slow // block until the test releases us
		return nil
	}
	t.Cleanup(func() { routeCommandRunner = restore })

	a := newTestAgentWithDir(t)
	a.hwid = "us"
	a.workloadRoutes = make(map[string]string)

	// hasTun drives the branch that programs routes.
	snap := routeSnapshot{
		warpIP:  "100.96.0.1",
		hasTun:  true,
		desired: map[string]routeTarget{"10.100.9.1": {ownerID: "peer", ownerIP: "100.96.0.2"}},
	}

	applyDone := make(chan struct{})
	go func() { defer close(applyDone); a.applyRoutes(snap) }()

	// Give applyRoutes time to reach the (blocked) command.
	time.Sleep(200 * time.Millisecond)

	// An API request only needs stateMu.RLock. It must be served *now*, while
	// route programming is stuck.
	lockOK := make(chan struct{})
	go func() {
		defer close(lockOK)
		a.stateMu.RLock()
		a.stateMu.RUnlock()
	}()

	select {
	case <-lockOK:
	case <-time.After(3 * time.Second):
		close(slow)
		<-applyDone
		t.Fatal("an API-path read lock was blocked while a route command was " +
			"stuck - fork/exec is happening under stateMu again, which is the " +
			"deadlock that took the control plane down")
	}

	close(slow)
	<-applyDone
}

func TestApplyRoutesDoesNotHoldRouteMapAcrossExec(t *testing.T) {
	// The userspace tunnel's send loop takes workloadRoutesMu for every
	// outbound packet. If applyRoutes holds it across a hung `ip`, all
	// cross-node workload traffic stops - the data-plane twin of the stateMu
	// freeze. And a route installed before the hang must already be usable.
	release := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	restore := routeCommandRunner
	routeCommandRunner = func(_ time.Duration, _ string, _ ...string) error {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n > 1 {
			<-release // every command after the first hangs
		}
		return nil
	}
	t.Cleanup(func() { routeCommandRunner = restore })

	a := newTestAgentWithDir(t)
	a.hwid = "us"
	a.workloadRoutes = map[string]string{"10.100.9.9": "old-owner"} // stale
	snap := routeSnapshot{
		warpIP: "100.96.0.1",
		hasTun: true,
		desired: map[string]routeTarget{
			"10.100.9.1": {ownerID: "peer", ownerIP: "100.96.0.2"},
			"10.100.9.2": {ownerID: "peer", ownerIP: "100.96.0.2"},
		},
	}

	applyDone := make(chan struct{})
	go func() { defer close(applyDone); a.applyRoutes(snap) }()

	// The first command (the stale delete) returns; the next hangs.
	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		n := calls
		mu.Unlock()
		if n >= 2 {
			break
		}
		select {
		case <-deadline:
			close(release)
			<-applyDone
			t.Fatal("applyRoutes never reached its second command")
		case <-time.After(5 * time.Millisecond):
		}
	}

	got := make(chan map[string]string, 1)
	go func() {
		a.workloadRoutesMu.Lock()
		cp := make(map[string]string, len(a.workloadRoutes))
		for k, v := range a.workloadRoutes {
			cp[k] = v
		}
		a.workloadRoutesMu.Unlock()
		got <- cp
	}()
	select {
	case m := <-got:
		if _, ok := m["10.100.9.9"]; ok {
			t.Error("stale route still published to the packet path")
		}
	case <-time.After(3 * time.Second):
		close(release)
		<-applyDone
		t.Fatal("workloadRoutesMu held across a hung route command - the " +
			"userspace tunnel send loop would stall all cross-node traffic")
	}

	close(release)
	<-applyDone
	a.workloadRoutesMu.Lock()
	defer a.workloadRoutesMu.Unlock()
	if len(a.workloadRoutes) != 2 {
		t.Errorf("workloadRoutes = %v, want both desired routes published", a.workloadRoutes)
	}
}

// TestNoUnboundedExecCommand is a lint-style guard. Every one-shot command
// must go through command.go's bounded helpers: an unbounded exec stalls its
// caller forever when the child hangs, and if that caller holds a lock -
// as the route path did - it takes the control plane with it.
//
// The allowlist is long-running processes that are Start()ed and supervised
// (a deadline would kill a healthy daemon or an operator's shell) and the
// self-update's fire-and-forget last resort, which is never waited on.
func TestNoUnboundedExecCommand(t *testing.T) {
	allowed := map[string][]string{
		"cloudflared.go":          {`exec.Command("cloudflared"`},
		"cloudflared_watchdog.go": {`exec.Command("cloudflared"`},
		"warp.go":                 {`exec.Command("warp-svc"`},
		"handlers_terminal.go":    {`exec.Command("docker", "exec", "-i", "-t"`, `exec.Command("/usr/bin/nsenter"`, `exec.Command(shell)`},
		"handlers_nodes.go":       {`exec.Command("docker", "stop", "-t", "5"`, `exec.Command("docker", "start"`},
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
	lines:
		for i, line := range strings.Split(string(src), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") || !strings.Contains(line, "exec.Command(") {
				continue
			}
			for _, ok := range allowed[name] {
				if strings.Contains(line, ok) {
					continue lines
				}
			}
			t.Errorf("%s:%d uses an unbounded exec.Command; use runCmd / "+
				"runCmdOutput / runCmdCombined / boundedCommand (command.go) so a "+
				"stuck child cannot stall its caller:\n  %s", name, i+1, trimmed)
		}
	}
}

// TestNoRouteReconcileUnderStateLock guards the call-site contract: handlers
// may hold stateMu when they *ask* for a reconcile, but nothing may perform
// one inline.
func TestNoRouteReconcileUnderStateLock(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	var offenders []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if name == "routereconcile.go" {
			continue // the reconciler itself
		}
		src, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			for _, bad := range []string{"a.applyRoutes(", "a.reconcileWorkloadRoutes("} {
				if strings.Contains(line, bad) {
					offenders = append(offenders, name+":"+itoa(i+1)+": "+trimmed)
				}
			}
		}
	}
	if len(offenders) > 0 {
		t.Errorf("route programming must go through triggerRouteReconcile, not "+
			"be called inline - inline calls can run under stateMu:\n  %s",
			strings.Join(offenders, "\n  "))
	}
}

func TestSnapshotRoutesSkipsLocalWorkloads(t *testing.T) {
	// Local workloads are reached via iptables DNAT; installing a route for
	// one would be wrong, not merely redundant.
	a := newTestAgentWithDir(t)
	a.hwid = "us"
	a.setWarpIP("100.96.0.1")

	a.stateMu.Lock()
	a.state.Peers["peer"] = &Peer{ID: "peer", Name: "peer", IP: "100.96.0.2", Healthy: true}
	a.state.Workloads["10.100.0.1"] = &Workload{Name: "mine", IP: "10.100.0.1", Owner: "us"}
	a.state.Workloads["10.100.0.2"] = &Workload{Name: "theirs", IP: "10.100.0.2", Owner: "peer"}
	a.stateMu.Unlock()

	snap, ok := a.snapshotRoutes()
	if !ok {
		t.Fatal("snapshot refused")
	}
	if _, has := snap.desired["10.100.0.1"]; has {
		t.Error("a local workload was given a route")
	}
	if _, has := snap.desired["10.100.0.2"]; !has {
		t.Error("a remote workload on a healthy peer was not given a route")
	}
}

func TestSnapshotRoutesSkipsUnhealthyPeers(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.hwid = "us"
	a.setWarpIP("100.96.0.1")

	a.stateMu.Lock()
	a.state.Peers["dead"] = &Peer{ID: "dead", Name: "dead", IP: "100.96.0.3", Healthy: false}
	a.state.Workloads["10.100.0.5"] = &Workload{Name: "orphan", IP: "10.100.0.5", Owner: "dead"}
	a.stateMu.Unlock()

	snap, _ := a.snapshotRoutes()
	if _, has := snap.desired["10.100.0.5"]; has {
		t.Error("a route was pointed at an unhealthy peer, which black-holes traffic")
	}
}

func TestConcurrentTriggersAndSnapshotsDoNotRace(t *testing.T) {
	// Run under -race: triggers come from event handlers on many goroutines
	// while the reconciler snapshots.
	a := newTestAgentWithDir(t)
	a.hwid = "us"
	a.setWarpIP("100.96.0.1")
	a.routeReconcileCh = make(chan struct{}, 1)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				a.stateMu.Lock()
				a.state.Workloads["10.100.1."+itoa(n)] = &Workload{
					Name: "w", IP: "10.100.1." + itoa(n), Owner: "peer",
				}
				a.triggerRouteReconcile()
				a.stateMu.Unlock()
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			a.snapshotRoutes()
		}
	}()
	wg.Wait()
}
