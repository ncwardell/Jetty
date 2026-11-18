package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// =============================================================================
// Wedged-agent self-restart
// =============================================================================
//
// /api/livez was built to be free: no goroutine, no timer, nothing runs until
// someone asks (see livez.go and ROADMAP.md). That was the right call while
// the question was "can an operator tell the agent is wedged?". The
// 2026-11-18 incident answered a different question: Radxa's agent wedged
// with stateMu write-held by a command that never returned, livez answered
// 200 the whole time, every keyed API call hung forever - and nothing acted,
// because nothing was asking, and nothing that asked could restart a
// process from outside the host.
//
// So there is now exactly one background goroutine, and it is kept as cheap
// as the problem allows: every WedgeProbeInterval it polls TryRLock for at
// most WedgeProbeWait. On a healthy agent the first TryRLock succeeds and the
// whole tick is two atomic operations. No goroutine is spawned per tick, so a
// lock that never comes back cannot leak one goroutine per probe (the trap
// with "RLock in a goroutine, select on a timeout").
//
// If the lock stays unacquirable continuously for JETTY_WEDGE_RESTART_AFTER,
// the watchdog dumps every goroutine stack (to the log AND to a file in the
// data dir, because the dump is the only evidence of what held the lock) and
// exits non-zero. Docker's restart policy (unless-stopped) brings the agent
// back.
//
// Why exiting is safe for workloads:
//   - Workloads are separate docker compose projects on the host's docker
//     daemon, not children of this process. Exiting does not stop them.
//   - We call os.Exit directly and deliberately do NOT run Agent.Stop().
//     Stop() calls gracefulDrain, which announces a memberlist Leave so peers
//     immediately fail over every revivable workload we own - the wrong thing
//     for a restart we expect to finish in under a minute. Stop() would also
//     block on stateMu (cleanupNetwork takes it), i.e. on the very wedge we
//     are escaping.
//   - What does not survive: the cloudflared child (respawned on start), and
//     mesh plumbing this process owns (jetty0 is recreated by initNetwork and
//     workload IP/DNAT by autostart/reconcile) - the same as any agent
//     restart or image update.

// WedgeRestartAfterDefault is how long stateMu may stay unacquirable before
// the agent restarts itself. Long enough that no legitimate critical section
// comes close (state writes are microseconds; even the slow exec paths being
// moved out from under the lock are bounded at seconds), short enough that a
// wedged node is back well inside a typical outage window.
const WedgeRestartAfterDefault = 3 * time.Minute

const (
	// WedgeProbeInterval is how often the watchdog checks the lock.
	WedgeProbeInterval = 10 * time.Second
	// WedgeProbeWait bounds a single check. TryRLock also fails while a
	// writer is merely queued, so one failed attempt proves nothing; polling
	// across a couple of seconds finds the gap between ordinary writes.
	WedgeProbeWait = 2 * time.Second
	// wedgeProbePoll is the spacing of TryRLock attempts within one check.
	wedgeProbePoll = 50 * time.Millisecond
	// wedgeExitCode is distinctive so `docker inspect` / restart logs show
	// this was the watchdog and not a crash.
	wedgeExitCode = 3
	// wedgeDumpKeep bounds how many goroutine dump files we keep in dataDir.
	wedgeDumpKeep = 5
	// livezWedgedAfter is how long the watchdog must have seen the lock
	// unacquirable before /api/livez calls the node WEDGED on that basis
	// alone. A wedge on a quiet node does not pile up goroutines, so the
	// goroutine-count heuristic by itself can miss it.
	livezWedgedAfter = 60 * time.Second
)

// wedgeExit is os.Exit, swappable so tests can observe the decision.
var wedgeExit = os.Exit

// wedgeProbeWaitFor is WedgeProbeWait, swappable so tests do not spend 2s per
// probe.
var wedgeProbeWaitFor = WedgeProbeWait

// wedgeRestartAfter reads JETTY_WEDGE_RESTART_AFTER. 0 disables the restart;
// the watchdog still runs and still feeds /api/livez.
func wedgeRestartAfter() time.Duration {
	return getEnvDuration("JETTY_WEDGE_RESTART_AFTER", WedgeRestartAfterDefault)
}

// stateLockUnacquirableFor is how long the watchdog has continuously failed
// to acquire stateMu (0 while healthy). Lock-free, so livez can call it.
func (a *Agent) stateLockUnacquirableFor(now time.Time) time.Duration {
	since := a.lockUnacquirableSince.Load()
	if since == 0 {
		return 0
	}
	return now.Sub(time.Unix(0, since))
}

// probeStateLock reports whether stateMu could be read-locked within wait.
// Never blocks on the lock itself.
func (a *Agent) probeStateLock(wait time.Duration) bool {
	deadline := time.Now().Add(wait)
	for {
		if a.stateMu.TryRLock() {
			a.stateMu.RUnlock()
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(wedgeProbePoll)
	}
}

// wedgeCheck runs one probe and returns true if the agent should restart.
// Split from the loop so tests can drive it without timers.
func (a *Agent) wedgeCheck(now time.Time, threshold time.Duration) bool {
	if a.probeStateLock(wedgeProbeWaitFor) {
		if a.lockUnacquirableSince.Swap(0) != 0 {
			logInfof("Wedge watchdog: state lock acquirable again")
		}
		return false
	}
	if a.lockUnacquirableSince.CompareAndSwap(0, now.UnixNano()) {
		logWarnf("Wedge watchdog: state lock not acquirable within %v - watching", wedgeProbeWaitFor)
		return false
	}
	stuck := a.stateLockUnacquirableFor(now)
	return threshold > 0 && stuck >= threshold
}

// wedgeWatchdogLoop is the single long-lived prober. Runs for the life of the
// process; it is the thing that must keep working when nothing else does.
func (a *Agent) wedgeWatchdogLoop() {
	threshold := wedgeRestartAfter()
	a.wedgeThreshold.Store(int64(threshold))
	if threshold == 0 {
		logInfof("Wedge watchdog: self-restart disabled (JETTY_WEDGE_RESTART_AFTER=0); still reporting to /api/livez")
	}

	tick := time.NewTicker(WedgeProbeInterval)
	defer tick.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-tick.C:
		}
		now := time.Now()
		if !a.wedgeCheck(now, threshold) {
			continue
		}
		a.restartWedged(a.stateLockUnacquirableFor(now), threshold)
		return // only reached when wedgeExit is stubbed
	}
}

// restartWedged dumps every goroutine stack and exits so Docker restarts us.
func (a *Agent) restartWedged(stuck, threshold time.Duration) {
	stacks := allGoroutineStacks()

	logErrorf("Wedge watchdog: state lock has not been acquirable for %v (threshold %v, %d goroutines). "+
		"The control plane is WEDGED; exiting with code %d so the container restart policy brings the "+
		"agent back. Workload containers are separate docker projects and keep running. "+
		"Goroutine dump follows.", stuck.Round(time.Second), threshold, runtime.NumGoroutine(), wedgeExitCode)

	// The dump is the only record of what held the lock. Write it straight to
	// stderr (one write, not thousands of log records) and to a file in the
	// data dir, which survives the restart even if the container is
	// recreated and its log with it.
	fmt.Fprintf(os.Stderr, "===== jetty wedge goroutine dump %s =====\n%s\n===== end dump =====\n",
		time.Now().UTC().Format(time.RFC3339), stacks)
	if path, err := a.saveWedgeDump(stacks); err != nil {
		logErrorf("Wedge watchdog: could not save goroutine dump: %v", err)
	} else {
		logErrorf("Wedge watchdog: goroutine dump saved to %s", path)
	}

	wedgeExit(wedgeExitCode)
}

// allGoroutineStacks returns runtime.Stack(all=true), growing the buffer
// until the dump fits (capped, so a pathological pile-up cannot OOM us on
// the way out).
func allGoroutineStacks() []byte {
	const maxDump = 64 << 20
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) || len(buf) >= maxDump {
			return buf[:n]
		}
		buf = make([]byte, len(buf)*2)
	}
}

// saveWedgeDump writes the dump to <dataDir>/wedge-dumps/ and prunes all but
// the newest few.
func (a *Agent) saveWedgeDump(stacks []byte) (string, error) {
	if a.dataDir == "" {
		return "", fmt.Errorf("no data dir")
	}
	dir := filepath.Join(a.dataDir, "wedge-dumps")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "goroutines-"+time.Now().UTC().Format("20060102T150405Z")+".txt")
	if err := os.WriteFile(path, stacks, 0600); err != nil {
		return "", err
	}

	if entries, err := os.ReadDir(dir); err == nil {
		var names []string
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "goroutines-") {
				names = append(names, e.Name())
			}
		}
		sort.Strings(names) // timestamped names sort chronologically
		for len(names) > wedgeDumpKeep {
			os.Remove(filepath.Join(dir, names[0]))
			names = names[1:]
		}
	}
	return path, nil
}
