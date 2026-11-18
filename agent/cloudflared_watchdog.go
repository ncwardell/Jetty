package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"time"
)

// =============================================================================
// Cloudflared readiness watchdog
// =============================================================================
//
// monitorCloudflared only restarts cloudflared when the process EXITS. On
// 2026-11-18 a connector was found that had stayed alive for 19 days holding
// zero edge connections: the process was up, the agent reported the tunnel as
// "running", and every public request routed to that connector had nowhere to
// go. Nothing exited, so nothing restarted it.
//
// cloudflared already knows whether it is connected - its metrics server
// answers /ready with 200 and readyConnections>0 when it holds at least one
// edge connection, and 503 otherwise. So we start it with --metrics on a fixed
// loopback address and poll that. If a process has been not-ready
// continuously for CloudflaredNotReadyRestart, we kill it, and the monitor's
// existing respawn path brings up a fresh one. Killing is the whole remedy:
// a new process re-resolves the edge, re-authenticates and re-dials, which is
// what an operator would have done by hand.
//
// Rules this keeps:
//   - Never fights a deliberate stop. The watchdog exits when cfStopCh closes
//     (stopCloudflared: token removed, node disabled, shutdown) and when the
//     monitor gives up, and re-checks both under cfMu before killing.
//   - Never touches stateMu. The probe is HTTP, and HTTP under the state lock
//     is how the agent wedged in the same incident.
//   - Never judges a process it cannot observe. If the metrics endpoint has
//     never answered for this process (port clash, an old cloudflared), the
//     watchdog logs that once and leaves the process alone - restarting a
//     healthy tunnel every three minutes would be worse than the bug.

// cloudflaredMetricsAddr is where we ask cloudflared to serve metrics/readiness.
func cloudflaredMetricsAddr() string {
	return getEnv("JETTY_CF_METRICS_ADDR", CloudflaredMetricsAddrDefault)
}

// cloudflaredNotReadyRestartAfter is the not-ready tolerance. 0 disables the
// restart (the probe still runs, so readiness is still reported).
func cloudflaredNotReadyRestartAfter() time.Duration {
	return getEnvDuration("JETTY_CF_READY_RESTART_AFTER", CloudflaredNotReadyRestart)
}

// newCloudflaredCmd builds the cloudflared command line. One place, because
// there are two start sites (first start and the monitor's respawn) and they
// must agree on --metrics or the watchdog probes nothing after a restart.
//
// --no-autoupdate is a global flag and must come before 'tunnel'. --metrics
// is a tunnel flag and goes between 'tunnel' and 'run'. The token is passed
// with --token (more reliable than the TUNNEL_TOKEN env var).
func newCloudflaredCmd(token, metricsAddr string) *exec.Cmd {
	return exec.Command("cloudflared", "--no-autoupdate",
		"tunnel", "--metrics", metricsAddr,
		"run", "--token", token)
}

// usableMetricsAddr returns want if it can be bound right now, otherwise a
// free loopback port.
//
// This matters more than it looks: cloudflared treats a metrics bind failure
// as FATAL (verified against 2025.10.1: "Error opening metrics server
// listener ... address already in use", exit 1). An unlucky clash - another
// service, or an orphaned cloudflared from a previous agent outside a
// container - would therefore turn every spawn into an instant exit, and
// after CloudflaredMaxFailures the monitor gives up on the tunnel for good.
// Adding the watchdog must never be able to take ingress down, so we check
// first and fall back. The tiny window between our probe-close and
// cloudflared's bind is acceptable: losing it costs one counted failure.
func usableMetricsAddr(want string) string {
	l, err := net.Listen("tcp", want)
	if err == nil {
		l.Close()
		return want
	}
	l, ferr := net.Listen("tcp", "127.0.0.1:0")
	if ferr != nil {
		return want // nothing better available; let cloudflared report it
	}
	got := l.Addr().String()
	l.Close()
	logWarnf("Cloudflare tunnel: metrics address %s unavailable (%v), using %s for readiness instead", want, err, got)
	return got
}

// spawnCloudflaredLocked starts a cloudflared process and records it as the
// current one. Caller must hold cfMu.
func (a *Agent) spawnCloudflaredLocked(token string) error {
	addr := usableMetricsAddr(cloudflaredMetricsAddr())
	a.cfMetricsAddr = addr
	cmd := newCloudflaredCmd(token, addr)

	// Use filtered log writer to capture important messages while suppressing verbose output
	logFilter := &cloudflaredLogFilter{prefix: "cloudflared"}
	cmd.Stdout = logFilter
	cmd.Stderr = logFilter

	a.cfCmd = cmd
	if err := cmd.Start(); err != nil {
		a.cfAlive = false
		return err
	}
	a.cfAlive = true
	a.cfStartedAt = time.Now()
	a.cfKilledByWatchdog = false

	a.cfReadyMu.Lock()
	a.cfReady.Running = true
	a.cfReady.Ready = false
	a.cfReady.ReadyConnections = 0
	a.cfReady.LastError = ""
	a.cfReady.MetricsAddr = addr
	a.cfReadyMu.Unlock()
	return nil
}

// cloudflaredReadiness is the last thing the watchdog observed. Kept apart
// from cfMu (stopCloudflared holds that for up to 5s) and from stateMu, so the
// liveness endpoint can always read it.
type cloudflaredReadiness struct {
	Running          bool
	Ready            bool
	ReadyConnections int
	LastReady        time.Time // last probe that saw >=1 edge connection
	LastProbe        time.Time
	LastError        string
	WatchdogRestarts int
	MetricsAddr      string
}

// setCloudflaredNotRunning records that there is no live process (exited,
// stopped, or between restarts). LastReady is kept: "when were we last
// connected" is the most useful number during an outage.
func (a *Agent) setCloudflaredNotRunning() {
	a.cfReadyMu.Lock()
	a.cfReady.Running = false
	a.cfReady.Ready = false
	a.cfReady.ReadyConnections = 0
	a.cfReadyMu.Unlock()
}

// cloudflaredReadinessReport is the readiness block for /api/livez, the
// tunnel endpoint and cluster status. Timestamps are omitted (null) rather
// than reported as year 1 when we have never seen them.
func (a *Agent) cloudflaredReadinessReport() map[string]interface{} {
	a.cfReadyMu.Lock()
	r := a.cfReady
	a.cfReadyMu.Unlock()

	ts := func(t time.Time) interface{} {
		if t.IsZero() {
			return nil
		}
		return t.UTC().Format(time.RFC3339)
	}
	out := map[string]interface{}{
		"running":           r.Running,
		"ready":             r.Ready,
		"ready_connections": r.ReadyConnections,
		"last_ready":        ts(r.LastReady),
		"last_probe":        ts(r.LastProbe),
		"watchdog_restarts": r.WatchdogRestarts,
		"metrics_addr":      r.MetricsAddr,
	}
	if r.LastError != "" {
		out["last_error"] = r.LastError
	}
	// The state that used to be invisible: say it in words.
	if r.Running && !r.Ready && !r.LastProbe.IsZero() {
		out["diagnosis"] = "cloudflared process is up but has no edge connections"
	}
	return out
}

// cloudflaredProbe is one /ready observation.
type cloudflaredProbe struct {
	answered    bool // got any HTTP response at all - proves we can observe this process
	ready       bool
	connections int
	err         string
}

// probeCloudflaredReady asks cloudflared's metrics server whether it holds
// any edge connections. Bounded by the client's timeout.
func probeCloudflaredReady(client *http.Client, addr string) cloudflaredProbe {
	resp, err := client.Get("http://" + addr + "/ready")
	if err != nil {
		return cloudflaredProbe{err: err.Error()}
	}
	defer resp.Body.Close()

	var body struct {
		ReadyConnections int `json:"readyConnections"`
	}
	// Best-effort: the status code is the authoritative signal, the count is
	// for operators.
	_ = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&body)

	p := cloudflaredProbe{answered: true, connections: body.ReadyConnections}
	p.ready = resp.StatusCode == http.StatusOK
	if !p.ready {
		p.err = fmt.Sprintf("/ready returned HTTP %d (readyConnections=%d)", resp.StatusCode, body.ReadyConnections)
	}
	return p
}

// recordCloudflaredProbe publishes a probe result for the status endpoints.
func (a *Agent) recordCloudflaredProbe(p cloudflaredProbe, now time.Time) {
	a.cfReadyMu.Lock()
	defer a.cfReadyMu.Unlock()
	a.cfReady.LastProbe = now
	a.cfReady.Ready = p.ready
	a.cfReady.ReadyConnections = p.connections
	a.cfReady.LastError = p.err
	if p.ready {
		a.cfReady.LastReady = now
	}
}

// cloudflaredShouldRestart is the watchdog's decision, kept pure so it can be
// tested without a cloudflared binary.
//
// notReadySince is the later of the process's start and its last ready probe,
// so a fresh process gets a full threshold to connect and a single good probe
// restarts the clock. answered gates the whole thing: a process whose metrics
// endpoint has never responded is unobservable, not unhealthy.
func cloudflaredShouldRestart(now, notReadySince time.Time, answered bool, threshold time.Duration) bool {
	if threshold <= 0 || !answered {
		return false
	}
	return now.Sub(notReadySince) >= threshold
}

// cfWatchState is the watchdog's memory of the process it is judging. Reset
// whenever the monitor swaps in a new process.
type cfWatchState struct {
	cmd                *exec.Cmd
	notReadySince      time.Time
	answered           bool
	unobservableLogged bool
}

// cloudflaredReadyWatchdog polls readiness for the life of one monitor and
// kills a process that stays disconnected. stopCh is the session's cfStopCh
// (closed by a deliberate stop); done closes when the monitor returns.
func (a *Agent) cloudflaredReadyWatchdog(stopCh <-chan struct{}, done <-chan struct{}) {
	threshold := cloudflaredNotReadyRestartAfter()
	// Own client: loopback only, short timeout, and no keep-alive so a
	// killed process never leaves a dead pooled connection for the next one.
	client := &http.Client{
		Timeout:   CloudflaredReadyProbeTimeout,
		Transport: &http.Transport{DisableKeepAlives: true, Proxy: nil},
	}

	tick := time.NewTicker(CloudflaredReadyPollInterval)
	defer tick.Stop()

	var w cfWatchState
	for {
		select {
		case <-stopCh:
			return
		case <-done:
			return
		case <-tick.C:
		}

		a.cfMu.Lock()
		cmd, alive, started, addr := a.cfCmd, a.cfAlive, a.cfStartedAt, a.cfMetricsAddr
		a.cfMu.Unlock()
		if cmd == nil || !alive {
			continue // between restarts; the monitor owns this window
		}
		if cmd != w.cmd {
			w = cfWatchState{cmd: cmd, notReadySince: started}
		}

		// No locks held from here to the decision.
		now := time.Now()
		p := probeCloudflaredReady(client, addr)
		a.recordCloudflaredProbe(p, now)

		if p.answered {
			w.answered = true
		}
		if p.ready {
			w.notReadySince = now
			continue
		}

		if !w.answered && threshold > 0 && now.Sub(w.notReadySince) >= threshold && !w.unobservableLogged {
			w.unobservableLogged = true
			logWarnf("Cloudflare tunnel readiness is unobservable: metrics endpoint %s has not answered "+
				"for %v (%s). Not restarting a process we cannot see; check for a port clash "+
				"(set JETTY_CF_METRICS_ADDR) or an old cloudflared.", addr, now.Sub(w.notReadySince).Round(time.Second), p.err)
		}

		if !cloudflaredShouldRestart(now, w.notReadySince, w.answered, threshold) {
			continue
		}

		down := now.Sub(w.notReadySince).Round(time.Second)
		if a.killCloudflaredForWatchdog(cmd, stopCh) {
			logErrorf("Cloudflare tunnel watchdog: cloudflared (pid %d) has had no edge connections for %v "+
				"(last: %s) - killing it so the monitor restarts it", cmd.Process.Pid, down, p.err)
			a.cfReadyMu.Lock()
			a.cfReady.WatchdogRestarts++
			a.cfReadyMu.Unlock()
		}
		// Either way this process is done being judged; the next one starts
		// with a clean slate when the monitor swaps it in.
		w = cfWatchState{cmd: cmd, notReadySince: time.Now(), answered: false, unobservableLogged: true}
	}
}

// killCloudflaredForWatchdog kills cmd if, and only if, it is still the
// current live process and nobody has asked for the tunnel to stop. The
// re-check is under cfMu so it cannot race stopCloudflared or a respawn.
func (a *Agent) killCloudflaredForWatchdog(cmd *exec.Cmd, stopCh <-chan struct{}) bool {
	a.cfMu.Lock()
	defer a.cfMu.Unlock()

	select {
	case <-stopCh:
		return false // deliberate stop in progress - not ours to restart
	default:
	}
	if a.cfCmd != cmd || !a.cfAlive || cmd.Process == nil {
		return false
	}
	a.cfKilledByWatchdog = true
	if err := cmd.Process.Kill(); err != nil {
		a.cfKilledByWatchdog = false
		logWarnf("Cloudflare tunnel watchdog: kill failed: %v", err)
		return false
	}
	return true
}
