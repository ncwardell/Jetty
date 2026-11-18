package agent

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// On 2026-11-18 a cloudflared connector was found alive with zero edge
// connections after 19 days. The exit-based monitor could not see it. These
// pin the pieces of the readiness watchdog that can be tested without a real
// cloudflared binary.

func TestNewCloudflaredCmdEnablesMetrics(t *testing.T) {
	cmd := newCloudflaredCmd("tok", "127.0.0.1:46880")
	got := strings.Join(cmd.Args[1:], " ")
	// --no-autoupdate is global (before 'tunnel'); --metrics is a tunnel
	// flag (between 'tunnel' and 'run'). Getting the order wrong makes
	// cloudflared reject the command line or silently ignore the flag.
	want := "--no-autoupdate tunnel --metrics 127.0.0.1:46880 run --token tok"
	if got != want {
		t.Fatalf("args = %q, want %q", got, want)
	}
}

func TestUsableMetricsAddrFallsBackOnAClash(t *testing.T) {
	// cloudflared exits immediately when it cannot bind --metrics, and the
	// monitor gives up after CloudflaredMaxFailures. A busy port must
	// therefore never reach cloudflared.
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot listen on loopback: %v", err)
	}
	defer busy.Close()

	got := usableMetricsAddr(busy.Addr().String())
	if got == busy.Addr().String() {
		t.Fatal("returned an address that is already bound")
	}
	l, err := net.Listen("tcp", got)
	if err != nil {
		t.Fatalf("fallback address %s is not bindable: %v", got, err)
	}
	l.Close()

	// A free configured address is used as-is.
	free := got
	if again := usableMetricsAddr(free); again != free {
		t.Errorf("free address %s replaced with %s", free, again)
	}
}

func TestProbeCloudflaredReady(t *testing.T) {
	client := &http.Client{Timeout: time.Second}

	t.Run("connected", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/ready" {
				t.Errorf("probed %s, want /ready", r.URL.Path)
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":200,"readyConnections":4,"connectorId":"x"}`))
		}))
		defer srv.Close()
		p := probeCloudflaredReady(client, strings.TrimPrefix(srv.URL, "http://"))
		if !p.answered || !p.ready || p.connections != 4 {
			t.Fatalf("probe = %+v, want answered ready with 4 connections", p)
		}
	})

	t.Run("up but disconnected", func(t *testing.T) {
		// The incident state: the process answers, it just has nothing.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":503,"readyConnections":0}`))
		}))
		defer srv.Close()
		p := probeCloudflaredReady(client, strings.TrimPrefix(srv.URL, "http://"))
		if !p.answered || p.ready {
			t.Fatalf("probe = %+v, want answered and not ready", p)
		}
		if p.err == "" {
			t.Error("a not-ready probe should carry a reason for operators")
		}
	})

	t.Run("nothing listening", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		addr := strings.TrimPrefix(srv.URL, "http://")
		srv.Close()
		p := probeCloudflaredReady(client, addr)
		if p.answered || p.ready {
			t.Fatalf("probe = %+v, want unanswered", p)
		}
	})
}

func TestCloudflaredShouldRestart(t *testing.T) {
	now := time.Now()
	threshold := 3 * time.Minute
	cases := []struct {
		name          string
		notReadySince time.Time
		answered      bool
		threshold     time.Duration
		want          bool
	}{
		{"within threshold", now.Add(-time.Minute), true, threshold, false},
		{"past threshold", now.Add(-4 * time.Minute), true, threshold, true},
		{"exactly at threshold", now.Add(-threshold), true, threshold, true},
		// A process we have never heard from could be a metrics port clash
		// on a perfectly healthy tunnel. Restarting it every 3 minutes would
		// be worse than the bug.
		{"never answered", now.Add(-time.Hour), false, threshold, false},
		{"disabled", now.Add(-time.Hour), true, 0, false},
	}
	for _, c := range cases {
		if got := cloudflaredShouldRestart(now, c.notReadySince, c.answered, c.threshold); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func startSleeper(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start a helper process: %v", err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	return cmd
}

func TestWatchdogKillNeverFightsADeliberateStop(t *testing.T) {
	a := newTestAgentWithDir(t)
	cmd := startSleeper(t)
	a.cfCmd = cmd
	a.cfAlive = true

	stopped := make(chan struct{})
	close(stopped)
	if a.killCloudflaredForWatchdog(cmd, stopped) {
		t.Fatal("watchdog killed cloudflared while a deliberate stop was in progress")
	}
	if a.cfKilledByWatchdog {
		t.Fatal("kill flag set although nothing was killed")
	}
}

func TestWatchdogKillOnlyTargetsTheCurrentLiveProcess(t *testing.T) {
	a := newTestAgentWithDir(t)
	old := startSleeper(t)
	current := startSleeper(t)
	a.cfCmd = current
	a.cfAlive = true
	running := make(chan struct{})

	// A stale observation (the monitor already swapped in a new process)
	// must not kill the new one.
	if a.killCloudflaredForWatchdog(old, running) {
		t.Fatal("watchdog killed a process that is no longer the current one")
	}

	// Between restarts cfCmd still points at the exited process.
	a.cfAlive = false
	if a.killCloudflaredForWatchdog(current, running) {
		t.Fatal("watchdog acted on a process the monitor already reaped")
	}

	a.cfAlive = true
	if !a.killCloudflaredForWatchdog(current, running) {
		t.Fatal("watchdog did not kill the current, live, not-ready process")
	}
	if !a.cfKilledByWatchdog {
		t.Fatal("monitor would misread the watchdog kill as a crash")
	}
	done := make(chan error, 1)
	go func() { done <- current.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("process still running after watchdog kill")
	}
}

func TestCloudflaredReadinessReportSaysUpButNotConnected(t *testing.T) {
	a := newTestAgentWithDir(t)

	if r := a.cloudflaredReadinessReport(); r["running"] != false || r["last_ready"] != nil {
		t.Fatalf("fresh agent report = %v", r)
	}

	a.cfReady.Running = true
	now := time.Now()
	a.recordCloudflaredProbe(cloudflaredProbe{answered: true, ready: true, connections: 4}, now)
	r := a.cloudflaredReadinessReport()
	if r["ready"] != true || r["ready_connections"] != 4 || r["last_ready"] == nil {
		t.Fatalf("connected report = %v", r)
	}
	if _, ok := r["diagnosis"]; ok {
		t.Errorf("connected tunnel should not carry a diagnosis: %v", r["diagnosis"])
	}

	a.recordCloudflaredProbe(cloudflaredProbe{answered: true, err: "503"}, now.Add(time.Minute))
	r = a.cloudflaredReadinessReport()
	if r["ready"] != false || r["diagnosis"] == nil {
		t.Fatalf("disconnected report = %v, want ready=false with a diagnosis", r)
	}
	if r["last_ready"] == nil {
		t.Error("last_ready must survive a not-ready probe - it is the outage start time")
	}
}

func TestGetEnvDuration(t *testing.T) {
	const key = "JETTY_TEST_DURATION"
	def := 3 * time.Minute
	cases := map[string]time.Duration{
		"":     def,
		"90s":  90 * time.Second,
		"2m":   2 * time.Minute,
		"120":  120 * time.Second, // bare integer = seconds
		"0":    0,                 // 0 is meaningful (disabled), not "unset"
		"-5":   def,
		"junk": def,
	}
	for in, want := range cases {
		t.Setenv(key, in)
		if got := getEnvDuration(key, def); got != want {
			t.Errorf("getEnvDuration(%q) = %v, want %v", in, got, want)
		}
	}
}
