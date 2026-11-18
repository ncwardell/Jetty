package agent

import (
	"testing"
	"time"
)

// 2026-11-18: vaultwarden was failed over from a wedged-but-serving node to a
// peer after the 30s grace, restored from a six-day-old backup, and both
// copies took writes. These pin the guard that makes stateful failover slow
// and explicit, and the merge behaviour that no longer deletes the copy that
// may hold the newest data.

func boolp(b bool) *bool { return &b }

func TestComposeIsStateful(t *testing.T) {
	cases := []struct {
		name    string
		compose string
		want    bool
	}{
		{"no volumes", "services:\n  web:\n    image: nginx\n", false},
		{"top-level named volume", "services:\n  db:\n    image: pg\n    volumes:\n      - data:/var/lib/pg\nvolumes:\n  data: {}\n", true},
		{"vaultwarden-like", "services:\n  vw:\n    image: vaultwarden/server\n    volumes:\n      - vw-data:/data\n  backup:\n    image: rclone\n    volumes:\n      - vw-data:/data:ro\nvolumes:\n  vw-data:\n", true},
		{"writable bind", "services:\n  app:\n    image: x\n    volumes:\n      - ./data:/data\n", true},
		{"env-var bind", "services:\n  app:\n    image: x\n    volumes:\n      - ${DATA}:/data\n", true},
		{"anonymous volume", "services:\n  app:\n    image: x\n    volumes:\n      - /data\n", true},
		{"read-only bind", "services:\n  app:\n    image: x\n    volumes:\n      - /etc/localtime:/etc/localtime:ro\n", false},
		{"ro among options", "services:\n  app:\n    image: x\n    volumes:\n      - ./conf:/conf:ro,z\n", false},
		{"docker socket", "services:\n  app:\n    image: x\n    volumes:\n      - /var/run/docker.sock:/var/run/docker.sock\n", false},
		{"long syntax volume", "services:\n  app:\n    image: x\n    volumes:\n      - type: volume\n        source: d\n        target: /d\n", true},
		{"long syntax read-only bind", "services:\n  app:\n    image: x\n    volumes:\n      - type: bind\n        source: ./c\n        target: /c\n        read_only: true\n", false},
		{"long syntax tmpfs", "services:\n  app:\n    image: x\n    volumes:\n      - type: tmpfs\n        target: /tmp\n", false},
		// When in doubt, pick the behaviour that cannot lose data.
		{"unparseable", "services: [::\n", true},
	}
	for _, c := range cases {
		if got := composeIsStateful(c.compose); got != c.want {
			t.Errorf("%s: composeIsStateful = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestWorkloadIsStatefulOverridesAndArchVariants(t *testing.T) {
	stateless := "services:\n  web:\n    image: nginx\n"
	stateful := "services:\n  db:\n    image: pg\n    volumes:\n      - d:/d\nvolumes:\n  d:\n"

	if workloadIsStateful(&Workload{Compose: stateless}) {
		t.Error("stateless compose detected as stateful")
	}
	// Every node must reach the same answer, so a stateful variant for ANY
	// arch marks the workload - not just the one this node would run.
	if !workloadIsStateful(&Workload{Compose: stateless, ComposeArm64: stateful}) {
		t.Error("stateful arm64 variant ignored")
	}
	if workloadIsStateful(&Workload{Compose: stateful, Stateful: boolp(false)}) {
		t.Error("explicit stateful=false not honoured")
	}
	if !workloadIsStateful(&Workload{Compose: stateless, Stateful: boolp(true)}) {
		t.Error("explicit stateful=true not honoured")
	}
}

func TestFailoverPolicyFor(t *testing.T) {
	stateless := &Workload{Compose: "services:\n  web:\n    image: nginx\n"}
	stateful := &Workload{Compose: "services:\n  db:\n    image: pg\n    volumes:\n      - d:/d\nvolumes:\n  d:\n"}

	t.Run("stateless keeps the short grace", func(t *testing.T) {
		p := failoverPolicyFor(stateless)
		if !p.auto || p.grace != FailoverGracePeriod || p.stateful {
			t.Fatalf("policy = %+v", p)
		}
	})
	t.Run("stateful defaults to the long grace", func(t *testing.T) {
		p := failoverPolicyFor(stateful)
		if !p.auto || p.grace != StatefulFailoverGraceDefault || !p.stateful {
			t.Fatalf("policy = %+v", p)
		}
		if p.grace <= FailoverGracePeriod {
			t.Fatal("stateful grace must be much longer than the stateless one")
		}
	})
	t.Run("grace is configurable", func(t *testing.T) {
		t.Setenv("JETTY_STATEFUL_FAILOVER_GRACE", "15m")
		if p := failoverPolicyFor(stateful); p.grace != 15*time.Minute {
			t.Fatalf("grace = %v, want 15m", p.grace)
		}
	})
	t.Run("grace 0 means never automatically", func(t *testing.T) {
		t.Setenv("JETTY_STATEFUL_FAILOVER_GRACE", "0")
		if p := failoverPolicyFor(stateful); p.auto {
			t.Fatalf("policy = %+v, want auto=false", p)
		}
		if p := failoverPolicyFor(stateless); !p.auto {
			t.Fatal("the stateful knob must not affect stateless workloads")
		}
	})
	t.Run("auto_failover=false opts out", func(t *testing.T) {
		w := *stateless
		w.AutoFailover = boolp(false)
		if p := failoverPolicyFor(&w); p.auto {
			t.Fatalf("policy = %+v, want auto=false", p)
		}
	})
	t.Run("auto_failover=true opts a stateful workload into fast failover", func(t *testing.T) {
		w := *stateful
		w.AutoFailover = boolp(true)
		if p := failoverPolicyFor(&w); !p.auto || p.grace != FailoverGracePeriod {
			t.Fatalf("policy = %+v, want the short grace", p)
		}
	})
}

func TestCheckFailoverHoldsStatefulWorkloadPastTheShortGrace(t *testing.T) {
	a := newTestAgent("s")
	a.hwid = "us"
	a.startTime = time.Now().Add(-time.Hour) // past FailoverStartupGrace
	a.failoverInProgress = map[string]time.Time{}
	a.peerUnhealthySince = map[string]time.Time{}
	a.maroonedLogged = map[string]time.Time{}

	// Owner looks dead (no memberlist; stored peer unhealthy and stale) and
	// has for 10 minutes - well past the 30s stateless grace, nowhere near
	// the stateful one. This is the incident timeline.
	a.state.Peers["radxa"] = &Peer{ID: "radxa", Name: "radxa", Healthy: false,
		LastSeen: time.Now().Add(-10 * time.Minute), Arch: "amd64"}
	a.peerUnhealthySince["radxa"] = time.Now().Add(-10 * time.Minute)
	vw := &Workload{Name: "vaultwarden", IP: "10.100.0.7", Owner: "radxa", Revive: true, Version: 1,
		Compose: "services:\n  vw:\n    image: vaultwarden/server\n    volumes:\n      - vw:/data\nvolumes:\n  vw:\n"}
	a.state.Workloads[vw.IP] = vw

	a.checkFailover()

	if vw.Owner != "radxa" || vw.Version != 1 {
		t.Fatalf("stateful workload claimed after 10m (owner=%s) - must wait for JETTY_STATEFUL_FAILOVER_GRACE", vw.Owner)
	}
	if _, claiming := a.failoverInProgress[vw.IP]; claiming {
		t.Fatal("failover started for a stateful workload inside its grace")
	}
	// The no-claim must come from the stateful guard, not from a later gate
	// (no internet witness in CI, marooned, election) that happens to also
	// say no - otherwise this test would pass without the guard.
	if _, held := a.maroonedLogged["failover-held:"+vw.IP]; !held {
		t.Fatal("stateful guard did not fire; the workload was only spared by a later check")
	}
}

func TestPreserveLostWorkload(t *testing.T) {
	stateful := &Workload{Name: "vaultwarden", IP: "10.100.0.7",
		Compose: "services:\n  vw:\n    image: v\n    volumes:\n      - vw:/data\nvolumes:\n  vw:\n"}
	stateless := &Workload{Name: "web", IP: "10.100.0.8", Compose: "services:\n  web:\n    image: nginx\n"}

	// Ownership moved while we were running it (failover claim landing on a
	// node that was wedged or partitioned): keep the volumes, they may hold
	// the newest writes. This used to be `down -v`.
	claimed := *stateful
	claimed.Owner = "hetzner"
	if !preserveLostWorkload(stateful, &claimed) {
		t.Error("stateful workload that changed owner must be stopped with volumes preserved, not removed")
	}
	// Deleted (a tombstone won): remove as before - that is what the
	// operator asked for.
	if preserveLostWorkload(stateful, nil) {
		t.Error("deleted workload must still be removed")
	}
	// IP reused by a different workload: ours is gone.
	other := *stateful
	other.Name = "something-else"
	if preserveLostWorkload(stateful, &other) {
		t.Error("workload replaced at the same IP must still be removed")
	}
	// Stateless: removing is safe and keeps the old behaviour.
	movedWeb := *stateless
	movedWeb.Owner = "hetzner"
	if preserveLostWorkload(stateless, &movedWeb) {
		t.Error("stateless workload should be removed as before")
	}
}
