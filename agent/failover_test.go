package agent

import (
	"testing"
)

// shouldClaim's contract: us + every healthy/allowed peer competes,
// ranked by current owned-workload count (asc) with HWID as tiebreak.
// We test the ranking via the HTTP-gossip path (memberlist nil) since
// that branch reads directly from a.state.Peers.

func TestShouldClaimPicksLeastLoaded(t *testing.T) {
	a := newTestAgent("s")
	a.hwid = "us"
	a.state.Peers["a"] = &Peer{ID: "a", Name: "a", Healthy: true, Arch: "amd64"}
	a.state.Peers["b"] = &Peer{ID: "b", Name: "b", Healthy: true, Arch: "amd64"}

	// Pre-load: peer "a" has 3 workloads, peer "b" has 1, we have 0.
	a.state.Workloads["10.100.0.1"] = &Workload{Name: "x1", IP: "10.100.0.1", Owner: "a", Compose: "x"}
	a.state.Workloads["10.100.0.2"] = &Workload{Name: "x2", IP: "10.100.0.2", Owner: "a", Compose: "x"}
	a.state.Workloads["10.100.0.3"] = &Workload{Name: "x3", IP: "10.100.0.3", Owner: "a", Compose: "x"}
	a.state.Workloads["10.100.0.4"] = &Workload{Name: "y1", IP: "10.100.0.4", Owner: "b", Compose: "x"}

	orphan := &Workload{Name: "victim", IP: "10.100.0.99", Owner: "dead", Compose: "x"}
	if !a.shouldClaim(orphan) {
		t.Errorf("we have 0 owned, should win election")
	}

	// Now make us busier than b.
	a.state.Workloads["10.100.0.10"] = &Workload{Name: "us1", IP: "10.100.0.10", Owner: "us", Compose: "x"}
	a.state.Workloads["10.100.0.11"] = &Workload{Name: "us2", IP: "10.100.0.11", Owner: "us", Compose: "x"}
	if a.shouldClaim(orphan) {
		t.Errorf("we have 2 owned, b has 1 - b should win not us")
	}
}

func TestShouldClaimTieBreaksByHWID(t *testing.T) {
	a := newTestAgent("s")
	a.hwid = "abc"
	a.state.Peers["xyz"] = &Peer{ID: "xyz", Name: "xyz", Healthy: true, Arch: "amd64"}
	// Both have 0 workloads; tie. HWID "abc" < "xyz" so we win.

	orphan := &Workload{Name: "v", IP: "10.100.0.99", Owner: "dead", Compose: "x"}
	if !a.shouldClaim(orphan) {
		t.Errorf("tie at zero load, our HWID is lower, we should win")
	}

	// Flip the tiebreak by making us higher.
	a.hwid = "zzz"
	if a.shouldClaim(orphan) {
		t.Errorf("our HWID is higher than xyz, xyz should win the tie")
	}
}

func TestShouldClaimIgnoresOrphanItselfInCount(t *testing.T) {
	// If we're computing load and the orphan is *currently* still listed
	// as owned by its dead previous owner, that count shouldn't bias
	// against the dead node (it's dead) or get double-counted.
	a := newTestAgent("s")
	a.hwid = "us"
	a.state.Peers["b"] = &Peer{ID: "b", Name: "b", Healthy: true, Arch: "amd64"}
	// Orphan's previous owner is unhealthy/dead and not in candidates anyway.
	orphan := &Workload{Name: "v", IP: "10.100.0.99", Owner: "dead", Compose: "x"}
	a.state.Workloads[orphan.IP] = orphan

	// We and b both have 0 workloads (excluding the orphan).
	// HWID tiebreak: "b" < "us" lexicographically, so b wins.
	if a.shouldClaim(orphan) {
		t.Errorf("b's HWID is lower; b should win (us=%s)", a.hwid)
	}
}

func TestShouldClaimRespectsArchGating(t *testing.T) {
	a := newTestAgent("s")
	a.hwid = "us"
	// Only peer is amd64 but workload is arm64-only.
	a.state.Peers["a"] = &Peer{ID: "a", Name: "a", Healthy: true, Arch: "amd64"}
	orphan := &Workload{Name: "v", IP: "10.100.0.99", ComposeArm64: "x"}
	// We default to runtime arch (likely amd64). On amd64 we can't run
	// arm64-only workloads, so we shouldn't claim.
	if a.shouldClaim(orphan) {
		t.Errorf("workload is arm64-only; on amd64 we should not claim")
	}
}
