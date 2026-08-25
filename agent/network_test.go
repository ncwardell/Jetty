package agent

import (
	"testing"
)

func TestDeriveMeshIPDeterministic(t *testing.T) {
	a := newTestAgent("s")
	a.serviceCIDR = "10.100.0.0/16"

	// Same ID must hash to same IP across calls.
	id := "abcdef1234567890"
	ip1 := a.deriveMeshIP(id)
	ip2 := a.deriveMeshIP(id)
	if ip1 != ip2 {
		t.Fatalf("non-deterministic: %s vs %s", ip1, ip2)
	}
}

func TestDeriveMeshIPInsideCIDR(t *testing.T) {
	a := newTestAgent("s")
	a.serviceCIDR = "10.100.0.0/16"

	for _, id := range []string{"a", "longer-id-here", "0xdeadbeef", ""} {
		ip := a.deriveMeshIP(id)
		if !a.isIPInCIDR(ip) {
			t.Errorf("derived IP %q for id %q is outside %s", ip, id, a.serviceCIDR)
		}
	}
}

func TestIsNodeAllowedArchGate(t *testing.T) {
	a := newTestAgent("s")

	// Default compose: works on any arch.
	wl := &Workload{Name: "x", Compose: "services:\n  a: { image: nginx }"}
	if !a.isNodeAllowed(wl, "id1", "node1", "amd64") {
		t.Error("default compose should run on amd64")
	}
	if !a.isNodeAllowed(wl, "id1", "node1", "arm64") {
		t.Error("default compose should run on arm64")
	}

	// Only arm64 compose: not allowed on amd64.
	wl = &Workload{Name: "x", ComposeArm64: "services: ..."}
	if a.isNodeAllowed(wl, "id1", "node1", "amd64") {
		t.Error("arm64-only workload must NOT run on amd64")
	}
	if !a.isNodeAllowed(wl, "id1", "node1", "arm64") {
		t.Error("arm64-only workload should run on arm64")
	}
}

func TestIsNodeAllowedAllowedNodes(t *testing.T) {
	a := newTestAgent("s")
	wl := &Workload{
		Name:         "x",
		Compose:      "services: ...",
		AllowedNodes: []string{"node-a", "id-special"},
	}

	if !a.isNodeAllowed(wl, "id-special", "whatever", "amd64") {
		t.Error("matching ID should be allowed")
	}
	if !a.isNodeAllowed(wl, "any-id", "node-a", "amd64") {
		t.Error("matching name should be allowed")
	}
	if a.isNodeAllowed(wl, "stranger-id", "stranger-name", "amd64") {
		t.Error("non-matching node should not be allowed")
	}

	// Wildcard
	wl.AllowedNodes = []string{"*"}
	if !a.isNodeAllowed(wl, "any-id", "any-name", "amd64") {
		t.Error("wildcard should allow all nodes")
	}
}

func TestAllocateServiceIPSkipsTaken(t *testing.T) {
	a := newTestAgent("s")
	a.serviceCIDR = "10.100.0.0/30" // only .1 and .2 are usable
	a.setWarpIP("10.100.0.1")       // we own .1
	a.state.Peers["p1"] = &Peer{ID: "p1", IP: "10.100.0.2"}

	got := a.allocateServiceIP()
	if got != "" {
		t.Errorf("expected empty (CIDR exhausted), got %q", got)
	}

	// Free up .2
	delete(a.state.Peers, "p1")
	got = a.allocateServiceIP()
	if got != "10.100.0.2" {
		t.Errorf("expected 10.100.0.2, got %q", got)
	}
}
