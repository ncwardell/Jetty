package agent

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/hashicorp/memberlist"
)

// JETTY_TUNNEL_HOST was read from the environment into a.tunnelHost and then
// never referenced again - dead config that looked wired up. Meanwhile
// getPeerAPIURL's fallback used a.tunnelDomain, the *cluster-wide* hostname,
// which Cloudflare resolves to whichever node it likes. So "send this to peer
// X" could land on peer Y, or on ourselves. These pin the wiring end to end.

func TestGetPeerAPIURLPrefersWarpIP(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.apiPort = 6880
	a.tunnelDomain = "cluster.example.com"

	peer := &Peer{ID: "p", IP: "100.96.0.2", TunnelHost: "node-b.example.com"}
	got := a.getPeerAPIURL(peer, "/api/health")
	want := "http://100.96.0.2:6880/api/health"
	if got != want {
		t.Errorf("got %q, want %q - WARP is direct and unambiguous, it must win", got, want)
	}
}

func TestGetPeerAPIURLUsesThePeersOwnHostWhenWarpIsDown(t *testing.T) {
	// The whole point. Without TunnelHost this returned the shared domain,
	// which addresses an arbitrary node rather than the one asked for.
	a := newTestAgentWithDir(t)
	a.tunnelDomain = "cluster.example.com"

	peer := &Peer{ID: "p", IP: "", TunnelHost: "node-b.example.com"}
	got := a.getPeerAPIURL(peer, "/api/peers/self/rotate-key")
	want := "https://node-b.example.com/api/peers/self/rotate-key"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if strings.Contains(got, "cluster.example.com") {
		t.Error("fell back to the cluster-wide domain despite knowing the peer's own host - " +
			"a peer-specific request would land on whichever node Cloudflare picked")
	}
}

func TestGetPeerAPIURLFallsBackToTheSharedDomain(t *testing.T) {
	// Peers that predate this field, or nodes with no JETTY_TUNNEL_HOST set,
	// must keep working exactly as before.
	a := newTestAgentWithDir(t)
	a.tunnelDomain = "cluster.example.com"

	got := a.getPeerAPIURL(&Peer{ID: "p"}, "/api/health")
	want := "https://cluster.example.com/api/health"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestGetPeerAPIURLReturnsEmptyWithNothingToGoOn(t *testing.T) {
	a := newTestAgentWithDir(t)
	if got := a.getPeerAPIURL(&Peer{ID: "p"}, "/api/health"); got != "" {
		t.Errorf("got %q, want empty", got)
	}
	if got := a.getPeerAPIURL(nil, "/api/health"); got != "" {
		t.Errorf("nil peer produced %q; callers index into peer maps that can miss", got)
	}
}

func TestNodeMetaCarriesOurTunnelHost(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.hwid = "us"
	a.hostname = "node-a"
	a.tunnelHost = "node-a.example.com"

	d := &jettyDelegate{agent: a}
	d.updateNodeMeta("100.96.0.1")

	d.metaMu.RLock()
	raw := d.meta
	d.metaMu.RUnlock()

	var meta NodeMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("meta is not JSON: %v", err)
	}
	if meta.TunnelHost != "node-a.example.com" {
		t.Errorf("TunnelHost = %q, want node-a.example.com - JETTY_TUNNEL_HOST is "+
			"read into the agent but never published, so no peer can address us specifically",
			meta.TunnelHost)
	}
}

func TestNotifyJoinIngestsTunnelHost(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.hwid = "us"
	e := &jettyEventDelegate{agent: a}

	meta, _ := json.Marshal(NodeMeta{
		ID: "peer-b", Name: "node-b", IP: "100.96.0.2",
		TunnelHost: "node-b.example.com",
	})
	e.NotifyJoin(&memberlist.Node{Name: "peer-b", Meta: meta})

	a.stateMu.RLock()
	peer := a.state.Peers["peer-b"]
	a.stateMu.RUnlock()
	if peer == nil {
		t.Fatal("peer was not added")
	}
	if peer.TunnelHost != "node-b.example.com" {
		t.Errorf("TunnelHost = %q; gossiped but not ingested, so getPeerAPIURL "+
			"still cannot address this peer", peer.TunnelHost)
	}
}

func TestNotifyUpdateIngestsTunnelHost(t *testing.T) {
	// A node that changes JETTY_TUNNEL_HOST and restarts arrives via
	// NotifyUpdate, not NotifyJoin. Missing it here means peers keep routing
	// to a hostname that no longer resolves to that node.
	a := newTestAgentWithDir(t)
	a.hwid = "us"
	a.stateMu.Lock()
	a.state.Peers["peer-b"] = &Peer{ID: "peer-b", Name: "node-b", TunnelHost: "old.example.com"}
	a.stateMu.Unlock()

	e := &jettyEventDelegate{agent: a}
	meta, _ := json.Marshal(NodeMeta{
		ID: "peer-b", Name: "node-b", IP: "100.96.0.2",
		TunnelHost: "new.example.com",
	})
	e.NotifyUpdate(&memberlist.Node{Name: "peer-b", Meta: meta})

	a.stateMu.RLock()
	got := a.state.Peers["peer-b"].TunnelHost
	a.stateMu.RUnlock()
	if got != "new.example.com" {
		t.Errorf("TunnelHost = %q, want new.example.com - a changed hostname was "+
			"not adopted, so we keep addressing the peer at a stale name", got)
	}
}

func TestPeerWireCarriesTunnelHost(t *testing.T) {
	// The /api/join response path. Without this the joiner learns every peer's
	// APIKey but not how to reach any of them individually.
	in := &Peer{ID: "p", Name: "node-b", TunnelHost: "node-b.example.com"}
	out := peerToWire(in).toPeer()
	if out.TunnelHost != "node-b.example.com" {
		t.Errorf("TunnelHost = %q; lost crossing the join wire", out.TunnelHost)
	}
}

// TestTunnelHostIsRejectedWhenItCouldRewriteTheURL is the one that matters for
// safety. TunnelHost is not display metadata - it becomes the authority of an
// https:// URL that we then send authenticated requests to. A peer supplying
// "evil.com/" or "evil.com#" or "x@evil.com" would redirect those requests,
// credentials included, to a host of its choosing.
func TestTunnelHostIsRejectedWhenItCouldRewriteTheURL(t *testing.T) {
	hostile := []string{
		"evil.com/api/x",     // path injection
		"attacker@evil.com",  // userinfo relocates the authority
		"evil.com:8080",      // port
		"evil.com#",          // fragment
		"evil.com?a=b",       // query
		"evil com",           // space
		"evil.com\nX-Api: y", // header/hosts-line injection
		"//evil.com",         // protocol-relative
		"-evil.com",          // leading dash
	}
	for _, h := range hostile {
		p := &Peer{ID: "p", Name: "node-b", TunnelHost: h}
		if !validIngestedPeer(p) {
			continue // rejecting the whole peer is also acceptable
		}
		if p.TunnelHost != "" {
			t.Errorf("tunnel host %q survived validation as %q - it would be "+
				"interpolated into an authenticated request URL", h, p.TunnelHost)
		}
	}
}

func TestValidTunnelHostsSurvive(t *testing.T) {
	for _, h := range []string{
		"node-a.example.com",
		"n1.cluster.internal",
		"node1",
		"a1-b2.c3.example.co.uk",
		"", // unset is normal
	} {
		p := &Peer{ID: "p", Name: "node-b", TunnelHost: h}
		if !validIngestedPeer(p) || p.TunnelHost != h {
			t.Errorf("legitimate tunnel host %q was blanked or rejected (got %q)", h, p.TunnelHost)
		}
	}
}

func TestNodeMetaShedsTunnelHostBeforeEssentialFields(t *testing.T) {
	// NodeMeta has a 512-byte cap and overflow used to truncate the JSON.
	// Adding a hostname pushes us closer to that cap, so TunnelHost has to be
	// in the shed list - but after Version and Name, because losing it costs
	// correctness rather than cosmetics.
	full, _ := json.Marshal(NodeMeta{
		ID: "peer-b", Name: "node-b", IP: "100.96.0.2",
		Version: "0.0.4", Arch: "arm64", APIPort: 6880,
		APIKey:     strings.Repeat("k", 200),
		TunnelHost: strings.Repeat("h", 120) + ".example.com",
	})
	d := &jettyDelegate{meta: full}

	// Large enough for everything: nothing is shed.
	if got := d.NodeMeta(len(full)); len(got) != len(full) {
		t.Errorf("meta was shed despite fitting")
	}

	// Tight enough that Version and Name must go, but TunnelHost still fits.
	var trimmed NodeMeta
	json.Unmarshal(full, &trimmed)
	trimmed.Version, trimmed.Name = "", ""
	withHost, _ := json.Marshal(trimmed)

	out := d.NodeMeta(len(withHost))
	if len(out) == 0 {
		t.Fatal("published nothing where shedding Version+Name was enough")
	}
	var got NodeMeta
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("shed meta is not parseable - the truncation bug is back: %v", err)
	}
	if got.TunnelHost == "" {
		t.Error("TunnelHost was shed before Version and Name; it is the more " +
			"valuable of the three")
	}
	if got.ID == "" || got.APIKey == "" || got.APIPort == 0 {
		t.Error("an essential field was shed")
	}

	// Tighter still: TunnelHost goes, essentials survive.
	trimmed.TunnelHost = ""
	minimal, _ := json.Marshal(trimmed)
	out = d.NodeMeta(len(minimal))
	// A fresh var: the fields are omitempty, so unmarshalling into the
	// previous `got` would leave stale values in place of absent ones.
	var got2 NodeMeta
	if err := json.Unmarshal(out, &got2); err != nil {
		t.Fatalf("shed meta is not parseable: %v", err)
	}
	got = got2
	if got.TunnelHost != "" {
		t.Error("TunnelHost was not shed even though the payload did not fit with it")
	}
	if got.ID == "" || got.APIKey == "" {
		t.Error("an essential field was shed before TunnelHost")
	}
}
