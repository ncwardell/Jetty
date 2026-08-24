package agent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// The incident these tests pin: DELETE /api/tunnel on one node broadcast an
// empty token to every peer, tearing down every cloudflared connector in the
// cluster and taking all public sites down with Cloudflare 530/1033. A
// per-node operation must have per-node blast radius.

func newTunnelTestAgent(t *testing.T) *Agent {
	t.Helper()
	a := &Agent{
		state:    NewState(),
		stateMu:  sync.RWMutex{},
		dataDir:  t.TempDir(),
		hostname: "node-a",
		hwid:     "hwid-a",
	}
	a.state.CFToken = "cluster-token"
	a.state.SelfAPIKey = "self-key"
	return a
}

// peerSpy stands in for a peer node and records tunnel-sync broadcasts.
type peerSpy struct {
	mu     sync.Mutex
	tokens []string
	srv    *httptest.Server
}

func newPeerSpy(t *testing.T, a *Agent) *peerSpy {
	t.Helper()
	spy := &peerSpy{}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/tunnel/sync", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Token string `json:"token"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		spy.mu.Lock()
		spy.tokens = append(spy.tokens, body.Token)
		spy.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	spy.srv = httptest.NewServer(mux)
	t.Cleanup(spy.srv.Close)

	// Point the agent's peer table at the spy: getPeerAPIURL builds
	// http://<peer.IP>:<apiPort>, so split the test server's address.
	u, err := url.Parse(spy.srv.URL)
	if err != nil {
		t.Fatalf("parse spy URL: %v", err)
	}
	port, _ := strconv.Atoi(u.Port())
	a.apiPort = port
	a.state.Peers["hwid-b"] = &Peer{
		ID:      "hwid-b",
		Name:    "node-b",
		IP:      u.Hostname(),
		Healthy: true,
		APIKey:  "peer-key",
	}
	return spy
}

func (s *peerSpy) received() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.tokens...)
}

func doTunnel(a *Agent, method, target, body string) *httptest.ResponseRecorder {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, target, http.NoBody)
	} else {
		r = httptest.NewRequest(method, target, strings.NewReader(body))
	}
	w := httptest.NewRecorder()
	switch method {
	case http.MethodGet:
		a.apiGetTunnel(w, r)
	case http.MethodPost:
		a.apiSetTunnel(w, r)
	case http.MethodDelete:
		a.apiDeleteTunnel(w, r)
	}
	return w
}

func TestDeleteTunnelDefaultScopeDoesNotTouchTheCluster(t *testing.T) {
	a := newTunnelTestAgent(t)
	spy := newPeerSpy(t, a)

	w := doTunnel(a, http.MethodDelete, "/api/tunnel", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	a.stateMu.RLock()
	token, disabled := a.state.CFToken, a.state.CFTunnelDisabled
	a.stateMu.RUnlock()

	if token != "cluster-token" {
		t.Errorf("cluster token = %q, want it preserved", token)
	}
	if !disabled {
		t.Error("node was not marked disabled")
	}
	if got := spy.received(); len(got) != 0 {
		t.Errorf("peer was contacted %d time(s) for a node-scoped delete: %v", len(got), got)
	}
}

func TestDeleteTunnelClusterScopeStillTearsDownEverywhere(t *testing.T) {
	// The destructive behaviour remains available - you just have to ask.
	a := newTunnelTestAgent(t)
	spy := newPeerSpy(t, a)

	w := doTunnel(a, http.MethodDelete, "/api/tunnel?scope=cluster", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	a.stateMu.RLock()
	token := a.state.CFToken
	a.stateMu.RUnlock()
	if token != "" {
		t.Errorf("cluster token = %q, want cleared", token)
	}

	got := spy.received()
	if len(got) != 1 || got[0] != "" {
		t.Errorf("peer broadcasts = %v, want exactly one empty-token teardown", got)
	}
}

func TestDeleteTunnelRejectsUnknownScope(t *testing.T) {
	// A typo must not fall through to the more destructive branch.
	a := newTunnelTestAgent(t)
	spy := newPeerSpy(t, a)

	w := doTunnel(a, http.MethodDelete, "/api/tunnel?scope=clustre", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}

	a.stateMu.RLock()
	token, disabled := a.state.CFToken, a.state.CFTunnelDisabled
	a.stateMu.RUnlock()

	if token != "cluster-token" {
		t.Error("cluster token was modified by a rejected request")
	}
	if disabled {
		t.Error("node was disabled by a rejected request")
	}
	if got := spy.received(); len(got) != 0 {
		t.Errorf("rejected request still broadcast: %v", got)
	}
}

func TestSetTunnelClusterScopeRequiresToken(t *testing.T) {
	a := newTunnelTestAgent(t)
	spy := newPeerSpy(t, a)

	w := doTunnel(a, http.MethodPost, "/api/tunnel?scope=cluster", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if got := spy.received(); len(got) != 0 {
		t.Errorf("rejected request still broadcast: %v", got)
	}
}

func TestSetTunnelNodeScopeReEnablesWithoutBroadcast(t *testing.T) {
	a := newTunnelTestAgent(t)
	spy := newPeerSpy(t, a)

	doTunnel(a, http.MethodDelete, "/api/tunnel", "")
	a.stateMu.RLock()
	disabled := a.state.CFTunnelDisabled
	a.stateMu.RUnlock()
	if !disabled {
		t.Fatal("precondition: node should be disabled")
	}

	// Status is not asserted: restartCloudflared shells out to the
	// cloudflared binary, which is absent in test environments. The state
	// transition and the absence of a broadcast are the contract here.
	doTunnel(a, http.MethodPost, "/api/tunnel", "")

	a.stateMu.RLock()
	disabled, token := a.state.CFTunnelDisabled, a.state.CFToken
	a.stateMu.RUnlock()

	if disabled {
		t.Error("node was not re-enabled")
	}
	if token != "cluster-token" {
		t.Errorf("cluster token = %q, want it untouched by a node-scoped enable", token)
	}
	if got := spy.received(); len(got) != 0 {
		t.Errorf("node-scoped enable broadcast to peers: %v", got)
	}
}

func TestSetTunnelNodeScopeFailsWithNoClusterToken(t *testing.T) {
	a := newTunnelTestAgent(t)
	a.state.CFToken = ""

	w := doTunnel(a, http.MethodPost, "/api/tunnel", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 when there is no token to re-attach to", w.Code)
	}
}

func TestGetTunnelReportsNodeLocalEnablement(t *testing.T) {
	a := newTunnelTestAgent(t)

	w := doTunnel(a, http.MethodGet, "/api/tunnel", "")
	var before map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &before)
	if before["configured"] != true || before["enabled"] != true {
		t.Fatalf("initial status = %v, want configured+enabled", before)
	}

	doTunnel(a, http.MethodDelete, "/api/tunnel", "")

	w = doTunnel(a, http.MethodGet, "/api/tunnel", "")
	var after map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &after)
	if after["configured"] != true {
		t.Error("configured should stay true - the cluster token is still set")
	}
	if after["enabled"] != false {
		t.Error("enabled should be false after a node-scoped delete")
	}
}

func TestStartCloudflaredHonoursNodeDisable(t *testing.T) {
	// The guard lives in startCloudflared rather than the handlers so that
	// the monitor's restart loop and token syncs cannot resurrect the
	// connector on a node that opted out.
	a := newTunnelTestAgent(t)
	a.state.CFTunnelDisabled = true

	if err := a.startCloudflared(); err != nil {
		t.Fatalf("startCloudflared on a disabled node = %v, want nil (no-op)", err)
	}
	if a.isTunnelRunning() {
		t.Error("cloudflared started on a disabled node")
	}
}
