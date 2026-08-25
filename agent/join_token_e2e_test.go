package agent

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// e2eAgent returns an agent with a fully bootstrapped state that can
// serve apiCreateToken / apiJoin without touching the network or fs.
func e2eAgent(t *testing.T) *Agent {
	t.Helper()
	a := &Agent{
		hwid:     "host-aaaaaaaaaaaa",
		hostname: "host-self",
		// WARP IP set via setWarpIP below
		apiPort:     6880,
		serviceCIDR: "10.100.0.0/16",
		state:       NewState(),
		stateMu:     sync.RWMutex{},
		dataDir:     t.TempDir(),
		composeDir:  t.TempDir(),
		hostsFile:   t.TempDir() + "/hosts",
	}
	a.setWarpIP("100.96.0.1")
	a.state.AdminKey = "admin-secret"
	a.state.SelfAPIKey = "self-secret"
	if err := a.ensureEncryptionKey(); err != nil {
		t.Fatal(err)
	}
	return a
}

// mountRoutes wires the routes used by these tests into a fresh router.
// Mirrors api.go but only what we need - no middleware (handlers do
// their own admin/middleware checks).
func mountRoutes(a *Agent) http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/api/tokens", a.apiCreateToken).Methods("POST")
	r.HandleFunc("/api/tokens", a.apiListTokens).Methods("GET")
	r.HandleFunc("/api/tokens/{id}", a.apiDeleteToken).Methods("DELETE")
	r.HandleFunc("/api/join", a.apiJoin).Methods("POST")
	return r
}

func doJSON(t *testing.T, h http.Handler, method, path string, body any, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	var buf io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, buf)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	rec.Result().Body = io.NopCloser(rec.Body)
	respBytes, _ := io.ReadAll(rec.Body)
	return rec.Result(), respBytes
}

// TestE2E_TokenIssueListRevoke exercises the admin-side token CRUD
// endpoints over HTTP.
func TestE2E_TokenIssueListRevoke(t *testing.T) {
	a := e2eAgent(t)
	h := mountRoutes(a)

	adminHeader := map[string]string{"X-API-Key": "admin-secret"}

	// Create
	resp, body := doJSON(t, h, "POST", "/api/tokens",
		map[string]any{"ttl_seconds": 60, "note": "test"}, adminHeader)
	if resp.StatusCode != 200 {
		t.Fatalf("create: %d %s", resp.StatusCode, body)
	}
	var created map[string]any
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	tokenID, _ := created["token"].(string)
	if tokenID == "" {
		t.Fatal("no token in response")
	}

	// List
	resp, body = doJSON(t, h, "GET", "/api/tokens", nil, adminHeader)
	if resp.StatusCode != 200 {
		t.Fatalf("list: %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"note":"test"`) {
		t.Fatalf("list missing note: %s", body)
	}

	// Revoke (need full unredacted id - test by going around the wire)
	a.stateMu.RLock()
	if _, ok := a.state.JoinTokens[tokenID]; !ok {
		t.Fatal("token not stored on agent")
	}
	a.stateMu.RUnlock()

	resp, body = doJSON(t, h, "DELETE", "/api/tokens/"+tokenID, nil, adminHeader)
	if resp.StatusCode != 200 {
		t.Fatalf("delete: %d %s", resp.StatusCode, body)
	}
	a.stateMu.RLock()
	_, stillExists := a.state.JoinTokens[tokenID]
	a.stateMu.RUnlock()
	if stillExists {
		t.Fatal("token still in state after revoke")
	}
}

// TestE2E_TokenEndpointsRequireAdmin checks that peer keys cannot mint
// or list tokens.
func TestE2E_TokenEndpointsRequireAdmin(t *testing.T) {
	a := e2eAgent(t)
	a.state.Peers["p1"] = &Peer{ID: "p1", Name: "p1", IP: "100.96.0.2", APIKey: "peer1-secret"}
	h := mountRoutes(a)

	peerHeader := map[string]string{"X-API-Key": "peer1-secret"}
	resp, _ := doJSON(t, h, "POST", "/api/tokens", nil, peerHeader)
	if resp.StatusCode != 401 {
		t.Fatalf("peer key shouldn't be allowed to mint, got %d", resp.StatusCode)
	}
	resp, _ = doJSON(t, h, "GET", "/api/tokens", nil, peerHeader)
	if resp.StatusCode != 401 {
		t.Fatalf("peer key shouldn't be allowed to list, got %d", resp.StatusCode)
	}
}

// TestE2E_JoinRequiresValidToken covers the join wire protocol:
// missing/bogus/expired tokens are rejected; a valid one is consumed
// exactly once and yields the expected keys/peers in the response.
func TestE2E_JoinRequiresValidToken(t *testing.T) {
	a := e2eAgent(t)
	h := mountRoutes(a)

	joinReq := func(token string) (*http.Response, []byte) {
		return doJSON(t, h, "POST", "/api/join", map[string]any{
			"join_token": token,
			"id":         "newnodehwid01",
			"name":       "newnode",
			"ip":         "100.96.0.99",
			"version":    "test",
			"arch":       "amd64",
			"api_key":    "newnode-self-key-of-sufficient-length",
		}, nil)
	}

	// 1. No token at all
	resp, body := joinReq("")
	if resp.StatusCode != 401 {
		t.Errorf("empty token should 401, got %d: %s", resp.StatusCode, body)
	}

	// 2. Bogus token
	resp, body = joinReq("not-a-real-token")
	if resp.StatusCode != 401 {
		t.Errorf("bogus token should 401, got %d: %s", resp.StatusCode, body)
	}

	// 3. Mint a real token and use it.
	tok := &JoinToken{
		ID:        "valid-token-xyz",
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
		Note:      "valid",
	}
	a.stateMu.Lock()
	a.state.JoinTokens[tok.ID] = tok
	a.stateMu.Unlock()

	resp, body = joinReq("valid-token-xyz")
	if resp.StatusCode != 200 {
		t.Fatalf("valid token should succeed, got %d: %s", resp.StatusCode, body)
	}
	var joinResp map[string]any
	if err := json.Unmarshal(body, &joinResp); err != nil {
		t.Fatal(err)
	}
	if joinResp["admin_key"] != "admin-secret" {
		t.Errorf("expected admin_key in response, got %v", joinResp["admin_key"])
	}
	if joinResp["encryption_key"] == nil {
		t.Errorf("expected encryption_key in response")
	}
	// Self entry in peers list must carry our SelfAPIKey
	peers, _ := joinResp["peers"].([]any)
	if len(peers) == 0 {
		t.Fatal("peers list empty")
	}
	self, _ := peers[0].(map[string]any)
	if self["api_key"] != "self-secret" {
		t.Errorf("self peer entry missing SelfAPIKey, got %v", self["api_key"])
	}

	// 4. Replay the same token - should fail.
	resp, body = joinReq("valid-token-xyz")
	if resp.StatusCode != 401 {
		t.Errorf("replay should 401, got %d: %s", resp.StatusCode, body)
	}

	// 5. Verify the joiner was registered with its APIKey.
	a.stateMu.RLock()
	registered, ok := a.state.Peers["newnodehwid01"]
	a.stateMu.RUnlock()
	if !ok {
		t.Fatal("joiner not registered as peer")
	}
	if registered.APIKey != "newnode-self-key-of-sufficient-length" {
		t.Errorf("APIKey not recorded, got %q", registered.APIKey)
	}
}

// TestE2E_StatusDoesNotLeakAPIKeys is a regression test for M2: the
// /api/status endpoint must not include peer api_key fields. Peer keys
// are equivalent for inbound auth, so leaking them would let any
// authenticated caller harvest every peer's credential.
func TestE2E_StatusDoesNotLeakAPIKeys(t *testing.T) {
	a := e2eAgent(t)
	a.state.Peers["p1"] = &Peer{
		ID: "p1", Name: "p1", IP: "100.96.0.2", APIKey: "peer1-MUST-NOT-LEAK",
	}
	r := mux.NewRouter()
	r.HandleFunc("/api/status", a.apiStatus).Methods("GET")

	resp, body := doJSON(t, r, "GET", "/api/status", nil, map[string]string{"X-API-Key": "admin-secret"})
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	if strings.Contains(string(body), "peer1-MUST-NOT-LEAK") {
		t.Errorf("/api/status leaked peer APIKey:\n%s", body)
	}
	if strings.Contains(string(body), "self-secret") {
		t.Errorf("/api/status leaked SelfAPIKey:\n%s", body)
	}
	if strings.Contains(string(body), `"api_key"`) {
		t.Errorf("/api/status response contains api_key field:\n%s", body)
	}
}

// TestE2E_ExpiredTokenRejected checks the time check fires.
func TestE2E_ExpiredTokenRejected(t *testing.T) {
	a := e2eAgent(t)
	h := mountRoutes(a)

	a.stateMu.Lock()
	a.state.JoinTokens["expired"] = &JoinToken{
		ID:        "expired",
		CreatedAt: time.Now().Add(-time.Hour),
		ExpiresAt: time.Now().Add(-time.Minute),
	}
	a.stateMu.Unlock()

	resp, body := doJSON(t, h, "POST", "/api/join", map[string]any{
		"join_token": "expired",
		"id":         "joiner",
		"name":       "joiner",
		"ip":         "100.96.0.55",
		"api_key":    "newnode-self-key-of-sufficient-length",
	}, nil)
	if resp.StatusCode != 401 {
		t.Fatalf("expired should 401, got %d: %s", resp.StatusCode, body)
	}
}
