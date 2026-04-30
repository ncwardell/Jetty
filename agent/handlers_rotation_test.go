package agent

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

func e2eAgentForRotation(t *testing.T) *Agent {
	t.Helper()
	a := newTestAgentWithDir(t)
	a.hwid = "self-hwid"
	a.hostname = "self"
	a.ip = "100.96.0.1"
	a.composeDir = t.TempDir()
	a.hostsFile = t.TempDir() + "/hosts"
	a.state.AdminKey = "admin-secret"
	a.state.SelfAPIKey = "self-secret"
	a.state.Peers["peer1"] = &Peer{ID: "peer1", Name: "p1", IP: "100.96.0.2", Healthy: true, APIKey: "peer1-secret"}
	return a
}

// TestRotateAdminKey_RejectsPeerKey - only AdminKey holders can rotate.
func TestRotateAdminKey_RejectsPeerKey(t *testing.T) {
	a := e2eAgentForRotation(t)
	req := httptest.NewRequest("POST", "/api/admin-key/rotate", nil)
	req.Header.Set("X-API-Key", "peer1-secret")
	rec := httptest.NewRecorder()
	a.apiRotateAdminKey(rec, req)
	if rec.Code != 401 {
		t.Errorf("peer key should be rejected, got %d", rec.Code)
	}
}

// TestRotateAdminKey_GeneratesIfMissing - empty body -> server picks
// a fresh 256-bit key and returns it.
func TestRotateAdminKey_GeneratesIfMissing(t *testing.T) {
	a := e2eAgentForRotation(t)
	req := httptest.NewRequest("POST", "/api/admin-key/rotate", nil)
	req.Header.Set("X-API-Key", "admin-secret")
	rec := httptest.NewRecorder()
	a.apiRotateAdminKey(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var resp map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["new_key"] == "" {
		t.Fatal("expected new_key in response")
	}
	if resp["new_key"] == "admin-secret" {
		t.Fatal("new_key matches the old admin key")
	}
	a.stateMu.RLock()
	if a.state.AdminKey != resp["new_key"] {
		t.Errorf("state.AdminKey = %q, want %q", a.state.AdminKey, resp["new_key"])
	}
	a.stateMu.RUnlock()
}

// TestRotateAdminKey_AcceptsCustomKey - operator can set a known value.
func TestRotateAdminKey_AcceptsCustomKey(t *testing.T) {
	a := e2eAgentForRotation(t)
	body := []byte(`{"new_key": "i-picked-this-myself-and-its-long-enough"}`)
	req := httptest.NewRequest("POST", "/api/admin-key/rotate", bytes.NewReader(body))
	req.Header.Set("X-API-Key", "admin-secret")
	rec := httptest.NewRecorder()
	a.apiRotateAdminKey(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.state.AdminKey != "i-picked-this-myself-and-its-long-enough" {
		t.Errorf("state.AdminKey = %q", a.state.AdminKey)
	}
}

// TestRotateAdminKey_RejectsShortKey - < 16 chars is suspicious.
func TestRotateAdminKey_RejectsShortKey(t *testing.T) {
	a := e2eAgentForRotation(t)
	body := []byte(`{"new_key": "tooshort"}`)
	req := httptest.NewRequest("POST", "/api/admin-key/rotate", bytes.NewReader(body))
	req.Header.Set("X-API-Key", "admin-secret")
	rec := httptest.NewRecorder()
	a.apiRotateAdminKey(rec, req)
	if rec.Code != 400 {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

// TestRotateSelfAPIKey - self-rotate generates a fresh key, persists,
// and updates our own peer entry if present.
func TestRotateSelfAPIKey(t *testing.T) {
	a := e2eAgentForRotation(t)
	a.state.Peers[a.hwid] = &Peer{ID: a.hwid, Name: a.hostname, IP: a.ip, Healthy: true, APIKey: a.state.SelfAPIKey}

	old := a.state.SelfAPIKey
	newKey, err := a.rotateSelfAPIKeyLocal()
	if err != nil {
		t.Fatal(err)
	}
	if newKey == old {
		t.Fatal("new key matches old")
	}
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.state.SelfAPIKey != newKey {
		t.Errorf("state.SelfAPIKey = %q, want %q", a.state.SelfAPIKey, newKey)
	}
	if a.state.Peers[a.hwid].APIKey != newKey {
		t.Errorf("self-peer entry still has old key")
	}
}

// TestApiRotatePeerKey_SelfPath - hitting the endpoint with id="self"
// (or our own hwid) rotates locally without any proxying.
func TestApiRotatePeerKey_SelfPath(t *testing.T) {
	a := e2eAgentForRotation(t)
	old := a.state.SelfAPIKey

	// Use the gorilla/mux handler indirectly via a shaped request.
	r := httptest.NewRequest("POST", "/api/peers/self/rotate-key", nil)
	r.Header.Set("X-API-Key", "admin-secret")
	// Manually inject mux vars since we're calling the handler
	// directly without registering a router.
	r = mux.SetURLVars(r, map[string]string{"id": "self"})
	rec := httptest.NewRecorder()
	a.apiRotatePeerKey(rec, r)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.state.SelfAPIKey == old {
		t.Error("SelfAPIKey not rotated")
	}
}

// TestApiRotatePeerKey_RejectsPeerKeyAuth - admin only, regardless of
// which peer is being rotated.
func TestApiRotatePeerKey_RejectsPeerKeyAuth(t *testing.T) {
	a := e2eAgentForRotation(t)
	r := httptest.NewRequest("POST", "/api/peers/self/rotate-key", nil)
	r.Header.Set("X-API-Key", "peer1-secret")
	r = mux.SetURLVars(r, map[string]string{"id": "self"})
	rec := httptest.NewRecorder()
	a.apiRotatePeerKey(rec, r)
	if rec.Code != 401 {
		t.Errorf("peer key should be rejected, got %d", rec.Code)
	}
}

// TestApiRotatePeerKey_UnknownPeer404 - non-existent target -> 404.
func TestApiRotatePeerKey_UnknownPeer404(t *testing.T) {
	a := e2eAgentForRotation(t)
	r := httptest.NewRequest("POST", "/api/peers/ghost/rotate-key", nil)
	r.Header.Set("X-API-Key", "admin-secret")
	r = mux.SetURLVars(r, map[string]string{"id": "ghost"})
	rec := httptest.NewRecorder()
	a.apiRotatePeerKey(rec, r)
	if rec.Code != 404 {
		t.Errorf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestRotateAdminKey_NewKeyIsBroadcastable - sanity check: the
// rotation handler invokes the broadcast helper. We don't run a real
// memberlist here, but the helper should noop on nil mlDelegate
// without crashing.
func TestRotateAdminKey_NoMemberlistOK(t *testing.T) {
	a := e2eAgentForRotation(t)
	a.mlDelegate = nil
	body := []byte(`{}`)
	req := httptest.NewRequest("POST", "/api/admin-key/rotate", bytes.NewReader(body))
	req.Header.Set("X-API-Key", "admin-secret")
	rec := httptest.NewRecorder()
	a.apiRotateAdminKey(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "new_key") {
		t.Error("response missing new_key")
	}
}
