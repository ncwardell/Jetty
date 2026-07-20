package agent

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// A pending (unused) token is listed with a redacted 8-char-prefix id.
// Revoking it via that displayed form must work - otherwise the dashboard
// delete button silently no-ops on pending tokens.
func TestDeleteToken_ByRedactedPrefix(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.state.AdminKey = "admin"
	fullID := "abcdef1234567890deadbeefcafe"
	a.state.JoinTokens[fullID] = &JoinToken{ID: fullID, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}

	redacted := redactTokenID(fullID) // "abcdef12..."
	req := httptest.NewRequest("DELETE", "/api/tokens/"+redacted, nil)
	req.Header.Set("X-API-Key", "admin")
	req = mux.SetURLVars(req, map[string]string{"id": redacted})
	rr := httptest.NewRecorder()
	a.apiDeleteToken(rr, req)

	if rr.Code != 200 {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"revoked":true`) {
		t.Fatalf("body = %s, want revoked:true", strings.TrimSpace(rr.Body.String()))
	}
	if _, ok := a.state.JoinTokens[fullID]; ok {
		t.Fatal("token should have been deleted by its redacted prefix")
	}
}

// Full-id delete still works (used tokens are shown in full).
func TestDeleteToken_ByFullID(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.state.AdminKey = "admin"
	a.state.JoinTokens["fullsecret"] = &JoinToken{ID: "fullsecret", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
	req := httptest.NewRequest("DELETE", "/api/tokens/fullsecret", nil)
	req.Header.Set("X-API-Key", "admin")
	req = mux.SetURLVars(req, map[string]string{"id": "fullsecret"})
	rr := httptest.NewRecorder()
	a.apiDeleteToken(rr, req)
	if _, ok := a.state.JoinTokens["fullsecret"]; ok {
		t.Fatal("token should have been deleted by full id")
	}
}

// An ambiguous redacted prefix (two tokens share the first 8 chars) must NOT
// delete anything - we only act on a unique match.
func TestDeleteToken_AmbiguousPrefixNoOp(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.state.AdminKey = "admin"
	a.state.JoinTokens["abcdef12AAAAAAAA"] = &JoinToken{ID: "abcdef12AAAAAAAA", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
	a.state.JoinTokens["abcdef12BBBBBBBB"] = &JoinToken{ID: "abcdef12BBBBBBBB", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}

	req := httptest.NewRequest("DELETE", "/api/tokens/abcdef12...", nil)
	req.Header.Set("X-API-Key", "admin")
	req = mux.SetURLVars(req, map[string]string{"id": "abcdef12..."})
	rr := httptest.NewRecorder()
	a.apiDeleteToken(rr, req)

	if len(a.state.JoinTokens) != 2 {
		t.Fatalf("ambiguous prefix must not delete anything; have %d tokens", len(a.state.JoinTokens))
	}
	if !strings.Contains(rr.Body.String(), `"revoked":false`) {
		t.Fatalf("body = %s, want revoked:false", strings.TrimSpace(rr.Body.String()))
	}
}

// claimHeal enforces the per-container restart cooldown.
func TestClaimHeal_Cooldown(t *testing.T) {
	a := &Agent{healTimes: make(map[string]time.Time), healTimesMu: sync.Mutex{}}
	if !a.claimHeal("c1") {
		t.Fatal("first claim should succeed")
	}
	if a.claimHeal("c1") {
		t.Fatal("second claim within cooldown should be denied")
	}
	if !a.claimHeal("c2") {
		t.Fatal("a different container should be allowed independently")
	}
	// Simulate the cooldown having elapsed.
	a.healTimes["c1"] = time.Now().Add(-2 * healContainerCooldown)
	if !a.claimHeal("c1") {
		t.Fatal("claim should succeed again after the cooldown elapses")
	}
}
