package agent

import (
	"sync"
	"testing"
	"time"
)

func newTestAgentWithDir(t *testing.T) *Agent {
	t.Helper()
	a := &Agent{
		state:   NewState(),
		stateMu: sync.RWMutex{},
		dataDir: t.TempDir(),
	}
	return a
}

func TestConsumeJoinToken_HappyPath(t *testing.T) {
	a := newTestAgentWithDir(t)
	tok := &JoinToken{
		ID:        "abc123",
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(1 * time.Hour),
	}
	a.state.JoinTokens["abc123"] = tok

	got, err := a.consumeJoinToken("abc123", "joiner-id")
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if !got.Used {
		t.Fatal("token not marked used")
	}
	if got.UsedBy != "joiner-id" {
		t.Fatalf("UsedBy = %q, want joiner-id", got.UsedBy)
	}
	if got.UsedAt.IsZero() {
		t.Fatal("UsedAt not set")
	}
}

func TestConsumeJoinToken_MissingTokenIs401(t *testing.T) {
	a := newTestAgentWithDir(t)
	if _, err := a.consumeJoinToken("", "x"); err == nil {
		t.Fatal("expected error for empty token")
	}
	if _, err := a.consumeJoinToken("does-not-exist", "x"); err == nil {
		t.Fatal("expected error for unknown token")
	}
}

func TestConsumeJoinToken_DoubleUseRejected(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.state.JoinTokens["t"] = &JoinToken{
		ID:        "t",
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(1 * time.Hour),
	}
	if _, err := a.consumeJoinToken("t", "first"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.consumeJoinToken("t", "second"); err == nil {
		t.Fatal("second use should be rejected")
	}
}

func TestConsumeJoinToken_ExpiredRejected(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.state.JoinTokens["t"] = &JoinToken{
		ID:        "t",
		CreatedAt: time.Now().Add(-2 * time.Hour),
		ExpiresAt: time.Now().Add(-1 * time.Hour),
	}
	if _, err := a.consumeJoinToken("t", "x"); err == nil {
		t.Fatal("expired token should be rejected")
	}
}

func TestGCExpiredTokens(t *testing.T) {
	a := newTestAgentWithDir(t)
	now := time.Now()
	a.state.JoinTokens["fresh"] = &JoinToken{ID: "fresh", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	a.state.JoinTokens["expired"] = &JoinToken{ID: "expired", CreatedAt: now.Add(-2 * time.Hour), ExpiresAt: now.Add(-time.Hour)}
	a.state.JoinTokens["old-burned"] = &JoinToken{ID: "old-burned", CreatedAt: now.Add(-48 * time.Hour), ExpiresAt: now.Add(-47 * time.Hour), Used: true, UsedAt: now.Add(-25 * time.Hour)}
	a.state.JoinTokens["recent-burned"] = &JoinToken{ID: "recent-burned", CreatedAt: now, ExpiresAt: now.Add(time.Hour), Used: true, UsedAt: now.Add(-1 * time.Minute)}

	a.gcExpiredTokens()

	if _, ok := a.state.JoinTokens["expired"]; ok {
		t.Error("expired-unused token was not collected")
	}
	if _, ok := a.state.JoinTokens["old-burned"]; ok {
		t.Error("old-burned token was not collected")
	}
	if _, ok := a.state.JoinTokens["fresh"]; !ok {
		t.Error("fresh token was incorrectly collected")
	}
	if _, ok := a.state.JoinTokens["recent-burned"]; !ok {
		t.Error("recent-burned token was incorrectly collected (within retention window)")
	}
}

func TestAuthorizeAPIKey(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.state.AdminKey = "admin-key"
	a.state.SelfAPIKey = "self-key"
	a.state.Peers["peer1"] = &Peer{ID: "peer1", APIKey: "peer1-key"}
	a.state.Peers["peer2"] = &Peer{ID: "peer2", APIKey: ""} // peer with no key set

	cases := []struct {
		key  string
		want bool
	}{
		{"admin-key", true},
		{"self-key", true},
		{"peer1-key", true},
		{"unknown", false},
		{"", false},
	}
	for _, c := range cases {
		if got := a.authorizeAPIKey(c.key); got != c.want {
			t.Errorf("authorizeAPIKey(%q) = %v, want %v", c.key, got, c.want)
		}
	}
}

func TestRedactTokenID(t *testing.T) {
	long := "abcdef1234567890"
	if got := redactTokenID(long); got != "abcdef12..." {
		t.Errorf("got %q", got)
	}
	if got := redactTokenID("short"); got != "short" {
		t.Errorf("short id should pass through, got %q", got)
	}
}
