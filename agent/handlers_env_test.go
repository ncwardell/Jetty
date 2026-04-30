package agent

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// envTestAgent returns a minimally-initialized Agent that can serve
// apiSetEnv / apiDeleteEnv without touching the network or peers.
func envTestAgent(t *testing.T) *Agent {
	t.Helper()
	a := &Agent{
		state:   NewState(),
		stateMu: sync.RWMutex{},
		dataDir: t.TempDir(),
	}
	a.state.AdminKey = "admin"
	a.state.SelfAPIKey = "self"
	if err := a.ensureEncryptionKey(); err != nil {
		t.Fatal(err)
	}
	return a
}

// TestSetEnv_ClearsExistingTombstone is the regression test for the
// bug we hit live: deleting a secret created a tombstone in
// DeletedEnvKeys. Re-setting the same key via apiSetEnv added the
// value to EnvData but left the tombstone in place. The next sync
// round merged the tombstone and silently re-deleted the value.
func TestSetEnv_ClearsExistingTombstone(t *testing.T) {
	a := envTestAgent(t)

	// Pre-seed a tombstone for STORAGEBOX_PASSWORD as if the key was
	// recently deleted. (Same shape apiDeleteEnv writes.)
	a.state.DeletedEnvKeys["STORAGEBOX_PASSWORD"] = &DeletedEnvKey{
		Key:     "STORAGEBOX_PASSWORD",
		Version: time.Now().UnixNano(),
	}

	// Re-set the key via apiSetEnv.
	body, _ := json.Marshal(map[string]any{
		"env": map[string]string{"STORAGEBOX_PASSWORD": "the-new-value"},
	})
	rec := httptest.NewRecorder()
	a.apiSetEnv(rec, httptest.NewRequest("POST", "/api/env", bytes.NewReader(body)))
	if rec.Code != 200 {
		t.Fatalf("apiSetEnv: %d %s", rec.Code, rec.Body.String())
	}

	// State invariants:
	//  1. EnvData has the new (encrypted) value.
	//  2. DeletedEnvKeys no longer has the tombstone - if it did, the
	//     next sync round would resurrect the deletion and wipe the
	//     value cluster-wide.
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if _, ok := a.state.EnvData["STORAGEBOX_PASSWORD"]; !ok {
		t.Error("EnvData missing the re-set key")
	}
	if _, stillTombstoned := a.state.DeletedEnvKeys["STORAGEBOX_PASSWORD"]; stillTombstoned {
		t.Error("tombstone was NOT cleared on re-set; the next sync round would wipe the value")
	}
}

// TestSetEnv_NoTombstoneNoOp - if there's no tombstone to begin with,
// apiSetEnv should still succeed and not synthesize one.
func TestSetEnv_NoTombstoneNoOp(t *testing.T) {
	a := envTestAgent(t)
	body, _ := json.Marshal(map[string]any{
		"env": map[string]string{"FRESH_KEY": "v1"},
	})
	rec := httptest.NewRecorder()
	a.apiSetEnv(rec, httptest.NewRequest("POST", "/api/env", bytes.NewReader(body)))
	if rec.Code != 200 {
		t.Fatalf("apiSetEnv: %d %s", rec.Code, rec.Body.String())
	}
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if _, ok := a.state.EnvData["FRESH_KEY"]; !ok {
		t.Error("EnvData missing the freshly added key")
	}
	if len(a.state.DeletedEnvKeys) != 0 {
		t.Errorf("DeletedEnvKeys should be empty, got %v", a.state.DeletedEnvKeys)
	}
}

// TestHandleEnvUndelete is the receive-side counterpart: a peer's
// env_undelete broadcast must clear our local tombstone so we don't
// re-broadcast it on the next /api/sync round.
func TestHandleEnvUndelete(t *testing.T) {
	a := envTestAgent(t)
	a.state.DeletedEnvKeys["FOO"] = &DeletedEnvKey{Key: "FOO", Version: time.Now().UnixNano()}

	a.handleEnvUndelete("FOO")

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if _, present := a.state.DeletedEnvKeys["FOO"]; present {
		t.Error("handleEnvUndelete did not clear the tombstone")
	}
}
