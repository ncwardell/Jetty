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

// TestHandleEnvUpdate_VersionNewerThanTombstone covers the gossip race
// the merge fix exists to absorb at the /api/sync layer: env_update
// arrives at a peer that still has a stale tombstone (because
// env_undelete was lost or arrives later). With version > tombstone,
// the peer must retire the tombstone and accept the value, instead of
// waiting up to 30s for the next sync round.
func TestHandleEnvUpdate_VersionNewerThanTombstone(t *testing.T) {
	a := envTestAgent(t)
	a.state.DeletedEnvKeys["FOO"] = &DeletedEnvKey{Key: "FOO", Version: 100}

	a.handleEnvUpdate("FOO", "v2", 200)

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if _, present := a.state.DeletedEnvKeys["FOO"]; present {
		t.Error("stale tombstone should have been retired")
	}
	if a.state.EnvData["FOO"] != "v2" {
		t.Errorf("EnvData not updated, got %q", a.state.EnvData["FOO"])
	}
}

// TestHandleEnvUpdate_VersionOlderThanTombstone is the inverse: a stale
// re-broadcast (version <= tombstone.version) must NOT resurrect a
// value the cluster has legitimately deleted.
func TestHandleEnvUpdate_VersionOlderThanTombstone(t *testing.T) {
	a := envTestAgent(t)
	a.state.DeletedEnvKeys["FOO"] = &DeletedEnvKey{Key: "FOO", Version: 200}

	a.handleEnvUpdate("FOO", "stale", 100)

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if _, present := a.state.DeletedEnvKeys["FOO"]; !present {
		t.Error("fresher tombstone should still be present")
	}
	if _, present := a.state.EnvData["FOO"]; present {
		t.Error("value should not have been resurrected")
	}
}

// TestHandleEnvUpdate_ZeroVersionBackcompat covers the rolling-upgrade
// case where a pre-versioned peer broadcasts env_update with version=0.
// We keep the conservative behavior (drop if tombstone present) so the
// older peer can't resurrect a value via a stale broadcast - /api/sync
// merge logic heals these cases within 30s instead.
func TestHandleEnvUpdate_ZeroVersionBackcompat(t *testing.T) {
	a := envTestAgent(t)
	a.state.DeletedEnvKeys["FOO"] = &DeletedEnvKey{Key: "FOO", Version: 100}

	a.handleEnvUpdate("FOO", "v2", 0)

	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if _, present := a.state.DeletedEnvKeys["FOO"]; !present {
		t.Error("tombstone should still be present when broadcast has no version")
	}
	if _, present := a.state.EnvData["FOO"]; present {
		t.Error("value should not be set when broadcast has no version and tombstone exists")
	}
}

// TestHandleEnvUpdate_NoTombstoneAcceptsValue covers the trivial happy
// path: no local tombstone, the value is accepted regardless of
// version.
func TestHandleEnvUpdate_NoTombstoneAcceptsValue(t *testing.T) {
	a := envTestAgent(t)
	a.handleEnvUpdate("FOO", "v1", 0)
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.state.EnvData["FOO"] != "v1" {
		t.Errorf("EnvData not stored, got %q", a.state.EnvData["FOO"])
	}
}
