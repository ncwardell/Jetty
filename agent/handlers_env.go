package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"sort"
	"time"

	"github.com/gorilla/mux"
)

// =============================================================================
// HTTP Handlers - encrypted environment variables
// =============================================================================
//
//   GET    /api/env         list keys (values stay encrypted)
//   POST   /api/env         set keys (batch); values encrypted before storage
//   GET    /api/env/{key}   return decrypted value (privileged)
//   DELETE /api/env/{key}   remove (writes a tombstone for sync)
//
// Encryption uses AES-256-GCM with an Argon2id-derived key (see crypto.go).
// Values are decrypted only at workload deploy time and on explicit GET.

// apiListEnv godoc
// @Summary List environment variables
// @Description Returns all stored environment variable keys (values are encrypted)
// @Tags env
// @Produce json
// @Success 200 {object} EnvListResponse
// @Router /env [get]
func (a *Agent) apiListEnv(w http.ResponseWriter, r *http.Request) {
	a.stateMu.RLock()
	keys := make([]string, 0, len(a.state.EnvData))
	for key := range a.state.EnvData {
		keys = append(keys, key)
	}
	a.stateMu.RUnlock()

	sort.Strings(keys)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"keys":  keys,
		"count": len(keys),
	})
}
// apiSetEnv godoc
// @Summary Set environment variables
// @Description Stores encrypted environment variables (existing keys are overwritten)
// @Tags env
// @Accept json
// @Param env body EnvSetRequest true "Environment variables to set"
// @Success 200 {object} EnvSetResponse
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 500 {object} ErrorResponse "Encryption error"
// @Router /env [post]
func (a *Agent) apiSetEnv(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Env map[string]string `json:"env"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	if len(req.Env) == 0 {
		http.Error(w, "env map required", 400)
		return
	}

	// Validate keys (alphanumeric and underscore only)
	envKeyPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	for key := range req.Env {
		if !envKeyPattern.MatchString(key) {
			http.Error(w, fmt.Sprintf("invalid env key: %s (must start with letter or underscore, contain only alphanumerics and underscores)", key), 400)
			return
		}
	}

	// Encrypt OUTSIDE the state lock - encryptValue takes stateMu
	// itself (to ensureEncryptionKey, then to read it back), so doing
	// the encryption inside our own Lock would deadlock.
	encryptedMap := make(map[string]string, len(req.Env))
	for key, value := range req.Env {
		encrypted, err := a.encryptValue(value)
		if err != nil {
			http.Error(w, fmt.Sprintf("encrypt %s: %v", key, err), 500)
			return
		}
		encryptedMap[key] = encrypted
	}

	a.stateMu.Lock()
	added := make([]string, 0)
	updated := make([]string, 0)
	undeleted := make([]string, 0)
	for key, encrypted := range encryptedMap {
		if _, exists := a.state.EnvData[key]; exists {
			updated = append(updated, key)
		} else {
			added = append(added, key)
		}
		// Setting a key after it was deleted: the tombstone in
		// DeletedEnvKeys would otherwise win on the next sync round
		// and re-delete the value. Clear it locally and remember to
		// broadcast the un-delete so peers clear theirs too.
		if _, hadTombstone := a.state.DeletedEnvKeys[key]; hadTombstone {
			delete(a.state.DeletedEnvKeys, key)
			undeleted = append(undeleted, key)
		}
		a.state.EnvData[key] = encrypted
	}
	a.stateMu.Unlock()

	a.saveState()

	// Push the change out via memberlist so peers see it within a few
	// hundred ms instead of waiting for the 30s /api/sync poll. Order
	// matters: send env_undelete first so any tombstone-aware peer
	// clears its tombstone before the env_update arrives.
	for _, key := range undeleted {
		a.broadcastEnvUndelete(key)
	}
	for key, encrypted := range encryptedMap {
		a.broadcastEnvUpdate(key, encrypted)
	}

	sort.Strings(added)
	sort.Strings(updated)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"added":   added,
		"updated": updated,
	})

	log.Printf("Env set: added=%v, updated=%v", added, updated)
}
// apiGetEnv godoc
// @Summary Get environment variable
// @Description Returns the decrypted value of a specific environment variable
// @Tags env
// @Produce json
// @Param key path string true "Environment variable key"
// @Success 200 {object} EnvGetResponse
// @Failure 404 {object} ErrorResponse "Key not found"
// @Failure 500 {object} ErrorResponse "Decryption error"
// @Router /env/{key} [get]
func (a *Agent) apiGetEnv(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]

	a.stateMu.RLock()
	encrypted, exists := a.state.EnvData[key]
	a.stateMu.RUnlock()

	if !exists {
		http.Error(w, "env key not found", 404)
		return
	}

	value, err := a.decryptValue(encrypted)
	if err != nil {
		http.Error(w, fmt.Sprintf("decrypt: %v", err), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"key":   key,
		"value": value,
	})
}
// apiDeleteEnv godoc
// @Summary Delete environment variable
// @Description Removes an environment variable from storage
// @Tags env
// @Param key path string true "Environment variable key"
// @Success 204 "Variable deleted"
// @Failure 404 {object} ErrorResponse "Key not found"
// @Router /env/{key} [delete]
func (a *Agent) apiDeleteEnv(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]

	a.stateMu.Lock()
	_, exists := a.state.EnvData[key]
	var tombstone *DeletedEnvKey
	if exists {
		delete(a.state.EnvData, key)
		// Create tombstone to propagate deletion across cluster
		tombstone = &DeletedEnvKey{
			Key:     key,
			Version: time.Now().UnixNano(),
		}
		a.state.DeletedEnvKeys[key] = tombstone
	}
	a.stateMu.Unlock()

	if !exists {
		http.Error(w, "env key not found", 404)
		return
	}

	a.saveState()

	// Push the deletion via memberlist so peers tombstone immediately
	// instead of waiting up to 30s for the next /api/sync round. The
	// receiver's handleEnvDelete deletes the value from its own
	// EnvData and stores the tombstone.
	if tombstone != nil {
		a.broadcastEnvDelete(tombstone)
	}

	w.WriteHeader(204)
	log.Printf("Env deleted: %s, created tombstone for sync propagation", key)
}
