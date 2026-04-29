package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// =============================================================================
// State Persistence
// =============================================================================

// saveState persists the current state to disk atomically.
// Uses write-to-temp-then-rename pattern for crash safety.
func (a *Agent) saveState() {
	a.stateMu.RLock()
	data, _ := json.MarshalIndent(a.state, "", "  ")
	a.stateMu.RUnlock()

	// Atomic write: write to temp file then rename
	statePath := filepath.Join(a.dataDir, "state.json")
	tempPath := statePath + ".tmp"

	// 0600: state.json contains plaintext cluster tokens (WARP/CF) and
	// encrypted-but-still-sensitive env data. Must not be world-readable.
	if err := os.WriteFile(tempPath, data, 0600); err != nil {
		log.Printf("Failed to write state: %v", err)
		return
	}

	if err := os.Rename(tempPath, statePath); err != nil {
		log.Printf("Failed to rename state file: %v", err)
		os.Remove(tempPath) // Clean up temp file
	}
}

// loadState loads the state from disk.
// Preserves environment variable tokens over saved state.
// On corrupt JSON, the bad file is preserved as state.json.corrupt-<timestamp>
// and the agent boots with empty state rather than overwriting it.
func (a *Agent) loadState() {
	statePath := filepath.Join(a.dataDir, "state.json")
	data, err := os.ReadFile(statePath)
	if err != nil {
		return
	}

	a.stateMu.Lock()
	defer a.stateMu.Unlock()

	// Preserve env var CF token if set
	envCFToken := a.state.CFToken

	if err := json.Unmarshal(data, a.state); err != nil {
		backup := fmt.Sprintf("%s.corrupt-%d", statePath, time.Now().Unix())
		if renameErr := os.Rename(statePath, backup); renameErr != nil {
			log.Printf("CRITICAL: state.json is corrupt (%v) and could not be moved aside (%v) - refusing to overwrite. Inspect %s manually.", err, renameErr, statePath)
		} else {
			log.Printf("CRITICAL: state.json was corrupt (%v) - moved to %s, booting with empty state", err, backup)
		}
		// Reset to a fresh state so we don't keep partially-decoded fields.
		a.state = NewState()
	}

	// Env var takes precedence over saved state
	if envCFToken != "" && a.state.CFToken == "" {
		a.state.CFToken = envCFToken
	}

	// Initialize maps if nil (for backwards compatibility or corrupted state)
	a.ensureStateMapsInitialized()

	log.Printf("Loaded: %d peers, %d workloads, %d env vars", len(a.state.Peers), len(a.state.Workloads), len(a.state.EnvData))
}

// ensureStateMapsInitialized ensures all state maps are non-nil.
// Must be called with stateMu held.
func (a *Agent) ensureStateMapsInitialized() {
	if a.state.Peers == nil {
		a.state.Peers = make(map[string]*Peer)
	}
	if a.state.Workloads == nil {
		a.state.Workloads = make(map[string]*Workload)
	}
	if a.state.EnvData == nil {
		a.state.EnvData = make(map[string]string)
	}
	if a.state.DeletedWorkloads == nil {
		a.state.DeletedWorkloads = make(map[string]*DeletedWorkload)
	}
	if a.state.DeletedEnvKeys == nil {
		a.state.DeletedEnvKeys = make(map[string]*DeletedEnvKey)
	}
}
