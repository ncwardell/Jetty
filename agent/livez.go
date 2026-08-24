package agent

import (
	"encoding/json"
	"net/http"
	"runtime"
	"time"
)

// =============================================================================
// Liveness
// =============================================================================
//
// /api/health is not a liveness check. It takes stateMu twice, and the auth
// middleware used to take it before even deciding whether the endpoint needed
// auth - so when a writer was stuck holding that lock, health blocked along
// with everything else. A node in that state answers nothing at all while its
// listener stays open, because the kernel owns the socket. From outside that
// is indistinguishable from a broken tunnel, which is exactly how two
// incidents got misattributed for hours.
//
// This endpoint answers the question health cannot: is the agent's control
// plane actually able to make progress?
//
// Deliberately cheap. There is no background goroutine, no polling and no
// timer - it costs nothing at all until someone asks. TryRLock is
// non-blocking by definition, so this handler cannot itself get stuck on the
// lock it is reporting about.

// livezStuckGoroutines is the point at which a goroutine count stops looking
// like load and starts looking like a pile-up. A healthy agent sits in the
// low tens; hundreds means requests are parked on something.
const livezStuckGoroutines = 200

// apiLivez godoc
// @Summary Liveness and lock-health probe
// @Description Answers without taking any lock, so it still responds when the agent's state mutex is held by a stuck writer - the one failure mode /api/health cannot report, because it takes that lock itself. `state_lock_acquirable` false alongside a high `goroutines` count means the control plane is wedged rather than merely busy. Unauthenticated and free: no background work, nothing runs until this is called.
// @Tags cluster
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /livez [get]
func (a *Agent) apiLivez(w http.ResponseWriter, r *http.Request) {
	// TryRLock never blocks. It returns false while a writer holds the lock
	// OR is merely queued, so a single false reading is not proof of a
	// problem - it is one signal to read alongside the goroutine count.
	acquirable := a.stateMu.TryRLock()
	if acquirable {
		a.stateMu.RUnlock()
	}

	goroutines := runtime.NumGoroutine()

	// hwid, hostname and startTime are set once at construction and never
	// mutated, so reading them without a lock is safe - which is the whole
	// point of this handler.
	resp := map[string]interface{}{
		"alive":                 true,
		"node":                  a.hostname,
		"id":                    a.hwid,
		"version":               Version,
		"uptime_seconds":        int(time.Since(a.startTime).Seconds()),
		"goroutines":            goroutines,
		"state_lock_acquirable": acquirable,
	}

	// Say what it means, so an operator does not have to know the internals
	// at 2am. This endpoint exists because the last two incidents were lost
	// time spent guessing.
	switch {
	case !acquirable && goroutines > livezStuckGoroutines:
		resp["diagnosis"] = "control plane appears WEDGED: the state lock is not " +
			"acquirable and goroutines have piled up. Something is holding stateMu. " +
			"Send SIGQUIT for a goroutine dump, or restart the agent."
		resp["alive"] = false
	case !acquirable:
		resp["diagnosis"] = "state lock momentarily unavailable - normal if a write " +
			"is in flight. Only a concern if it persists across several probes."
	case goroutines > livezStuckGoroutines:
		resp["diagnosis"] = "the lock is fine but goroutines are elevated; " +
			"look for a leak or a slow upstream rather than a deadlock."
	default:
		resp["diagnosis"] = "healthy"
	}

	// Deliberately not writeJSON: that is fine today, but this handler's whole
	// job is to keep working when other things do not, so it depends on as
	// little as possible.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	writeJSONRaw(w, resp)
}

// writeJSONRaw encodes directly with no shared state or helpers, so the
// liveness path has as few dependencies as possible.
func writeJSONRaw(w http.ResponseWriter, v interface{}) {
	json.NewEncoder(w).Encode(v)
}
