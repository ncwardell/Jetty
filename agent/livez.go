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
// Deliberately cheap. The handler itself does no polling and starts no timer.
// TryRLock is non-blocking by definition, so this handler cannot itself get
// stuck on the lock it is reporting about. It does read one atomic fed by the
// wedge watchdog (wedge.go) - the single background prober added after
// 2026-11-18, when this endpoint said 200 over a wedged agent for as long as
// it stayed wedged and nothing acted on it.

// livezStuckGoroutines is the point at which a goroutine count stops looking
// like load and starts looking like a pile-up. A healthy agent sits in the
// low tens; hundreds means requests are parked on something.
const livezStuckGoroutines = 200

// apiLivez godoc
// @Summary Liveness and lock-health probe
// @Description Answers without taking any lock, so it still responds when the agent's state mutex is held by a stuck writer - the one failure mode /api/health cannot report, because it takes that lock itself. `state_lock_acquirable` false alongside a high `goroutines` count, or `state_lock_unacquirable_seconds` past a minute, means the control plane is wedged rather than merely busy; the response is then HTTP 503 (same body, alive=false) so external health checks can act on it. The agent restarts itself after `wedge_restart_after_seconds` of that. Unauthenticated; the handler takes no lock.
// @Tags cluster
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Failure 503 {object} map[string]interface{} "Control plane WEDGED (same body, alive=false)"
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

	// How long the wedge watchdog (wedge.go) has continuously failed to get
	// the lock. An atomic read - still no lock taken here.
	stuckFor := a.stateLockUnacquirableFor(time.Now())

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
		// Continuous, as seen by the wedge watchdog every 10s - the TryRLock
		// above is a single sample; this is the trend.
		"state_lock_unacquirable_seconds": int(stuckFor.Seconds()),
		"wedge_restart_after_seconds":     int(time.Duration(a.wedgeThreshold.Load()).Seconds()),
		"log_level":                       CurrentLogLevel(),
		"tunnel_stack":                    a.activeTunnelStack(),
		// Lock-free (cfReadyMu is never held across I/O), so it is safe
		// here. "running but not ready" is the state a 19-day public
		// ingress outage hid in; it belongs next to the other "is this
		// node actually working" signals.
		"cloudflared": a.cloudflaredReadinessReport(),
	}

	// Say what it means, so an operator does not have to know the internals
	// at 2am. This endpoint exists because the last two incidents were lost
	// time spent guessing.
	//
	// WEDGED has two routes in. The goroutine pile-up is the original one; the
	// wedge watchdog's continuous-failure clock is the second, because a node
	// that wedges while nobody is calling it never piles anything up - it just
	// sits there, and on 2026-11-18 this endpoint kept answering 200 over it.
	wedged := !acquirable &&
		(goroutines > livezStuckGoroutines || stuckFor >= livezWedgedAfter)
	switch {
	case wedged:
		resp["diagnosis"] = "control plane appears WEDGED: the state lock is not " +
			"acquirable and has not been for a while (or goroutines have piled up). " +
			"Something is holding stateMu. The wedge watchdog restarts the agent " +
			"after wedge_restart_after_seconds, saving a goroutine dump under " +
			"the data dir's wedge-dumps/; send SIGQUIT for a dump now, or restart the agent."
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
	//
	// 503 when wedged, so an external health check (docker HEALTHCHECK, an
	// uptime monitor, a load balancer) can act on the status code alone.
	// Everything short of WEDGED stays 200: a momentarily busy lock is normal
	// and must not get a healthy node pulled out of rotation.
	status := http.StatusOK
	if wedged {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	writeJSONRaw(w, resp)
}

// writeJSONRaw encodes directly with no shared state or helpers, so the
// liveness path has as few dependencies as possible.
func writeJSONRaw(w http.ResponseWriter, v interface{}) {
	json.NewEncoder(w).Encode(v)
}
