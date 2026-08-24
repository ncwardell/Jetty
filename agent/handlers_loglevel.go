package agent

import (
	"encoding/json"
	"io"
	"net/http"
)

// =============================================================================
// HTTP Handlers - runtime log level
// =============================================================================
//
//   GET  /api/log-level   what level is this node logging at
//   POST /api/log-level   change it, without a restart
//
// JETTY_LOG_LEVEL only applies at startup, so turning on debug logging used to
// mean restarting the node - which throws away the state you restarted it to
// investigate. The failures actually worth debugging (a wedged control plane,
// a stalled tunnel flow) are precisely the ones a restart erases.
//
// Admin-only: this is an operational control, and debug logging is verbose
// enough that leaving it on is its own small denial of service. It also raises
// the volume of anything that logs peer names, workload names or IPs, so it
// should not be peer-callable.

// apiGetLogLevel godoc
// @Summary Get the current log level
// @Description Returns the level this node is logging at right now, which may differ from JETTY_LOG_LEVEL if it was changed at runtime.
// @Tags cluster
// @Produce json
// @Success 200 {object} map[string]string
// @Router /log-level [get]
func (a *Agent) apiGetLogLevel(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{
		"level": CurrentLogLevel(),
		"node":  a.hostname,
	})
}

// apiSetLogLevel godoc
// @Summary Change the log level without restarting
// @Description Sets the log level on this node immediately. Use `debug` to investigate a live problem without restarting and losing the state you are investigating; `debug` also turns on source file:line. Reverts to JETTY_LOG_LEVEL on restart, so a forgotten debug setting cannot persist silently. Admin key required.
// @Tags cluster
// @Accept json
// @Produce json
// @Param level body map[string]string true "e.g. {\"level\":\"debug\"}"
// @Success 200 {object} map[string]string
// @Failure 400 {object} ErrorResponse "Unrecognised level"
// @Failure 401 {object} ErrorResponse "Admin key required"
// @Router /log-level [post]
func (a *Agent) apiSetLogLevel(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthorize(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized: admin key required to change the log level")
		return
	}

	var req struct {
		Level string `json:"level"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Level == "" {
		writeError(w, http.StatusBadRequest, `level is required, e.g. {"level":"debug"}`)
		return
	}

	// Reject rather than silently falling back. parseLogLevel defaults unknown
	// values to info so a typo in an env var cannot stop a node booting, but
	// an operator typing a level into an API deserves to be told they got it
	// wrong instead of quietly getting info.
	if !isKnownLogLevel(req.Level) {
		writeError(w, http.StatusBadRequest,
			"unrecognised level "+req.Level+"; valid values are debug, info, warn, error")
		return
	}

	previous := CurrentLogLevel()
	applied := SetLogLevel(req.Level)

	// Log at warn so this is visible even to someone who just turned the level
	// *down* - a level change is an operational event and should not be
	// invisible at the level it selected.
	logWarnf("Log level changed at runtime: %s -> %s", previous, applied.String())

	writeJSON(w, map[string]string{
		"level":    applied.String(),
		"previous": previous,
		"node":     a.hostname,
		"note":     "reverts to JETTY_LOG_LEVEL on restart",
	})
}

// isKnownLogLevel reports whether name is a level we recognise, as distinct
// from parseLogLevel which deliberately falls back for env-var input.
func isKnownLogLevel(name string) bool {
	switch normalizeLogLevel(name) {
	case "debug", "info", "warn", "warning", "error":
		return true
	}
	return false
}
