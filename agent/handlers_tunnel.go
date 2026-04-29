package agent

import (
	"encoding/json"
	"log"
	"net/http"
)

// =============================================================================
// HTTP Handlers - Cloudflare Tunnel (cloudflared) configuration
// =============================================================================
//
//   GET    /api/tunnel    status (configured? running?)
//   POST   /api/tunnel    set the tunnel token; broadcasts to peers
//   DELETE /api/tunnel    drop the token and stop cloudflared
//
// The actual cloudflared subprocess lifecycle lives in agent/cloudflared.go.

// apiGetTunnel godoc
// @Summary Get tunnel status
// @Description Returns Cloudflare tunnel configuration status
// @Tags tunnel
// @Produce json
// @Success 200 {object} TunnelStatus
// @Router /tunnel [get]
func (a *Agent) apiGetTunnel(w http.ResponseWriter, r *http.Request) {
	a.stateMu.RLock()
	hasToken := a.state.CFToken != ""
	a.stateMu.RUnlock()

	resp := map[string]interface{}{
		"configured": hasToken,
		"running":    a.isTunnelRunning(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
// apiSetTunnel godoc
// @Summary Configure tunnel
// @Description Sets the Cloudflare tunnel token (propagates to all nodes)
// @Tags tunnel
// @Accept json
// @Param token body TunnelRequest true "Tunnel token"
// @Success 200 "Tunnel configured"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Router /tunnel [post]
func (a *Agent) apiSetTunnel(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	if req.Token == "" {
		http.Error(w, "token required", 400)
		return
	}

	a.stateMu.Lock()
	a.state.CFToken = req.Token
	a.stateMu.Unlock()

	a.saveState()

	// Start or restart the tunnel
	if err := a.restartCloudflared(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Broadcast to peers so they also start their tunnels
	a.broadcastTunnelToken(req.Token)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"configured": true,
		"running":    a.isTunnelRunning(),
	})

	log.Printf("Cloudflare tunnel configured")
}
// apiDeleteTunnel godoc
// @Summary Remove tunnel
// @Description Removes the Cloudflare tunnel configuration
// @Tags tunnel
// @Success 204 "Tunnel removed"
// @Router /tunnel [delete]
func (a *Agent) apiDeleteTunnel(w http.ResponseWriter, r *http.Request) {
	a.stateMu.Lock()
	a.state.CFToken = ""
	a.stateMu.Unlock()

	a.stopCloudflared()
	a.saveState()

	// Notify peers to stop their tunnels
	a.broadcastTunnelToken("")

	w.WriteHeader(204)
	log.Printf("Cloudflare tunnel removed")
}
