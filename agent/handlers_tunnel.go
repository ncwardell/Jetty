package agent

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
)

// =============================================================================
// HTTP Handlers - Cloudflare Tunnel (cloudflared) configuration
// =============================================================================
//
//   GET    /api/tunnel    status (configured? running? enabled here?)
//   POST   /api/tunnel    set the token (cluster) or re-enable this node
//   DELETE /api/tunnel    detach this node (default) or drop the token cluster-wide
//
// Two axes, deliberately separate:
//
//   CFToken           cluster-wide, shared, broadcast to peers
//   CFTunnelDisabled  node-local, never broadcast
//
// Every node running a connector on the same Cloudflare tunnel means
// Cloudflare load-balances public traffic across all of them. Detaching one
// misbehaving node must not disturb the others, so ?scope defaults to "node".
// The cluster-wide teardown is still available, but you have to ask for it.
//
// Both mutating endpoints accept ?node=<id|name> to target a peer, so an
// operator can detach a node from a dashboard served by any node - which
// matters because the dashboard is reached through the shared tunnel and
// lands on whichever node Cloudflare picks.
//
// The cloudflared subprocess lifecycle lives in agent/cloudflared.go.

// tunnelScope reads ?scope, defaulting to node-local. Returns ok=false and
// writes the error when the value is unrecognised - a typo'd scope must not
// silently fall through to the more destructive branch.
func tunnelScope(w http.ResponseWriter, r *http.Request) (scope string, ok bool) {
	switch s := r.URL.Query().Get("scope"); s {
	case "", "node":
		return "node", true
	case "cluster":
		return "cluster", true
	default:
		writeError(w, http.StatusBadRequest, "scope must be \"node\" or \"cluster\", got: "+s)
		return "", false
	}
}

// apiGetTunnel godoc
// @Summary Get tunnel status
// @Description Returns Cloudflare tunnel status. "configured" is cluster-wide (a token is set); "enabled" and "running" are local to this node. Pass ?node=<id|name> to query a peer.
// @Tags tunnel
// @Produce json
// @Param node query string false "Target node id or name"
// @Success 200 {object} TunnelStatus
// @Router /tunnel [get]
func (a *Agent) apiGetTunnel(w http.ResponseWriter, r *http.Request) {
	if a.proxyTunnelRequestIfRemote(w, r) {
		return
	}

	a.stateMu.RLock()
	hasToken := a.state.CFToken != ""
	disabled := a.state.CFTunnelDisabled
	a.stateMu.RUnlock()

	writeJSON(w, map[string]interface{}{
		"configured": hasToken,
		"enabled":    !disabled,
		"running":    a.isTunnelRunning(),
		"node":       a.hostname,
	})
}

// apiSetTunnel godoc
// @Summary Configure tunnel or re-enable this node
// @Description With scope=cluster (or a body containing a token), sets the Cloudflare tunnel token and propagates it to all nodes. With the default scope=node and no token, re-attaches this node to the existing cluster token after a scope=node delete.
// @Tags tunnel
// @Accept json
// @Produce json
// @Param scope query string false "node (default) or cluster"
// @Param node query string false "Target node id or name"
// @Param token body TunnelRequest false "Tunnel token (required for scope=cluster)"
// @Success 200 {object} TunnelStatus
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Router /tunnel [post]
func (a *Agent) apiSetTunnel(w http.ResponseWriter, r *http.Request) {
	if a.proxyTunnelRequestIfRemote(w, r) {
		return
	}
	scope, ok := tunnelScope(w, r)
	if !ok {
		return
	}

	var req struct {
		Token string `json:"token"`
	}
	// An empty body is valid for the re-enable case.
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Setting a token is inherently a cluster operation - it is shared state.
	if req.Token != "" {
		scope = "cluster"
	} else if scope == "cluster" {
		writeError(w, http.StatusBadRequest, "token required for scope=cluster")
		return
	}

	a.stateMu.Lock()
	if req.Token != "" {
		a.state.CFToken = req.Token
	}
	// Either operation re-attaches this node: an explicit re-enable, or an
	// operator setting a fresh token after having detached.
	a.state.CFTunnelDisabled = false
	hasToken := a.state.CFToken != ""
	a.stateMu.Unlock()

	a.saveState()

	if !hasToken {
		writeError(w, http.StatusBadRequest, "no tunnel token configured for this cluster")
		return
	}

	if err := a.restartCloudflared(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if scope == "cluster" {
		a.broadcastTunnelToken(req.Token)
		log.Printf("Cloudflare tunnel configured (cluster-wide)")
	} else {
		log.Printf("Cloudflare tunnel re-enabled on this node")
	}

	writeJSON(w, map[string]interface{}{
		"configured": true,
		"enabled":    true,
		"running":    a.isTunnelRunning(),
		"scope":      scope,
		"node":       a.hostname,
	})
}

// apiDeleteTunnel godoc
// @Summary Detach a node from the tunnel, or remove it cluster-wide
// @Description Default scope=node stops this node's cloudflared connector and leaves the cluster token and every other node untouched - use this to pull one node out of Cloudflare's load-balancing rotation. scope=cluster clears the token everywhere and stops every connector, taking public traffic down until a token is set again.
// @Tags tunnel
// @Produce json
// @Param scope query string false "node (default) or cluster"
// @Param node query string false "Target node id or name"
// @Success 200 {object} TunnelStatus
// @Failure 400 {object} ErrorResponse "Invalid scope"
// @Router /tunnel [delete]
func (a *Agent) apiDeleteTunnel(w http.ResponseWriter, r *http.Request) {
	if a.proxyTunnelRequestIfRemote(w, r) {
		return
	}
	scope, ok := tunnelScope(w, r)
	if !ok {
		return
	}

	a.stateMu.Lock()
	if scope == "cluster" {
		a.state.CFToken = ""
	} else {
		a.state.CFTunnelDisabled = true
	}
	hasToken := a.state.CFToken != ""
	a.stateMu.Unlock()

	a.stopCloudflared()
	a.saveState()

	if scope == "cluster" {
		a.broadcastTunnelToken("")
		log.Printf("Cloudflare tunnel removed cluster-wide")
	} else {
		log.Printf("Cloudflare tunnel disabled on this node (cluster token retained)")
	}

	writeJSON(w, map[string]interface{}{
		"configured": hasToken,
		"enabled":    false,
		"running":    false,
		"scope":      scope,
		"node":       a.hostname,
	})
}

// proxyTunnelRequestIfRemote forwards a /api/tunnel request to a peer when
// ?node=<id|name> names one. Returns true when the request was handled here
// (proxied, or failed to proxy) and the caller should stop.
//
// Same shape as proxyHostRequestIfRemote, but preserves the method, body, and
// ?scope. The forwarded URL deliberately drops ?node= so the peer treats it as
// local and cannot bounce it onward.
func (a *Agent) proxyTunnelRequestIfRemote(w http.ResponseWriter, r *http.Request) bool {
	nodeID := r.URL.Query().Get("node")
	if nodeID == "" || nodeID == "self" || nodeID == a.hwid || nodeID == a.hostname {
		return false
	}

	a.stateMu.RLock()
	var target *Peer
	for _, p := range a.state.Peers {
		if p.ID == nodeID || p.Name == nodeID {
			target = p
			break
		}
	}
	a.stateMu.RUnlock()
	if target == nil {
		writeError(w, http.StatusNotFound, "node not found: "+nodeID)
		return true
	}

	path := "/api/tunnel"
	if scope := r.URL.Query().Get("scope"); scope != "" {
		path += "?scope=" + scope
	}
	url := a.getPeerAPIURL(target, path)
	if url == "" {
		writeError(w, http.StatusBadGateway, "no route to node: "+nodeID)
		return true
	}

	req, err := a.peerRequest(r.Method, url, r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "build proxy request: "+err.Error())
		return true
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, "reach node: "+err.Error())
		return true
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	return true
}
