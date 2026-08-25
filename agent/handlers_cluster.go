package agent

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// HTTP Handlers - cluster status & internal sync endpoints
// =============================================================================
//
// Public:
//   GET  /api/status          aggregate cluster status (peers + workloads + tunnel)
//   GET  /api/sync            full state dump for peer pull
//
// Internal (allowlisted in apiKeyMiddleware - validated in-band):
//   POST /api/peer-announce   peer says "here is my current address/state"
//   POST /api/heartbeat       tunnel-mode liveness ping
//   POST /api/tunnel/sync     bootstrap tunnel token broadcast (CFToken init)
//
// broadcastTunnelToken is a helper that fan-outs CFToken changes to peers.

// apiStatus godoc
// @Summary Get cluster status
// @Description Returns full cluster status including node info, peers, workloads, and connectivity status
// @Tags cluster
// @Produce json
// @Success 200 {object} StatusResponse
// @Router /status [get]
func (a *Agent) apiStatus(w http.ResponseWriter, r *http.Request) {
	a.stateMu.RLock()
	peers := make([]*Peer, 0, len(a.state.Peers))
	for _, p := range a.state.Peers {
		peers = append(peers, p)
	}

	// Build peer info maps for enrichment
	peerIDToInfo := make(map[string]map[string]string)
	peersByID := make(map[string]*Peer)
	for _, p := range a.state.Peers {
		peerIDToInfo[p.ID] = map[string]string{
			"id":   p.ID,
			"name": p.Name,
			"ip":   p.IP,
		}
		peersByID[p.ID] = p
	}
	// Add local node to owner info map
	peerIDToInfo[a.hwid] = map[string]string{
		"id":   a.hwid,
		"name": a.hostname,
		"ip":   a.ip,
	}

	// Build enriched workloads with owner info and status
	type EnrichedWorkload struct {
		Name         string            `json:"name"`
		IP           string            `json:"ip"`
		Compose      string            `json:"compose"`
		Revive       bool              `json:"revive"`
		Autostart    bool              `json:"autostart"`
		AllowedNodes []string          `json:"allowed_nodes,omitempty"`
		Tags         []string          `json:"tags,omitempty"`
		Owner        map[string]string `json:"owner"`
		Version      int64             `json:"version"`
		Status       string            `json:"status"`
	}

	// Collect workload data - we'll fetch remote statuses after releasing the lock
	type workloadData struct {
		wl        *Workload
		ownerInfo map[string]string
		ownerPeer *Peer
		isLocal   bool
	}
	workloadInfos := make([]workloadData, 0, len(a.state.Workloads))
	for _, wl := range a.state.Workloads {
		ownerInfo := peerIDToInfo[wl.Owner]
		if ownerInfo == nil {
			ownerInfo = map[string]string{"id": wl.Owner, "name": "unknown", "ip": "unknown"}
		}
		isLocal := wl.Owner == a.hwid
		var ownerPeer *Peer
		if !isLocal {
			ownerPeer = peersByID[wl.Owner]
		}
		workloadInfos = append(workloadInfos, workloadData{
			wl:        wl,
			ownerInfo: ownerInfo,
			ownerPeer: ownerPeer,
			isLocal:   isLocal,
		})
	}

	hasTunnel := a.state.CFToken != ""
	a.stateMu.RUnlock()

	// Fetch remote workload statuses in parallel with 2 second timeout
	statusClient := &http.Client{Timeout: 2 * time.Second}
	statuses := make(map[string]string)
	var statusMu sync.Mutex
	var wg sync.WaitGroup

	for _, info := range workloadInfos {
		if info.isLocal {
			// Rich status: running/unhealthy/starting/restarting/stopped/unknown.
			// See computeWorkloadStatus in handlers_workloads.go.
			statuses[info.wl.Name] = a.computeWorkloadStatus(info.wl.Name)
		} else if info.ownerPeer != nil && info.ownerPeer.Healthy {
			wg.Add(1)
			go func(wl *Workload, peer *Peer) {
				defer wg.Done()
				status := "remote" // Default fallback

				url := a.getPeerAPIURL(peer, "/api/workloads/"+wl.Name)
				if url != "" {
					req, err := a.peerRequest("GET", url, nil)
					if err == nil {
						resp, err := statusClient.Do(req)
						if err == nil {
							defer resp.Body.Close()
							if resp.StatusCode == 200 {
								var data map[string]interface{}
								if json.NewDecoder(resp.Body).Decode(&data) == nil {
									// Check containers array for running status
									if containers, ok := data["containers"].([]interface{}); ok && len(containers) > 0 {
										hasRunning := false
										for _, c := range containers {
											if cm, ok := c.(map[string]interface{}); ok {
												if running, ok := cm["running"].(bool); ok && running {
													hasRunning = true
													break
												}
											}
										}
										if hasRunning {
											status = "running"
										} else {
											status = "stopped"
										}
									} else {
										status = "stopped"
									}
								}
							}
						}
					}
				}

				statusMu.Lock()
				statuses[wl.Name] = status
				statusMu.Unlock()
			}(info.wl, info.ownerPeer)
		} else {
			statuses[info.wl.Name] = "remote"
		}
	}
	wg.Wait()

	// Build final workloads list
	workloads := make([]EnrichedWorkload, 0, len(workloadInfos))
	for _, info := range workloadInfos {
		workloads = append(workloads, EnrichedWorkload{
			Name:         info.wl.Name,
			IP:           info.wl.IP,
			Compose:      info.wl.Compose,
			Revive:       info.wl.Revive,
			Autostart:    info.wl.Autostart,
			AllowedNodes: info.wl.AllowedNodes,
			Tags:         info.wl.Tags,
			Owner:        info.ownerInfo,
			Version:      info.wl.Version,
			Status:       statuses[info.wl.Name],
		})
	}

	resp := map[string]interface{}{
		"node": map[string]interface{}{
			"id":        a.hwid,
			"name":      a.hostname,
			"ip":        a.ip,
			"arch":      runtime.GOARCH,
			"version":   Version,
			"healthy":   true, // Self is always healthy if we're responding
			"last_seen": time.Now(),
			"is_self":   true,
		},
		"peers":        peers,
		"workloads":    workloads,
		"service_cidr": a.serviceCIDR,
		"tunnel": map[string]interface{}{
			"configured": hasTunnel,
			"running":    a.isTunnelRunning(),
			// The cluster-wide hostname, exposed so the dashboard can
			// prefill the join URL in a generated docker run. Without it
			// the UI can only offer whatever address the browser happens
			// to be using - a LAN IP when you are on the LAN, which
			// produces a join command that cannot work from anywhere else.
			// Not a secret: it is the public name of the cluster.
			"domain": a.tunnelDomain,
		},
		// a.ip is non-empty whenever the CloudflareWARP interface has
		// an IPv4 lease (see detectWarpIP). That's the only state that
		// matters for the dashboard's "WARP Mesh" indicator - if we
		// have a WARP IP, the mesh is up.
		"warp": map[string]interface{}{
			"enabled": a.ip != "",
			"ip":      a.ip,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// apiSync godoc
// @Summary Full replicated state dump (internal)
// @Description Node-to-node pull used as the backstop when memberlist gossip is unavailable. Returns every workload, deletion tombstone, env value and env tombstone so a peer can merge by highest-version-wins. Peer API key required.
// @Tags internal
// @Produce json
// @Success 200 {object} SyncResponse
// @Router /sync [get]
func (a *Agent) apiSync(w http.ResponseWriter, r *http.Request) {
	// Return all workloads, deleted workloads (tombstones), and env data for sync
	// This allows other nodes to get a complete view of cluster state including deletions
	a.stateMu.RLock()
	workloads := make([]*Workload, 0, len(a.state.Workloads))
	for _, wl := range a.state.Workloads {
		workloads = append(workloads, wl)
	}
	deletedWorkloads := make([]*DeletedWorkload, 0, len(a.state.DeletedWorkloads))
	for _, dw := range a.state.DeletedWorkloads {
		deletedWorkloads = append(deletedWorkloads, dw)
	}
	envData := make(map[string]string)
	for k, v := range a.state.EnvData {
		envData[k] = v
	}
	deletedEnvKeys := make([]*DeletedEnvKey, 0, len(a.state.DeletedEnvKeys))
	for _, dek := range a.state.DeletedEnvKeys {
		deletedEnvKeys = append(deletedEnvKeys, dek)
	}
	removedPeers := a.removedPeersSlice()
	a.stateMu.RUnlock()

	// Return sync response with workloads, tombstones, and env data
	resp := map[string]interface{}{
		"workloads": workloads,
	}
	if len(deletedWorkloads) > 0 {
		resp["deleted_workloads"] = deletedWorkloads
	}
	if len(envData) > 0 {
		resp["env_data"] = envData
	}
	if len(deletedEnvKeys) > 0 {
		resp["deleted_env_keys"] = deletedEnvKeys
	}
	if len(removedPeers) > 0 {
		resp["removed_peers"] = removedPeers
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// apiPeerAnnounce godoc
// @Summary Announce a peer's current address and state (internal)
// @Description Node-to-node. A peer reports its identity, WARP IP, version and architecture so the receiver can add or refresh it in the peer table. Peer API key required.
// @Tags internal
// @Accept json
// @Produce json
// @Success 200 {object} map[string]string
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Router /peer-announce [post]
func (a *Agent) apiPeerAnnounce(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Peer Peer `json:"peer"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Auth is enforced by apiKeyMiddleware; the caller's X-API-Key
	// already matched AdminKey, SelfAPIKey, or a registered peer key.

	// Don't add ourselves as a peer
	if req.Peer.ID == a.hwid {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ignored", "reason": "self"})
		return
	}

	// Reject malformed peer fields. Peer.Name and Peer.IP both flow into
	// /etc/hosts; an unvalidated newline/tab would let a (compromised)
	// peer inject arbitrary host -> IP mappings on every node.
	if !validIngestedPeer(&req.Peer) {
		writeError(w, http.StatusBadRequest, "invalid peer fields")
		return
	}

	req.Peer.Healthy = true
	req.Peer.LastSeen = time.Now()

	// SECURITY: never let apiPeerAnnounce mutate APIKey. The APIKey is
	// established once during /api/join (under a one-time token) and
	// any later "announcement" carrying an APIKey would let any peer
	// rewrite another peer's credential, or let a stale self-announce
	// from announceOurIP wipe the stored value. Force-clear it from
	// the request body and merge from the existing entry below.
	req.Peer.APIKey = ""

	// Refuse to repoint an existing peer's IP via this endpoint.
	a.stateMu.Lock()
	// A removed node announces itself like any other - it is still running.
	// Without this it is back in the peer table on the next announce.
	if a.peerRemovedLocked(req.Peer.ID) {
		a.stateMu.Unlock()
		logWarnf("Rejecting announce from removed node %s (%s); "+
			"issue a fresh join token to re-admit it", req.Peer.Name, shortID(req.Peer.ID, 12))
		writeError(w, http.StatusForbidden, "node has been removed from this cluster")
		return
	}
	oldPeer := a.state.Peers[req.Peer.ID]
	oldIP := ""
	oldAPIKey := ""
	if oldPeer != nil {
		oldIP = oldPeer.IP
		oldAPIKey = oldPeer.APIKey
	}
	if oldPeer != nil && oldIP != "" && req.Peer.IP != "" && oldIP != req.Peer.IP {
		remoteHost, _, _ := net.SplitHostPort(r.RemoteAddr)
		if remoteHost != req.Peer.IP {
			a.stateMu.Unlock()
			logInfof("Refusing peer-announce IP change for %s: %s -> %s (RemoteAddr=%s does not match new IP)",
				req.Peer.ID, oldIP, req.Peer.IP, remoteHost)
			writeError(w, http.StatusForbidden, "peer IP change must come from the new IP")
			return
		}
	}
	// Preserve the existing APIKey so the merge doesn't blank it out.
	req.Peer.APIKey = oldAPIKey
	// Same for version/arch: older agents announce without them, and a
	// wholesale store would erase what we learned at join time.
	if oldPeer != nil {
		if req.Peer.Version == "" {
			req.Peer.Version = oldPeer.Version
		}
		if req.Peer.Arch == "" {
			req.Peer.Arch = oldPeer.Arch
		}
	}
	ipChanged := oldIP != "" && oldIP != req.Peer.IP
	a.state.Peers[req.Peer.ID] = &req.Peer
	a.stateMu.Unlock()

	a.updateHosts()
	a.saveState()

	// Always ensure we have an IPIP tunnel to this peer (for receiving their traffic)
	// IPIP requires both sides to have matching tunnels
	if req.Peer.IP != "" {
		if err := a.ensurePeerTunnel(req.Peer.ID, req.Peer.IP); err != nil {
			logWarnf("failed to create tunnel to %s: %v", req.Peer.Name, err)
		}
	}

	// If peer IP changed, also update workload routes
	if ipChanged {
		logInfof("Peer %s IP changed: %s -> %s", req.Peer.Name, oldIP, req.Peer.IP)
		a.stateMu.Lock()
		a.triggerRouteReconcile()
		a.stateMu.Unlock()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

	logInfof("Peer announced: %s (%s)", req.Peer.Name, req.Peer.IP)
}

// apiHeartbeat receives heartbeats from peers in tunnel-only mode.
// This allows peers to track each other's health through the Cloudflare tunnel.
// apiHeartbeat godoc
// @Summary Peer liveness ping (internal)
// @Description Node-to-node liveness used in tunnel mode, where peers cannot reach each other directly over WARP. Refreshes the sender's LastSeen. Peer API key required.
// @Tags internal
// @Accept json
// @Produce json
// @Success 200 {object} map[string]string
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Router /heartbeat [post]
func (a *Agent) apiHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Auth is enforced by apiKeyMiddleware.

	// Ignore our own heartbeat (can happen when tunnel routes back to us)
	if req.ID == a.hwid {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "self"})
		return
	}

	// Update peer's LastSeen
	a.stateMu.Lock()
	if peer, ok := a.state.Peers[req.ID]; ok {
		peer.LastSeen = time.Now()
		peer.Healthy = true
	}
	a.stateMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "received_by": a.hostname})
}

// broadcastTunnelToken sends the CF token to all peers so they can start their tunnels.
// Sends X-API-Key via peerRequest so the receiver's apiKeyMiddleware admits it.
func (a *Agent) broadcastTunnelToken(token string) {
	data, _ := json.Marshal(map[string]string{"token": token})

	// In tunnel-only mode, broadcast to tunnel (one node receives, gossip propagates)
	if a.tunnelDomain != "" {
		url := a.getTunnelAPIURL("/api/tunnel/sync")
		req, err := a.peerRequest("POST", url, strings.NewReader(string(data)))
		if err != nil {
			logErrorf("Failed to build tunnel-token broadcast: %v", err)
			return
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			logErrorf("Failed to broadcast tunnel token: %v", err)
		} else {
			resp.Body.Close()
		}
		return
	}

	// Direct mode: send to each peer
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	a.stateMu.RUnlock()

	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/tunnel/sync", peer.IP, a.apiPort)
		req, err := a.peerRequest("POST", url, strings.NewReader(string(data)))
		if err != nil {
			logErrorf("Failed to build tunnel-token broadcast to %s: %v", peer.Name, err)
			continue
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			logErrorf("Failed to broadcast tunnel token to %s: %v", peer.Name, err)
			continue
		}
		resp.Body.Close()
	}
}

// apiTunnelSync godoc
// @Summary Receive a tunnel token broadcast (internal)
// @Description Node-to-node. Accepts the cluster CFToken from a peer and starts or stops cloudflared to match. A node that has opted out via DELETE /tunnel?scope=node accepts the token but stays stopped. Peer API key required.
// @Tags internal
// @Accept json
// @Produce json
// @Param token body TunnelRequest true "Cluster tunnel token; empty string tears the tunnel down"
// @Success 200 {object} map[string]string
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Router /tunnel/sync [post]
func (a *Agent) apiTunnelSync(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	a.stateMu.Lock()
	oldToken := a.state.CFToken
	a.state.CFToken = req.Token
	a.stateMu.Unlock()

	// Only restart if token changed
	if oldToken != req.Token {
		a.saveState()
		if req.Token == "" {
			a.stopCloudflared()
			logInfof("Cloudflare tunnel removed via sync")
		} else {
			a.stateMu.RLock()
			disabled := a.state.CFTunnelDisabled
			a.stateMu.RUnlock()
			if disabled {
				// The token is cluster state and we accept it, but a node
				// that opted out stays out. startCloudflared enforces this
				// too; short-circuiting here keeps the log honest.
				logInfof("Cloudflare tunnel token synced but this node is disabled; not starting")
			} else if err := a.restartCloudflared(); err != nil {
				logErrorf("Failed to start tunnel after sync: %v", err)
			} else {
				logInfof("Cloudflare tunnel started via sync")
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
