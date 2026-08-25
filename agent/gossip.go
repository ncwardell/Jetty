package agent

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"time"
)

// =============================================================================
// Gossip
// =============================================================================

func (a *Agent) gossipLoop() {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

	// Run tombstone GC every 10 minutes (every 60th tick)
	gcCounter := 0

	for {
		select {
		case <-a.stopCh:
			return
		case <-tick.C:
			a.checkPeers()
			a.syncWorkloads()

			// Garbage collect old tombstones periodically
			gcCounter++
			if gcCounter >= 60 { // Every 10 minutes (60 * 10 seconds)
				a.gcTombstones()
				gcCounter = 0
			}
		}
	}
}

// gcTombstones removes tombstones older than 1 hour.
// This prevents unbounded growth of the tombstone map while ensuring
// deletions have enough time to propagate across all nodes.
func (a *Agent) gcTombstones() {
	cutoff := time.Now().UnixNano() - int64(time.Hour) // 1 hour ago

	a.stateMu.Lock()
	removedWorkloads := 0
	for ip, dw := range a.state.DeletedWorkloads {
		if dw.Version < cutoff {
			delete(a.state.DeletedWorkloads, ip)
			removedWorkloads++
		}
	}
	// Node tombstones expire on their own, much longer horizon - a removed
	// node can legitimately be powered off for weeks.
	a.gcRemovedPeers()
	removedEnvKeys := 0
	for key, dek := range a.state.DeletedEnvKeys {
		if dek.Version < cutoff {
			delete(a.state.DeletedEnvKeys, key)
			removedEnvKeys++
		}
	}
	a.stateMu.Unlock()

	if removedWorkloads > 0 || removedEnvKeys > 0 {
		logInfof("GC: removed %d workload tombstones, %d env key tombstones", removedWorkloads, removedEnvKeys)
		a.saveState()
	}
}

func (a *Agent) checkPeers() {
	// In tunnel-only mode, send heartbeat through tunnel and check peer staleness
	if a.tunnelDomain != "" {
		a.tunnelModeHealthCheck()
		return
	}

	// Direct mode: check each peer individually via mesh IP
	// Copy peer info outside the lock to avoid holding mutex during network calls
	type peerCheck struct {
		id string
		ip string
	}
	a.stateMu.RLock()
	peers := make([]peerCheck, 0, len(a.state.Peers))
	for _, p := range a.state.Peers {
		peers = append(peers, peerCheck{id: p.ID, ip: p.IP})
	}
	a.stateMu.RUnlock()

	// Check each peer without holding the mutex
	type peerResult struct {
		id      string
		healthy bool
	}
	results := make([]peerResult, 0, len(peers))

	for _, peer := range peers {
		// Use ?node=local to get only the peer's local health, avoiding recursive peer queries.
		// peerClient (5s) instead of httpClient (30s) so a stale TCP connection
		// from before a network blip costs at most one tick to expire instead of
		// blocking checkPeers for half a minute.
		url := fmt.Sprintf("http://%s:%d/api/health?node=local", peer.ip, a.apiPort)
		resp, err := peerClient.Get(url)

		result := peerResult{id: peer.id, healthy: false}
		if err == nil {
			result.healthy = resp.StatusCode == 200
			resp.Body.Close()
		}
		results = append(results, result)
	}

	// Update peer status under the lock
	a.stateMu.Lock()
	defer a.stateMu.Unlock()

	now := time.Now()
	for _, result := range results {
		if peer, ok := a.state.Peers[result.id]; ok {
			peer.Healthy = result.healthy
			if result.healthy {
				peer.LastSeen = now
			}
		}
	}
}

// tunnelModeHealthCheck handles peer health in tunnel-only mode.
// Since we can't directly reach peers, we:
// 1. Send our heartbeat through the tunnel (any node receiving it updates our LastSeen)
// 2. Check peer staleness based on LastSeen
func (a *Agent) tunnelModeHealthCheck() {
	// Send our heartbeat through the tunnel. Auth via X-API-Key
	// (peerRequest sets it from SelfAPIKey).
	heartbeat := map[string]interface{}{
		"id":   a.hwid,
		"name": a.hostname,
	}
	data, _ := json.Marshal(heartbeat)

	url := a.getTunnelAPIURL("/api/heartbeat")
	req, err := a.peerRequest("POST", url, strings.NewReader(string(data)))
	if err == nil {
		resp, err := httpClient.Do(req)
		if err != nil {
			if time.Since(a.lastHeartbeatErrLog) > time.Minute {
				logErrorf("Heartbeat failed: %v (suppressing further errors for 1 min)", err)
				a.lastHeartbeatErrLog = time.Now()
			}
		} else {
			resp.Body.Close()
		}
	}

	// Check peer health based on LastSeen staleness
	a.stateMu.Lock()

	staleThreshold := HealthTimeout // Backstop for HTTP-gossip mode
	now := time.Now()
	healthChanged := false

	for _, peer := range a.state.Peers {
		wasHealthy := peer.Healthy
		if now.Sub(peer.LastSeen) > staleThreshold {
			if peer.Healthy {
				logInfof("Peer %s marked unhealthy (no heartbeat for %v)", peer.Name, now.Sub(peer.LastSeen))
			}
			peer.Healthy = false
		} else {
			peer.Healthy = true
		}
		if wasHealthy != peer.Healthy {
			healthChanged = true
		}
	}

	// Update routes if any peer health status changed
	// This ensures routes to unhealthy peers are removed immediately
	if healthChanged {
		a.triggerRouteReconcile()
	}
	a.stateMu.Unlock()
}

func (a *Agent) announcePeer(newPeer *Peer) {
	// Body now carries only the peer record - auth flows via the
	// X-API-Key header (peerRequest sets it from SelfAPIKey).
	announcement := struct {
		Peer *Peer `json:"peer"`
	}{Peer: newPeer}
	data, _ := json.Marshal(announcement)

	// Track if any direct announcements succeeded
	directSuccess := 0

	// Try direct peer IPs first (if we have peers with IPs)
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		if p.ID != newPeer.ID && p.IP != "" {
			peers = append(peers, p)
		}
	}
	a.stateMu.RUnlock()

	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/peer-announce", peer.IP, a.apiPort)
		req, err := a.peerRequest("POST", url, strings.NewReader(string(data)))
		if err != nil {
			logErrorf("Failed to build announce request to %s: %v", peer.Name, err)
			continue
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			logErrorf("Failed to announce peer to %s via direct IP: %v", peer.Name, err)
			continue
		}
		resp.Body.Close()
		directSuccess++
	}

	// Use tunnel as fallback if direct failed or no peers had IPs
	if directSuccess == 0 && a.tunnelDomain != "" {
		url := a.getTunnelAPIURL("/api/peer-announce")
		req, err := a.peerRequest("POST", url, strings.NewReader(string(data)))
		if err != nil {
			logErrorf("Failed to build announce request via tunnel: %v", err)
			return
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			logErrorf("Failed to announce peer via tunnel: %v", err)
		} else {
			resp.Body.Close()
		}
	}
}

// announceOurIP sends our current IP to all known peers.
// Called after WARP connects so peers can update our address.
func (a *Agent) announceOurIP() {
	if a.warpIP() == "" {
		return
	}

	self := &Peer{
		ID:       a.hwid,
		Name:     a.hostname,
		IP:       a.warpIP(),
		Healthy:  true,
		LastSeen: time.Now(),
		// Carry version/arch: apiPeerAnnounce stores the announced
		// record wholesale, so omitting these would blank them
		// cluster-wide on every IP announcement.
		Version: Version,
		Arch:    runtime.GOARCH,
	}

	logInfof("Announcing our IP (%s) to cluster...", a.warpIP())
	a.announcePeer(self)
}
