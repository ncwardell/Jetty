package agent

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// validIngestedWorkload checks that a workload received from a peer has a
// safe name and a parseable IP. Returning false here means we drop the
// entry entirely - a peer who has the cluster secret should not be able to
// inject a name like "../../etc/cron.d/x" (which would land outside the
// composeDir when deployWorkload writes the compose file) or one with
// embedded newlines (which inject extra lines into /etc/hosts).
//
// Tags are normalized in place if any are present. Invalid tags drop
// the whole workload (a malformed tag is a wire-protocol violation,
// not a per-tag issue).
func validIngestedWorkload(w *Workload) bool {
	if w == nil {
		return false
	}
	if !ValidateWorkloadName(w.Name) {
		logInfof("Sync: rejecting workload with invalid name %q", w.Name)
		return false
	}
	if net.ParseIP(w.IP) == nil {
		logInfof("Sync: rejecting workload %q with invalid IP %q", w.Name, w.IP)
		return false
	}
	if len(w.Tags) > 0 {
		normTags, badTag := normalizeTags(w.Tags)
		if badTag != "" {
			logInfof("Sync: rejecting workload %q with invalid tag %q", w.Name, badTag)
			return false
		}
		w.Tags = normTags
	}
	return true
}

// validIngestedTombstone gates DeletedWorkload entries. Tombstones are
// keyed on IP, so we only need to ensure the IP parses (a malformed IP
// could still poison /etc/hosts via routes.go::updateHosts).
func validIngestedTombstone(dw *DeletedWorkload) bool {
	if dw == nil {
		return false
	}
	if net.ParseIP(dw.IP) == nil {
		logInfof("Sync: rejecting tombstone with invalid IP %q", dw.IP)
		return false
	}
	return true
}

// validIngestedPeer checks that a Peer received from the network has
// safe identity fields. Peer.Name and Peer.IP both flow into /etc/hosts
// via routes.go::updateHosts; if either contains a newline or tab, the
// generated block can be hijacked to inject arbitrary host -> IP
// mappings (which workloads inherit because they run --net host).
func validIngestedPeer(p *Peer) bool {
	if p == nil {
		return false
	}
	if !ValidateWorkloadName(p.ID) {
		logInfof("Sync: rejecting peer with invalid ID %q", p.ID)
		return false
	}
	// Peer.Name is operator-supplied (hostname); allow alphanumerics,
	// dash, underscore, and dot for FQDN-style hostnames.
	if !validPeerNamePattern.MatchString(p.Name) {
		logInfof("Sync: rejecting peer %q with invalid name %q", p.ID, p.Name)
		return false
	}
	// Peer.IP may be empty briefly during bootstrap before WARP attaches.
	if p.IP != "" && net.ParseIP(p.IP) == nil {
		logInfof("Sync: rejecting peer %q with invalid IP %q", p.ID, p.IP)
		return false
	}
	// Version/arch are peer-supplied and rendered by the dashboard -
	// blank them if unsafe rather than rejecting the whole peer.
	sanitizePeerMeta(p.ID, &p.Version, &p.Arch)
	// TunnelHost becomes the authority of an https:// URL we send
	// authenticated requests to, so it gets a stricter charset than the
	// display-only fields above.
	sanitizeTunnelHost(p.ID, &p.TunnelHost)
	return true
}

// =============================================================================
// State Synchronization
// =============================================================================

// MergeResult contains the results of merging sync data
type MergeResult struct {
	LostOwnership []*Workload // Workloads we lost ownership of
	EnvUpdated    bool        // Whether env data or env tombstones were modified
	// Changed is true if ANY local mutation happened during the merge
	// (new tombstones, adopted workloads, retired tombstones, env
	// updates, ownership transfer). Callers that need to persist must
	// check Changed, not just EnvUpdated/LostOwnership - otherwise
	// adopting a peer's new tombstone or workload silently fails to
	// persist and is lost on restart until the next /api/sync round.
	Changed bool
}

// mergeWorkloadState merges incoming sync data with local state.
// This consolidates the duplicated sync logic into a single function.
// Returns workloads we lost ownership of and whether env was updated.
// Must be called with stateMu held (will be released and re-acquired).
func (a *Agent) mergeWorkloadState(syncResp *SyncResponse) *MergeResult {
	result := &MergeResult{}

	// Node removals first. A tombstone we have not seen yet may evict a peer
	// whose workloads appear later in this same payload, and processing it
	// first means those workloads are attributed to a node we already know is
	// gone rather than one we are about to remove.
	if a.mergeRemovedPeers(syncResp.RemovedPeers) {
		result.Changed = true
	}

	// Process deleted workloads (tombstones) first
	for _, dw := range syncResp.DeletedWorkloads {
		if !validIngestedTombstone(dw) {
			continue
		}
		// Merge tombstone if we don't have it or peer's is newer
		existingTombstone := a.state.DeletedWorkloads[dw.IP]
		if existingTombstone == nil || dw.Version > existingTombstone.Version {
			a.state.DeletedWorkloads[dw.IP] = dw
			result.Changed = true
		}
		// Check if we have a local workload that should be deleted
		existing := a.state.Workloads[dw.IP]
		if existing != nil && dw.Version > existing.Version {
			logInfof("Sync: removing workload %s (IP %s) - deleted by peer (tombstone version %d > workload version %d)",
				existing.Name, dw.IP, dw.Version, existing.Version)
			if existing.Owner == a.hwid {
				result.LostOwnership = append(result.LostOwnership, existing)
			}
			delete(a.state.Workloads, dw.IP)
			result.Changed = true
		}
	}

	// Process workloads - only add/update if not tombstoned with a newer version
	for _, w := range syncResp.Workloads {
		if !validIngestedWorkload(w) {
			continue
		}
		// Check if there's a tombstone that supersedes this workload
		tombstone := a.state.DeletedWorkloads[w.IP]
		if tombstone != nil && tombstone.Version > w.Version {
			// Tombstone is newer, ignore this workload
			continue
		}
		existing := a.state.Workloads[w.IP]
		if existing == nil || w.Version > existing.Version {
			// Check if we lost ownership (IP collision resolution)
			if existing != nil && existing.Owner == a.hwid && w.Owner != a.hwid {
				logInfof("Lost ownership of %s (IP %s) to %s - newer version wins", existing.Name, w.IP, shortID(w.Owner, 12))
				result.LostOwnership = append(result.LostOwnership, existing)
			}
			a.state.Workloads[w.IP] = w
			result.Changed = true
			// Clear any older tombstone since we're accepting a newer workload
			if tombstone != nil {
				delete(a.state.DeletedWorkloads, w.IP)
			}
		}
	}

	// Process deleted env keys (tombstones) first
	for _, dek := range syncResp.DeletedEnvKeys {
		if dek == nil || !ValidateEnvKey(dek.Key) {
			continue
		}
		existingTombstone := a.state.DeletedEnvKeys[dek.Key]
		if existingTombstone == nil || dek.Version > existingTombstone.Version {
			a.state.DeletedEnvKeys[dek.Key] = dek
			result.Changed = true
		}
		if _, exists := a.state.EnvData[dek.Key]; exists {
			logInfof("Sync: removing env key %s - deleted by peer", dek.Key)
			delete(a.state.EnvData, dek.Key)
			result.EnvUpdated = true
		}
	}

	// Merge env data from peer.
	//
	// Subtle: an authoritative peer that has env_data[k] AND does NOT
	// have a tombstone for k is telling us "this key is alive". If we
	// still have a local tombstone, ours is stale (e.g. an out-of-order
	// memberlist broadcast left us with the tombstone after the
	// originator already retired it). Trust peer's view, clear our
	// tombstone, accept the value. Without this, a delete-then-re-set
	// race silently re-deletes the value.
	peerTombstoned := make(map[string]bool, len(syncResp.DeletedEnvKeys))
	for _, dek := range syncResp.DeletedEnvKeys {
		if dek != nil {
			peerTombstoned[dek.Key] = true
		}
	}
	for k, v := range syncResp.EnvData {
		if !ValidateEnvKey(k) {
			logInfof("Sync: rejecting env key with invalid name %q", k)
			continue
		}
		if tombstone := a.state.DeletedEnvKeys[k]; tombstone != nil {
			if peerTombstoned[k] {
				// Peer has both the value and a tombstone for it -
				// shouldn't happen in well-formed state, but be safe
				// and trust the tombstone.
				continue
			}
			// Peer has the value, no tombstone - retire ours.
			delete(a.state.DeletedEnvKeys, k)
			result.EnvUpdated = true
		}
		if a.state.EnvData[k] != v {
			a.state.EnvData[k] = v
			result.EnvUpdated = true
		}
	}

	// EnvUpdated and LostOwnership both imply Changed; mirror the flag
	// so callers can rely on Changed alone.
	if result.EnvUpdated || len(result.LostOwnership) > 0 {
		result.Changed = true
	}
	return result
}

// mergeStartupSyncData is similar to mergeWorkloadState but with startup-specific
// logging. Must be called with stateMu held. Returns LostOwnership so the caller
// can stop containers we own locally that the rest of the cluster has reassigned -
// without this, an update or restart that lasted long enough for failover to
// claim our workloads leaves orphan containers running on this node forever
// (the steady-state syncWorkloads path doesn't catch them either, because by
// then state.Workloads[X].Owner already matches the peer's view).
func (a *Agent) mergeStartupSyncData(syncResp *SyncResponse) *MergeResult {
	result := &MergeResult{}

	// Process deleted workloads (tombstones) first
	for _, dw := range syncResp.DeletedWorkloads {
		if !validIngestedTombstone(dw) {
			continue
		}
		existingTombstone := a.state.DeletedWorkloads[dw.IP]
		if existingTombstone == nil || dw.Version > existingTombstone.Version {
			a.state.DeletedWorkloads[dw.IP] = dw
		}
		existing := a.state.Workloads[dw.IP]
		if existing != nil && dw.Version > existing.Version {
			logInfof("Startup sync: workload %s was deleted while we were down", existing.Name)
			if existing.Owner == a.hwid {
				result.LostOwnership = append(result.LostOwnership, existing)
			}
			delete(a.state.Workloads, dw.IP)
		}
	}

	// Process workloads
	for _, w := range syncResp.Workloads {
		if !validIngestedWorkload(w) {
			continue
		}
		tombstone := a.state.DeletedWorkloads[w.IP]
		if tombstone != nil && tombstone.Version > w.Version {
			continue
		}
		existing := a.state.Workloads[w.IP]
		if existing == nil || w.Version > existing.Version {
			if existing != nil && existing.Owner == a.hwid && w.Owner != a.hwid {
				logInfof("Workload %s was revived by %s while we were down", w.Name, shortID(w.Owner, 12))
				result.LostOwnership = append(result.LostOwnership, existing)
			}
			a.state.Workloads[w.IP] = w
			if tombstone != nil {
				delete(a.state.DeletedWorkloads, w.IP)
			}
		}
	}

	// Process deleted env keys (tombstones) first
	for _, dek := range syncResp.DeletedEnvKeys {
		if dek == nil || !ValidateEnvKey(dek.Key) {
			continue
		}
		existingTombstone := a.state.DeletedEnvKeys[dek.Key]
		if existingTombstone == nil || dek.Version > existingTombstone.Version {
			a.state.DeletedEnvKeys[dek.Key] = dek
		}
		if _, exists := a.state.EnvData[dek.Key]; exists {
			logInfof("Startup sync: env key %s was deleted while we were down", dek.Key)
			delete(a.state.EnvData, dek.Key)
		}
	}

	// Merge env data from peer. Same stale-tombstone retirement as
	// mergeWorkloadState: if peer has the value AND no tombstone for
	// it, peer is authoritative; clear our local tombstone instead of
	// letting it silently re-delete the value.
	peerTombstoned := make(map[string]bool, len(syncResp.DeletedEnvKeys))
	for _, dek := range syncResp.DeletedEnvKeys {
		if dek != nil {
			peerTombstoned[dek.Key] = true
		}
	}
	for k, v := range syncResp.EnvData {
		if !ValidateEnvKey(k) {
			logInfof("Startup sync: rejecting env key with invalid name %q", k)
			continue
		}
		if a.state.DeletedEnvKeys[k] != nil {
			if peerTombstoned[k] {
				continue
			}
			delete(a.state.DeletedEnvKeys, k)
		}
		a.state.EnvData[k] = v
	}

	return result
}

// syncStateOnStartup synchronizes state with peers during startup.
// Tries tunnel first, then direct peer connections.
func (a *Agent) syncStateOnStartup() {
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		peers = append(peers, p)
	}
	a.stateMu.RUnlock()

	if len(peers) == 0 && a.tunnelDomain == "" {
		return
	}

	logInfof("Syncing state with cluster on startup...")
	synced := false
	var allLostOwnership []*Workload

	// Try tunnel first (works even if WARP IPs have changed)
	if a.tunnelDomain != "" {
		url := a.getTunnelAPIURL("/api/sync")
		req, err := a.peerRequest("GET", url, nil)
		if err == nil {
			resp, err := httpClient.Do(req)
			if err == nil {
				func() {
					defer resp.Body.Close()
					var syncResp SyncResponse
					if err := json.NewDecoder(resp.Body).Decode(&syncResp); err == nil {
						a.stateMu.Lock()
						result := a.mergeStartupSyncData(&syncResp)
						a.stateMu.Unlock()
						allLostOwnership = append(allLostOwnership, result.LostOwnership...)
						synced = true
					}
				}()
			}
		}
	}

	// Try direct peer connections
	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/sync", peer.IP, a.apiPort)
		req, err := a.peerRequest("GET", url, nil)
		if err != nil {
			continue
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			continue
		}

		func() {
			defer resp.Body.Close()
			var syncResp SyncResponse
			if err := json.NewDecoder(resp.Body).Decode(&syncResp); err == nil {
				a.stateMu.Lock()
				result := a.mergeStartupSyncData(&syncResp)
				a.stateMu.Unlock()
				allLostOwnership = append(allLostOwnership, result.LostOwnership...)
				synced = true
			}
		}()
	}

	// Stop containers we no longer own. removeWorkload runs `docker compose
	// down -v` and is safe to call without the state lock - we already
	// updated state inside the merge.
	for _, wl := range allLostOwnership {
		logInfof("Startup sync: stopping local workload %s - ownership transferred during downtime", wl.Name)
		a.removeWorkload(wl)
	}

	if synced {
		logInfof("Startup sync complete")
		a.saveState()
	} else {
		logWarnf("could not reach any peers for startup sync")
	}
}

// syncWorkloads synchronizes workloads with peers during normal operation.
func (a *Agent) syncWorkloads() {
	// In tunnel-only mode, sync through tunnel domain
	if a.tunnelDomain != "" {
		a.tunnelModeSyncWorkloads()
		return
	}

	// Direct mode: sync with each peer via mesh IP.
	//
	// Attempt every peer regardless of cached p.Healthy. A stale
	// "unhealthy" flag (from a transient network blip that never got
	// updated) used to skip sync, leaving split-brain ownership stuck
	// because the lower-version owner never saw the higher-version
	// peer state. Treat the sync request itself as the live health
	// probe: success flips the peer to healthy and bumps LastSeen,
	// failure leaves the existing state alone for checkPeers to retry.
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		if p.IP != "" {
			peers = append(peers, p)
		}
	}
	a.stateMu.RUnlock()

	var allLostOwnership []*Workload
	var anyEnvUpdated bool
	var recoveredPeers []string

	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/sync", peer.IP, a.apiPort)
		req, err := a.peerRequest("GET", url, nil)
		if err != nil {
			continue
		}
		resp, err := peerClient.Do(req)
		if err != nil {
			continue
		}

		// Sync call succeeded - peer is reachable right now.
		recoveredPeers = append(recoveredPeers, peer.ID)

		func() {
			defer resp.Body.Close()
			var syncResp SyncResponse
			if err := json.NewDecoder(resp.Body).Decode(&syncResp); err != nil {
				return
			}

			a.stateMu.Lock()
			result := a.mergeWorkloadState(&syncResp)
			a.stateMu.Unlock()

			allLostOwnership = append(allLostOwnership, result.LostOwnership...)
			if result.EnvUpdated {
				anyEnvUpdated = true
			}
		}()
	}

	// Stop workloads we lost ownership of (outside lock)
	for _, wl := range allLostOwnership {
		logInfof("Stopping local workload %s - ownership transferred", wl.Name)
		a.removeWorkload(wl)
	}

	// Reflect live reachability into the cached peer state. Without
	// this, a peer that was marked unhealthy by a stale network blip
	// stays unhealthy in state.json (and in the dashboard) until the
	// next checkPeers tick happens to land on a fresh connection.
	if len(recoveredPeers) > 0 {
		now := time.Now()
		a.stateMu.Lock()
		for _, id := range recoveredPeers {
			if peer := a.state.Peers[id]; peer != nil {
				peer.Healthy = true
				peer.LastSeen = now
			}
		}
		a.stateMu.Unlock()
		// Drop any pending failover-grace timer for these peers; they
		// just answered an HTTP request, they're clearly alive.
		a.peerUnhealthySinceMu.Lock()
		for _, id := range recoveredPeers {
			delete(a.peerUnhealthySince, id)
		}
		a.peerUnhealthySinceMu.Unlock()
	}

	if anyEnvUpdated {
		a.saveState()
	}

	a.updateHosts()
}

// tunnelModeSyncWorkloads syncs workloads through the tunnel.
// In tunnel-only mode, we hit the tunnel and get workloads from whichever node responds.
func (a *Agent) tunnelModeSyncWorkloads() {
	url := a.getTunnelAPIURL("/api/sync")
	req, err := a.peerRequest("GET", url, nil)
	if err != nil {
		return
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var syncResp SyncResponse
	if err := json.NewDecoder(resp.Body).Decode(&syncResp); err != nil {
		return
	}

	a.stateMu.Lock()
	result := a.mergeWorkloadState(&syncResp)
	a.stateMu.Unlock()

	// Stop workloads we lost ownership of (outside lock)
	for _, wl := range result.LostOwnership {
		logInfof("Stopping local workload %s - ownership transferred", wl.Name)
		a.removeWorkload(wl)
	}

	if result.EnvUpdated {
		a.saveState()
	}

	a.updateHosts()
}

// broadcastState notifies peers of state changes.
func (a *Agent) broadcastState() {
	// In tunnel-only mode, just trigger a sync through the tunnel
	if a.tunnelDomain != "" {
		url := a.getTunnelAPIURL("/api/sync")
		if req, err := a.peerRequest("GET", url, nil); err == nil {
			if resp, err := httpClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}
		return
	}

	// Direct mode: sync with each peer
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		peers = append(peers, p)
	}
	a.stateMu.RUnlock()

	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/sync", peer.IP, a.apiPort)
		if req, err := a.peerRequest("GET", url, nil); err == nil {
			if resp, err := httpClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}
	}
}
