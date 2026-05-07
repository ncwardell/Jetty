package agent

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"
)

// hasInternetWitness returns true if this node can reach the public
// internet. Used by checkFailover as a partition-detection guard:
// before claiming a workload because the peer "looks dead," we need
// to confirm that we're not the side that's actually offline. With
// only two nodes there's no quorum, so an external witness is the
// best we can do.
//
// Probes in order:
//  1. The cluster's tunnel domain (https). A success here proves at
//     least one cloudflared connector is reachable - and Cloudflare's
//     edge confirms general internet connectivity at the same time.
//  2. TCP 1.1.1.1:443. Generic always-on endpoint; only used when
//     the cluster has no tunnel configured.
//
// Hard-capped at ~1.5s total so the failover scan still feels snappy.
func (a *Agent) hasInternetWitness() bool {
	if a.tunnelDomain != "" {
		url := fmt.Sprintf("https://%s/api/health?node=local", a.tunnelDomain)
		req, err := http.NewRequest("GET", url, nil)
		if err == nil {
			resp, err := unhealthyPeerClient.Do(req)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode < 500 {
					return true
				}
			}
		}
	}
	conn, err := net.DialTimeout("tcp", "1.1.1.1:443", 1500*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// =============================================================================
// Failover
// =============================================================================

func (a *Agent) failoverLoop() {
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-a.stopCh:
			return
		case <-tick.C:
			a.checkFailover()
		}
	}
}

func (a *Agent) checkFailover() {
	// Don't claim workloads in the first FailoverStartupGrace seconds
	// after agent startup. The state we loaded from disk has stale
	// peer-health flags from the last time we ran (e.g. if the laptop
	// shut down with Hetzner marked unreachable from a transient blip,
	// that "false" persists across restart). Even with the per-peer
	// 30s grace window, claims fire too eagerly when the agent is
	// fresh because peerUnhealthySince is in-memory and starts
	// counting from boot. Block claims entirely until memberlist +
	// checkPeers have had time to converge on actual peer health.
	if time.Since(a.startTime) < FailoverStartupGrace {
		return
	}

	// Clean up stale failover entries (older than 5 minutes - deployment timeout)
	a.failoverInProgressMu.Lock()
	for ip, startTime := range a.failoverInProgress {
		if time.Since(startTime) > 5*time.Minute {
			log.Printf("Failover for workload IP %s timed out, allowing retry", ip)
			delete(a.failoverInProgress, ip)
		}
	}
	a.failoverInProgressMu.Unlock()

	// Witness probe before taking the state lock - if we have no
	// public-internet connectivity, we're almost certainly the
	// partitioned side and the "dead" peer is still serving traffic
	// just fine. Done outside the lock so the network call doesn't
	// block other state operations.
	hasWitness := a.hasInternetWitness()

	a.stateMu.Lock()
	defer a.stateMu.Unlock()

	// Find orphaned workloads
	for _, wl := range a.state.Workloads {
		if !wl.Revive {
			continue
		}

		// Check if owner is dead
		if wl.Owner == a.hwid {
			continue // We own it
		}

		// Check if failover is already in progress for this workload
		a.failoverInProgressMu.Lock()
		if _, inProgress := a.failoverInProgress[wl.IP]; inProgress {
			a.failoverInProgressMu.Unlock()
			continue // Already being claimed
		}
		a.failoverInProgressMu.Unlock()

		// Use memberlist for health check if available (faster, more accurate)
		ownerHealthy := false
		if a.memberlist != nil {
			ownerHealthy = a.isPeerHealthyInMemberlist(wl.Owner)
		} else {
			// Fallback to stored peer state
			owner := a.state.Peers[wl.Owner]
			if owner != nil && owner.Healthy {
				ownerHealthy = true
			}
			if owner != nil && time.Since(owner.LastSeen) < HealthTimeout {
				ownerHealthy = true // Recently seen via gossip
			}
		}

		if ownerHealthy {
			// Owner recovered (or never actually went away). Clear any
			// pending unhealthy timer so a future blip starts a fresh
			// grace window.
			a.peerUnhealthySinceMu.Lock()
			delete(a.peerUnhealthySince, wl.Owner)
			a.peerUnhealthySinceMu.Unlock()
			continue
		}

		// Owner appears dead. Apply a debounce: don't claim until the
		// peer has been unreachable for at least FailoverGracePeriod.
		// Memberlist marks a node "left" on a single missed heartbeat,
		// which a 5-second WiFi blip or laptop suspend can trigger;
		// without this gate we end up with split-brain ownership the
		// moment the network recovers and both sides claim.
		a.peerUnhealthySinceMu.Lock()
		firstSeen, exists := a.peerUnhealthySince[wl.Owner]
		if !exists {
			a.peerUnhealthySince[wl.Owner] = time.Now()
			a.peerUnhealthySinceMu.Unlock()
			continue
		}
		elapsed := time.Since(firstSeen)
		a.peerUnhealthySinceMu.Unlock()
		if elapsed < FailoverGracePeriod {
			continue
		}

		// Witness check: if we couldn't reach the public internet at
		// the start of this scan, refuse to claim. With only two nodes
		// there's no quorum, so this is our only defense against
		// split-brain when WE are the partitioned side - claiming
		// would just duplicate the workload while the peer keeps
		// serving traffic perfectly fine.
		if !hasWitness {
			a.warnPartitioned(wl)
			continue
		}

		// Owner has been unreachable for the full grace window. Before
		// electing, check whether *any* node in the cluster can run this
		// workload. If not, the workload is marooned - usually because
		// its arch-specific compose file matches no live node. Surface a
		// periodic warning so the user knows their arm64-only workload
		// is sitting orphaned with the arm64 node down.
		if !a.anyNodeCanRun(wl) {
			a.warnMarooned(wl)
			continue
		}

		// Owner is dead - should we claim?
		if a.shouldClaim(wl) {
			log.Printf("Claiming orphaned workload: %s", wl.Name)

			// Mark failover in progress before releasing state lock
			a.failoverInProgressMu.Lock()
			a.failoverInProgress[wl.IP] = time.Now()
			a.failoverInProgressMu.Unlock()

			oldOwner := wl.Owner
			oldVersion := wl.Version
			wl.Owner = a.hwid
			wl.Version = time.Now().UnixNano() // Use nanoseconds for better precision

			// Deploy it - capture workload state for rollback on failure
			go func(w *Workload, prevOwner string, prevVersion int64) {
				// Ensure we clean up the in-progress marker when done
				defer func() {
					a.failoverInProgressMu.Lock()
					delete(a.failoverInProgress, w.IP)
					a.failoverInProgressMu.Unlock()
				}()

				if err := a.deployWorkload(w); err != nil {
					log.Printf("Failover deploy failed for %s: %v - reverting ownership", w.Name, err)
					// Rollback ownership on failure so another node can try
					a.stateMu.Lock()
					if existing := a.state.Workloads[w.IP]; existing != nil && existing.Owner == a.hwid {
						existing.Owner = prevOwner
						existing.Version = prevVersion
					}
					a.stateMu.Unlock()
					return
				}
				a.updateHosts()
				a.saveState()
				// Broadcast via memberlist if available, otherwise use HTTP
				if a.memberlist != nil {
					a.broadcastWorkloadUpdate(w)
				} else {
					a.broadcastState()
				}
			}(wl, oldOwner, oldVersion)
		}
	}
}

// anyNodeCanRun returns true if at least one node in the cluster (us or a
// healthy peer) is allowed to run this workload AND has a compatible
// architecture. Used to detect "marooned" workloads where the compose file
// matches no live node.
//
// Caller must hold a.stateMu.
func (a *Agent) anyNodeCanRun(wl *Workload) bool {
	if a.isThisNodeAllowed(wl) {
		return true
	}
	if a.memberlist != nil {
		for _, node := range a.memberlist.Members() {
			if node.Name == a.hwid {
				continue
			}
			meta := parseNodeMeta(node.Meta)
			if meta != nil && a.isNodeAllowed(wl, meta.ID, meta.Name, meta.Arch) {
				return true
			}
		}
	} else {
		for _, p := range a.state.Peers {
			if p.Healthy && a.isNodeAllowed(wl, p.ID, p.Name, p.Arch) {
				return true
			}
		}
	}
	return false
}

// warnMarooned logs a "no compatible node" warning, rate-limited to once
// per workload per 10 minutes so the failover loop doesn't spam.
func (a *Agent) warnMarooned(wl *Workload) {
	a.maroonedLoggedMu.Lock()
	defer a.maroonedLoggedMu.Unlock()
	if last, ok := a.maroonedLogged[wl.IP]; ok && time.Since(last) < 10*time.Minute {
		return
	}
	a.maroonedLogged[wl.IP] = time.Now()

	reasons := []string{}
	if len(wl.AllowedNodes) > 0 {
		reasons = append(reasons, fmt.Sprintf("allowed_nodes=%v", wl.AllowedNodes))
	}
	archHas := []string{}
	if wl.Compose != "" {
		archHas = append(archHas, "default")
	}
	if wl.ComposeAmd64 != "" {
		archHas = append(archHas, "amd64")
	}
	if wl.ComposeArm64 != "" {
		archHas = append(archHas, "arm64")
	}
	if len(archHas) > 0 {
		reasons = append(reasons, fmt.Sprintf("compose=%v", archHas))
	}
	log.Printf("Workload %s (%s) is MAROONED: owner is dead and no other node is compatible (%s)",
		wl.Name, wl.IP, strings.Join(reasons, ", "))
}

// warnPartitioned logs a "we appear to be partitioned" warning,
// rate-limited per workload so the failover loop doesn't spam during
// an outage. Reuses the marooned-log map since both are per-workload
// suppress-spam log throttles - they don't overlap (a workload is
// either MAROONED or its peer-owner is just unreachable from us).
func (a *Agent) warnPartitioned(wl *Workload) {
	a.maroonedLoggedMu.Lock()
	defer a.maroonedLoggedMu.Unlock()
	if last, ok := a.maroonedLogged[wl.IP]; ok && time.Since(last) < 10*time.Minute {
		return
	}
	a.maroonedLogged[wl.IP] = time.Now()
	log.Printf("Failover: refusing to claim %s (owned by %s) - this node has no public-internet connectivity, so we may be the partitioned side. Will retry when connectivity returns.",
		wl.Name, shortID(wl.Owner, 12))
}

// shouldClaim determines if this node should claim an orphaned workload.
//
// Election: every candidate node (allowed by allowed_nodes + compatible
// arch) is ranked by ascending owned-workload count, with HWID as a
// deterministic tie-breaker. Lowest rank wins. This biases failover
// toward the least-busy node so a single host doesn't end up running
// every survivor's workloads after a multi-node failure.
//
// The election is deterministic across the cluster because every peer
// observes the same a.state.Workloads via gossip. As long as every peer's
// view of "who is healthy" agrees (memberlist's job), every peer reaches
// the same conclusion about the winner without any consensus protocol.
//
// NOTE: Caller must already hold stateMu lock.
func (a *Agent) shouldClaim(wl *Workload) bool {
	// First check if this node is even allowed to run the workload
	// (this also checks architecture compatibility via runtime.GOARCH)
	if !a.isThisNodeAllowed(wl) {
		return false
	}

	// Build candidate list: us + every other allowed-and-healthy node.
	var candidates []string
	candidates = append(candidates, a.hwid)
	if a.memberlist != nil {
		for _, node := range a.memberlist.Members() {
			if node.Name == a.hwid {
				continue // Skip self (already added)
			}
			meta := parseNodeMeta(node.Meta)
			if meta != nil && a.isNodeAllowed(wl, meta.ID, meta.Name, meta.Arch) {
				candidates = append(candidates, meta.ID)
			}
		}
	} else {
		for _, p := range a.state.Peers {
			if p.Healthy && a.isNodeAllowed(wl, p.ID, p.Name, p.Arch) {
				candidates = append(candidates, p.ID)
			}
		}
	}

	// Count currently-owned workloads per candidate. We exclude the
	// orphan itself so we're not double-counting it against its (dead)
	// previous owner.
	loads := make(map[string]int, len(candidates))
	for _, w := range a.state.Workloads {
		if w.Owner != "" && w.IP != wl.IP {
			loads[w.Owner]++
		}
	}

	// Sort: fewer owned workloads first; HWID asc as tiebreak.
	sort.Slice(candidates, func(i, j int) bool {
		li, lj := loads[candidates[i]], loads[candidates[j]]
		if li != lj {
			return li < lj
		}
		return candidates[i] < candidates[j]
	})

	return len(candidates) > 0 && candidates[0] == a.hwid
}
