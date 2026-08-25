package agent

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// =============================================================================
// Peer removal
// =============================================================================
//
// Removing a node used to be a unilateral local write: delete it from this
// node's peer table and broadcast. That cannot work, because the removed node
// is still running, still on WARP and still an active memberlist member - so
// NotifyJoin and /api/peer-announce put it straight back, while the removal
// had already torn down the IPIP tunnel, the route table and /etc/hosts. The
// resulting rebuild-teardown flap is what took the cluster down.
//
// Removal is now two things that have to happen together:
//
//   1. A tombstone, so no peer can resurrect the node by gossip.
//   2. A cooperative leave, so the node itself stops participating. Memberlist
//      has no forcible eviction - a node can only be made to leave by asking
//      it - so removal is a conversation, not an announcement.
//
// The tombstone deliberately does NOT gate /api/join. Presenting a fresh,
// valid join token is an explicit operator decision to re-admit the node, and
// it clears the tombstone. What the tombstone blocks is resurrection by
// gossip, which is nobody's decision at all.

// peerRemovedLocked reports whether a node ID carries a live removal
// tombstone. Callers must hold stateMu.
func (a *Agent) peerRemovedLocked(id string) bool {
	if id == "" {
		return false
	}
	rp := a.state.RemovedPeers[id]
	if rp == nil {
		return false
	}
	// An expired tombstone is not a removal any more; let the node back in
	// rather than stranding it forever.
	return time.Since(time.Unix(0, rp.Version)) < RemovedPeerMaxAge
}

// tombstonePeerLocked records a removal. Callers must hold stateMu.
func (a *Agent) tombstonePeerLocked(id, name string) *RemovedPeer {
	if a.state.RemovedPeers == nil {
		a.state.RemovedPeers = make(map[string]*RemovedPeer)
	}
	rp := &RemovedPeer{ID: id, Name: name, Version: time.Now().UnixNano()}
	a.state.RemovedPeers[id] = rp
	return rp
}

// clearPeerTombstoneLocked forgets a removal, re-admitting the node. Only
// /api/join should call this: it means an operator minted a fresh join token
// for the node, which is an explicit decision to let it back in. Callers must
// hold stateMu.
func (a *Agent) clearPeerTombstoneLocked(id string) {
	delete(a.state.RemovedPeers, id)
}

// mergeRemovedPeers folds a peer's removal tombstones into ours and drops any
// peer the tombstone supersedes. Highest version wins, matching the workload
// tombstone rule. Callers must hold stateMu. Reports whether anything changed.
func (a *Agent) mergeRemovedPeers(incoming []*RemovedPeer) bool {
	changed := false
	for _, rp := range incoming {
		if rp == nil || rp.ID == "" || !validPeerNamePattern.MatchString(rp.Name) && rp.Name != "" {
			continue
		}
		existing := a.state.RemovedPeers[rp.ID]
		if existing != nil && existing.Version >= rp.Version {
			continue
		}
		if a.state.RemovedPeers == nil {
			a.state.RemovedPeers = make(map[string]*RemovedPeer)
		}
		a.state.RemovedPeers[rp.ID] = rp
		changed = true

		// Removing self is not something a peer gets to do to us. Accept the
		// tombstone so it keeps propagating, but never act on it - otherwise
		// one stale gossip message could evict a healthy node from its own
		// view of the cluster.
		if rp.ID == a.hwid {
			logWarnf("Ignoring a removal tombstone for this node from a peer; "+
				"rejoin with a fresh join token if the removal was intended (version %d)", rp.Version)
			continue
		}

		if peer := a.state.Peers[rp.ID]; peer != nil {
			logInfof("Removing peer %s (%s) - tombstoned by a peer", peer.Name, shortID(rp.ID, 12))
			delete(a.state.Peers, rp.ID)
		}
	}
	return changed
}

// gcRemovedPeers drops expired tombstones. Callers must hold stateMu.
func (a *Agent) gcRemovedPeers() {
	for id, rp := range a.state.RemovedPeers {
		if time.Since(time.Unix(0, rp.Version)) > RemovedPeerMaxAge {
			delete(a.state.RemovedPeers, id)
		}
	}
}

// requestPeerLeave asks a node to leave the cluster. Returns whether it
// acknowledged, and a short reason when it did not.
//
// Best-effort by necessity: the node may already be off, unreachable, or
// running a build that predates /api/leave. A node that cannot be reached is
// also not serving traffic, so failing over its workloads is safe. What is not
// safe is a *reachable* node that keeps running them, which is exactly the
// case this call exists to eliminate.
func (a *Agent) requestPeerLeave(peer *Peer) (bool, string) {
	url := a.getPeerAPIURL(peer, "/api/leave")
	if url == "" {
		return false, "no route to the node"
	}
	req, err := a.peerRequest("POST", url, strings.NewReader(`{}`))
	if err != nil {
		return false, "could not build the request: " + err.Error()
	}
	resp, err := peerClient.Do(req)
	if err != nil {
		return false, "unreachable: " + err.Error()
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return false, "the node is running a version without /api/leave"
	}
	if resp.StatusCode >= 300 {
		return false, "the node returned HTTP " + resp.Status
	}
	return true, ""
}

// apiLeave godoc
// @Summary Leave the cluster (internal)
// @Description Node-to-node. Tells this node it has been removed: it stops the workloads it owns, leaves the memberlist gossip pool, and tombstones itself so it does not rejoin by gossip. Re-admission requires a fresh join token. Peer or admin API key required.
// @Tags internal
// @Produce json
// @Success 200 {object} map[string]string
// @Router /leave [post]
func (a *Agent) apiLeave(w http.ResponseWriter, r *http.Request) {
	logWarnf("Leaving the cluster: another node removed us. " +
		"Stopping owned workloads and leaving the gossip pool. " +
		"Re-admission requires a fresh join token.")

	// Stop what we own first. The cluster is about to fail these over, and
	// two copies of a stateful workload on shared storage is the failure this
	// whole path exists to prevent. Do it before leaving the pool so we are
	// still able to see our own state consistently.
	a.stateMu.Lock()
	owned := make([]*Workload, 0)
	for _, wl := range a.state.Workloads {
		if wl.Owner == a.hwid {
			owned = append(owned, wl)
		}
	}
	// Tombstone ourselves so a stray gossip round cannot re-add us to our own
	// peer table, and so the state we persist records that we are out.
	a.tombstonePeerLocked(a.hwid, a.hostname)
	a.stateMu.Unlock()

	// STOP, do not remove.
	//
	// removeWorkload runs `docker compose down -v --remove-orphans`, and the
	// -v destroys every named volume in the project - the database files, the
	// SQLite state, the auth tokens. Being removed from a cluster must never
	// mean losing the data on the node being removed: the operator may be
	// decommissioning it, or may simply have clicked the wrong row.
	//
	// `stop` gives us everything the leave actually needs. The point is that
	// this node is no longer serving these workloads while the cluster fails
	// them over, so two copies never run at once. The containers and their
	// volumes stay exactly where they are, and `docker compose start` (or a
	// rejoin) brings them back.
	for _, wl := range owned {
		logInfof("Leave: stopping %s (containers and volumes are preserved)", wl.Name)
		if out, err := a.composeCmd(wl.Name, "stop"); err != nil {
			logWarnf("Leave: failed to stop %s: %v: %s", wl.Name, err, strings.TrimSpace(out))
		}
	}

	if a.memberlist != nil {
		if err := a.memberlist.Leave(memberlistLeaveTimeout); err != nil {
			logWarnf("Leave: memberlist leave failed: %v", err)
		}
	}

	a.saveState()

	writeJSON(w, map[string]string{
		"status":   "left",
		"id":       a.hwid,
		"name":     a.hostname,
		"stopped":  strconv.Itoa(len(owned)),
		"rejoin":   "requires a fresh join token",
		"workload": "owned workloads stopped",
	})
}

// removedPeersSlice renders tombstones for the wire. Callers must hold stateMu.
func (a *Agent) removedPeersSlice() []*RemovedPeer {
	out := make([]*RemovedPeer, 0, len(a.state.RemovedPeers))
	for _, rp := range a.state.RemovedPeers {
		out = append(out, rp)
	}
	return out
}
