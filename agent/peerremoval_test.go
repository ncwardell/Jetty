package agent

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Node removal took production down. The failure was not that removal did the
// wrong thing locally - it was that removal could not *stick*: the removed
// node kept running, kept gossiping, and every peer put it straight back,
// while the teardown of tunnels and routes had already happened. These run the
// removal through the same multi-node harness as the convergence tests, which
// is the only place that failure is visible.

func removedIDs(a *Agent) map[string]bool {
	out := map[string]bool{}
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	for id := range a.state.RemovedPeers {
		out[id] = true
	}
	return out
}

// linkPeers gives every agent a peer entry for every other agent.
func linkPeers(c *cluster) {
	for _, a := range c.agents {
		for _, p := range c.agents {
			if p == a {
				continue
			}
			a.stateMu.Lock()
			a.state.Peers[p.hwid] = &Peer{
				ID: p.hwid, Name: p.hostname, Healthy: true, Arch: runtime.GOARCH,
			}
			a.stateMu.Unlock()
		}
	}
}

func TestRemovalPropagatesToEveryPeer(t *testing.T) {
	c := newCluster(t, 4)
	linkPeers(c)
	victim := c.agents[3]

	c.agents[0].stateMu.Lock()
	delete(c.agents[0].state.Peers, victim.hwid)
	c.agents[0].tombstonePeerLocked(victim.hwid, victim.hostname)
	c.agents[0].stateMu.Unlock()

	c.gossipUntilQuiet()

	// Everyone except the victim should have dropped it and be holding the
	// tombstone. The victim ignores a tombstone naming itself.
	for _, a := range c.agents[:3] {
		a.stateMu.RLock()
		_, stillPeer := a.state.Peers[victim.hwid]
		a.stateMu.RUnlock()
		if stillPeer {
			t.Errorf("%s still lists the removed node as a peer", a.hwid)
		}
		if !removedIDs(a)[victim.hwid] {
			t.Errorf("%s did not receive the removal tombstone", a.hwid)
		}
	}
}

func TestRemovedNodeCannotBeResurrectedByGossip(t *testing.T) {
	// The actual outage. The removed node is still up and still announcing,
	// so without a tombstone check every peer re-adds it within a round and
	// the teardown/rebuild flaps.
	c := newCluster(t, 3)
	linkPeers(c)
	victim := c.agents[2]

	for _, a := range c.agents[:2] {
		a.stateMu.Lock()
		delete(a.state.Peers, victim.hwid)
		a.tombstonePeerLocked(victim.hwid, victim.hostname)
		a.stateMu.Unlock()
	}

	// The victim keeps gossiping its full state, as a live node does.
	for round := 0; round < 5; round++ {
		for _, a := range c.agents[:2] {
			c.gossip(victim, a)
		}
	}

	for _, a := range c.agents[:2] {
		a.stateMu.RLock()
		_, back := a.state.Peers[victim.hwid]
		a.stateMu.RUnlock()
		if back {
			t.Errorf("%s re-added the removed node from its own gossip - "+
				"this is the flap that bricked the cluster", a.hwid)
		}
	}
}

func TestPeerAnnounceFromARemovedNodeIsRejected(t *testing.T) {
	// The real resurrection path. A removed node is still running, so it
	// keeps announcing itself on its normal schedule; before the tombstone
	// check this handler put it straight back into the peer table.
	a := newTestAgentWithDir(t)
	a.hwid = "us"
	a.hostname = "us-host"
	a.stateMu.Lock()
	a.tombstonePeerLocked("gone", "gone-node")
	a.stateMu.Unlock()

	body := `{"peer":{"id":"gone","name":"gone-node","ip":"100.96.0.9","arch":"amd64"}}`
	r := httptest.NewRequest(http.MethodPost, "/api/peer-announce", strings.NewReader(body))
	w := httptest.NewRecorder()
	a.apiPeerAnnounce(w, r)

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403 for an announce from a removed node", w.Code)
	}
	a.stateMu.RLock()
	_, back := a.state.Peers["gone"]
	a.stateMu.RUnlock()
	if back {
		t.Error("a removed node re-added itself via /api/peer-announce — " +
			"this is the path that undid the removal in production")
	}
}

func TestPeerAnnounceFromALiveNodeStillWorks(t *testing.T) {
	// The guard must not break normal announces.
	a := newTestAgentWithDir(t)
	a.hwid = "us"
	a.hostname = "us-host"

	body := `{"peer":{"id":"friend","name":"friend-node","ip":"100.96.0.8","arch":"amd64"}}`
	r := httptest.NewRequest(http.MethodPost, "/api/peer-announce", strings.NewReader(body))
	w := httptest.NewRecorder()
	a.apiPeerAnnounce(w, r)

	if w.Code >= 300 {
		t.Fatalf("status = %d, want success for a normal announce", w.Code)
	}
	a.stateMu.RLock()
	_, added := a.state.Peers["friend"]
	a.stateMu.RUnlock()
	if !added {
		t.Error("a live node was not registered by /api/peer-announce")
	}
}

func TestNotifyJoinRefusesARemovedNode(t *testing.T) {
	a := newTestAgent("s")
	a.hwid = "us"
	a.stateMu.Lock()
	a.tombstonePeerLocked("gone", "gone-node")
	a.stateMu.Unlock()

	a.stateMu.RLock()
	blocked := a.peerRemovedLocked("gone")
	a.stateMu.RUnlock()
	if !blocked {
		t.Fatal("tombstoned node is not reported as removed")
	}

	a.stateMu.RLock()
	live := a.peerRemovedLocked("someone-else")
	a.stateMu.RUnlock()
	if live {
		t.Error("a node with no tombstone was reported as removed")
	}
}

func TestFreshJoinTokenReadmitsARemovedNode(t *testing.T) {
	// The tombstone blocks resurrection by gossip, not readmission by
	// decision. Minting a join token is an explicit operator action.
	a := newTestAgent("s")
	a.stateMu.Lock()
	a.tombstonePeerLocked("gone", "gone-node")
	a.clearPeerTombstoneLocked("gone")
	readmitted := !a.peerRemovedLocked("gone")
	a.stateMu.Unlock()

	if !readmitted {
		t.Error("a fresh join token did not clear the removal tombstone")
	}
}

func TestATombstoneNamingUsIsAcceptedButNotActedOn(t *testing.T) {
	// A node must never evict itself on someone else's say-so - one stale
	// gossip message would otherwise take a healthy node out of its own view
	// of the cluster. The tombstone is still stored so it keeps propagating.
	a := newTestAgent("s")
	a.hwid = "us"
	a.hostname = "us-host"
	a.stateMu.Lock()
	a.state.Peers["other"] = &Peer{ID: "other", Name: "other", Healthy: true}
	changed := a.mergeRemovedPeers([]*RemovedPeer{
		{ID: "us", Name: "us-host", Version: time.Now().UnixNano()},
	})
	_, selfStillPeer := a.state.Peers["other"]
	stored := a.state.RemovedPeers["us"] != nil
	a.stateMu.Unlock()

	if !changed || !stored {
		t.Error("a tombstone naming this node should still be stored and propagated")
	}
	if !selfStillPeer {
		t.Error("merging a self-tombstone disturbed unrelated peers")
	}
}

func TestRemovalTombstonesUseHighestVersionWins(t *testing.T) {
	a := newTestAgent("s")
	a.stateMu.Lock()
	a.tombstonePeerLocked("n1", "node-one")
	original := a.state.RemovedPeers["n1"].Version

	// An older tombstone must not overwrite a newer one.
	a.mergeRemovedPeers([]*RemovedPeer{{ID: "n1", Name: "node-one", Version: original - 1000}})
	afterOld := a.state.RemovedPeers["n1"].Version

	a.mergeRemovedPeers([]*RemovedPeer{{ID: "n1", Name: "node-one", Version: original + 1000}})
	afterNew := a.state.RemovedPeers["n1"].Version
	a.stateMu.Unlock()

	if afterOld != original {
		t.Errorf("an older tombstone overwrote a newer one: %d -> %d", original, afterOld)
	}
	if afterNew != original+1000 {
		t.Errorf("a newer tombstone was not adopted: got %d", afterNew)
	}
}

func TestExpiredTombstonesStopBlockingAndAreCollected(t *testing.T) {
	// A node powered off for longer than the retention window has to be able
	// to come back, or it is stranded permanently with no way in but a manual
	// state edit.
	a := newTestAgent("s")
	a.stateMu.Lock()
	a.state.RemovedPeers["old"] = &RemovedPeer{
		ID: "old", Version: time.Now().Add(-RemovedPeerMaxAge - time.Hour).UnixNano(),
	}
	a.state.RemovedPeers["recent"] = &RemovedPeer{
		ID: "recent", Version: time.Now().UnixNano(),
	}

	if a.peerRemovedLocked("old") {
		t.Error("an expired tombstone is still blocking the node")
	}
	if !a.peerRemovedLocked("recent") {
		t.Error("a live tombstone stopped blocking")
	}

	a.gcRemovedPeers()
	_, oldKept := a.state.RemovedPeers["old"]
	_, recentKept := a.state.RemovedPeers["recent"]
	a.stateMu.Unlock()

	if oldKept {
		t.Error("expired tombstone was not collected")
	}
	if !recentKept {
		t.Error("GC collected a live tombstone")
	}
}

func TestRemovalStateStillConverges(t *testing.T) {
	// Removals ride the same merge path as workloads and env, so they must
	// not break convergence - including when two nodes remove different peers
	// concurrently and only learn about each other's removal later.
	c := newCluster(t, 4)
	linkPeers(c)

	c.agents[0].stateMu.Lock()
	c.agents[0].tombstonePeerLocked(c.agents[2].hwid, c.agents[2].hostname)
	delete(c.agents[0].state.Peers, c.agents[2].hwid)
	c.agents[0].stateMu.Unlock()

	c.agents[1].stateMu.Lock()
	c.agents[1].tombstonePeerLocked(c.agents[3].hwid, c.agents[3].hostname)
	delete(c.agents[1].state.Peers, c.agents[3].hwid)
	c.agents[1].stateMu.Unlock()

	c.gossipUntilQuiet()
	c.assertConverged(t)

	for _, a := range c.agents[:2] {
		got := removedIDs(a)
		if !got[c.agents[2].hwid] || !got[c.agents[3].hwid] {
			t.Errorf("%s did not converge on both removals: %v", a.hwid, got)
		}
	}
}

// TestLeaveNeverDestroysVolumes guards an invariant that cannot be exercised
// in a unit test (it shells out to docker) but is the most dangerous thing on
// this path.
//
// removeWorkload runs `docker compose down -v --remove-orphans`, and the -v
// destroys every named volume in the project - databases, SQLite state, auth
// tokens. The first version of apiLeave called it for every workload the node
// owned, so DELETE /api/nodes/{id} would have told the target to wipe its own
// data. Being removed from a cluster must never mean losing what is on the
// node: the operator may be decommissioning it, or may have clicked the wrong
// row.
//
// Stopping is all the leave needs - it exists so two copies of a workload do
// not run while the cluster fails it over, not to reclaim disk.
func TestLeaveNeverDestroysVolumes(t *testing.T) {
	src := readAgentSource(t, "peerremoval.go")

	start := strings.Index(src, "func (a *Agent) apiLeave(")
	if start == -1 {
		t.Fatal("apiLeave not found")
	}
	end := strings.Index(src[start:], "\n}\n")
	if end == -1 {
		t.Fatal("could not find the end of apiLeave")
	}
	body := src[start : start+end]

	for _, forbidden := range []string{"removeWorkload", `"down"`, `"-v"`} {
		// Skip the explanatory comment lines; only real calls matter.
		for _, line := range strings.Split(body, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if strings.Contains(line, forbidden) {
				t.Errorf("apiLeave references %s - leaving a cluster must stop "+
					"workloads, never destroy their volumes:\n  %s", forbidden, trimmed)
			}
		}
	}

	if !strings.Contains(body, `composeCmd(wl.Name, "stop")`) {
		t.Error("apiLeave no longer stops the workloads it owns; without that, " +
			"the removed node keeps serving them while the cluster fails them " +
			"over - two live copies of the same workload")
	}
}
