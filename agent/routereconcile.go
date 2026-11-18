package agent

import (
	"time"
)

// =============================================================================
// Route reconciliation
// =============================================================================
//
// This exists because route updates used to run with stateMu held.
//
// updateWorkloadRoutes was documented "caller must hold a.stateMu", and it
// shelled out: three `ip route` calls of its own plus five more per peer
// inside ensurePeerTunnel - so 3+5N fork/execs, none of them bounded, all
// while holding the single mutex that guards every piece of cluster state.
//
// apiKeyMiddleware takes stateMu.RLock() on *every* API request to look up the
// caller's key, and Go's RWMutex excludes new readers as soon as a writer is
// queued. So one slow `ip` child - netlink contention, a fork under memory
// pressure, a child that simply never exits - stops the entire control plane.
// Not one endpoint: all of them, including /api/health, which means a wedged
// node cannot even report that it is wedged. The listener stays open because
// the kernel owns it, so from outside the node looks healthy and answers
// nothing. That is indistinguishable from a broken tunnel, which is why the
// symptom is so hard to attribute.
//
// The invariant that was being violated: never hold a lock across an operation
// of unbounded duration - I/O, network, or process execution.
//
// The fix has three parts:
//
//  1. Callers no longer reconcile inline. They call triggerRouteReconcile(),
//     a non-blocking send that is safe to make while holding stateMu.
//  2. The reconciler snapshots state under a brief read lock, releases it, and
//     only then execs. Nothing that forks runs under stateMu.
//  3. Reconciles are coalesced rather than queued. Route reconciliation is
//     idempotent and level-triggered: if three events arrive while one
//     reconcile is running, the right answer is one more reconcile against
//     final state, not three against three stale snapshots. A plain mutex
//     would have preserved ordering while still building that backlog.

const (
	// routeCommandTimeout bounds a single `ip` invocation. Route programming
	// is a local netlink operation - if it has not finished in this long it is
	// stuck, and waiting longer only delays reconciliation of everything else.
	routeCommandTimeout = hostCommandTimeout

	// routeReconcileDebounce coalesces bursts. Peer announces and health flaps
	// arrive together; reconciling once after the burst beats reconciling per
	// event.
	routeReconcileDebounce = 250 * time.Millisecond
)

// routeCommandRunner and routeCommandOutput are the indirection the route and
// tunnel paths exec through. Vars so tests can substitute a deliberately slow
// command and assert that API-path read locks are still served while it runs -
// the property that failed in production, and which a fast-failing command
// cannot demonstrate.
//
// Both are bounded (command.go); a hung child costs one routeCommandTimeout,
// not the reconcile loop.
var (
	routeCommandRunner = runCmd
	routeCommandOutput = runCmdCombined
)

// routePeer is the subset of a Peer the route path needs.
type routePeer struct {
	ID   string
	Name string
	IP   string
}

// routeTarget names the peer a workload's traffic should be sent to.
type routeTarget struct {
	ownerID string
	ownerIP string
}

// routeSnapshot is everything reconciliation needs, copied out of cluster
// state so the apply phase can run with no locks held.
type routeSnapshot struct {
	warpIP     string
	tunnelMode string
	hasTun     bool
	peers      []routePeer
	desired    map[string]routeTarget
}

// triggerRouteReconcile asks for a reconcile without performing one.
//
// Non-blocking and therefore safe to call with stateMu held, which is the
// whole point: the call sites are event handlers that legitimately hold the
// lock while mutating state. The channel has capacity 1 and reconciliation is
// level-triggered, so a pending request already covers any state written
// before it runs.
func (a *Agent) triggerRouteReconcile() {
	if a.routeReconcileCh == nil {
		return // not started (tests, or a partially constructed agent)
	}
	select {
	case a.routeReconcileCh <- struct{}{}:
	default: // a reconcile is already pending; it will see our writes
	}
}

// routeReconcileLoop owns route programming for the lifetime of the agent.
func (a *Agent) routeReconcileLoop() {
	for {
		select {
		case <-a.stopCh:
			return
		case <-a.routeReconcileCh:
			// Let a burst settle before doing the work. Peer announces,
			// health flaps and memberlist events arrive in clusters.
			select {
			case <-time.After(routeReconcileDebounce):
			case <-a.stopCh:
				return
			}
			a.reconcileWorkloadRoutes()
		}
	}
}

// reconcileWorkloadRoutes snapshots state, releases the lock, then programs
// routes. The split is the fix; keep it.
func (a *Agent) reconcileWorkloadRoutes() {
	snap, ok := a.snapshotRoutes()
	if !ok {
		return
	}
	a.applyRoutes(snap)
}

// snapshotRoutes copies what reconciliation needs. Takes stateMu.RLock
// briefly - no exec, no I/O, no blocking calls of any kind in here.
func (a *Agent) snapshotRoutes() (routeSnapshot, bool) {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()

	if a.warpIP() == "" {
		return routeSnapshot{}, false // WARP not connected
	}

	snap := routeSnapshot{
		warpIP:     a.warpIP(),
		tunnelMode: a.tunnelMode,
		hasTun:     a.tunDevice != nil,
		desired:    make(map[string]routeTarget),
	}

	for _, peer := range a.state.Peers {
		if peer.IP != "" {
			snap.peers = append(snap.peers, routePeer{ID: peer.ID, Name: peer.Name, IP: peer.IP})
		}
	}

	for _, wl := range a.state.Workloads {
		if wl.Owner == a.hwid {
			continue // local workload: handled by iptables DNAT, no route
		}
		// Only route through healthy peers with a known address.
		if peer, ok := a.state.Peers[wl.Owner]; ok && peer.IP != "" && peer.Healthy {
			snap.desired[wl.IP] = routeTarget{ownerID: peer.ID, ownerIP: peer.IP}
		}
	}
	return snap, true
}

// applyRoutes programs the kernel to match the snapshot. Runs with stateMu
// NOT held; every exec here is bounded.
func (a *Agent) applyRoutes(snap routeSnapshot) {
	// Refresh the transport to every known peer, healthy or not - gate only on
	// having an address. In tunnel mode the health check runs *through* this
	// tunnel, so rebuilding tunnels only for already-healthy peers deadlocks
	// recovery: a peer whose underlay changed could never heal, because the
	// tunnel health depends on is only rebuilt for peers already considered
	// healthy. Route *installation* below still targets healthy peers only, so
	// we never point a route at a dead node.
	if snap.tunnelMode != "" {
		for _, peer := range snap.peers {
			if err := a.ensurePeerTunnel(peer.ID, peer.IP); err != nil {
				logWarnf("failed to ensure tunnel to %s: %v", peer.Name, err)
			}
		}
	} else if snap.hasTun {
		for _, peer := range snap.peers {
			a.updateTunPeerAddr(peer.ID, peer.IP)
		}
	}

	// workloadRoutesMu is not held across any exec below. The userspace
	// tunnel's send loop takes it on *every outbound packet* to find the
	// owner of the destination, so holding it across 2N `ip route` calls - as
	// this function used to - meant one hung `ip` stalled all cross-node
	// workload traffic for as long as the child lived, on top of stalling
	// reconciliation. Instead: unpublish stale entries and copy the map under
	// the lock, exec with it released, publish successes under it again.
	//
	// The reconcile loop is the only writer outside shutdown teardown, so the
	// copy cannot go stale under us except by cleanupNetwork, after which the
	// agent is exiting anyway.
	type staleRoute struct{ wlIP, ownerID string }
	var stale []staleRoute
	a.workloadRoutesMu.Lock()
	prev := make(map[string]string, len(a.workloadRoutes))
	for wlIP, ownerID := range a.workloadRoutes {
		prev[wlIP] = ownerID
		// Stale: no longer wanted, or the owner changed. Unpublish first
		// so the packet path stops forwarding to the old owner now, not
		// after the kernel delete returns.
		if desired, ok := snap.desired[wlIP]; !ok || desired.ownerID != ownerID {
			stale = append(stale, staleRoute{wlIP, ownerID})
			delete(a.workloadRoutes, wlIP)
		}
	}
	a.workloadRoutesMu.Unlock()

	for _, r := range stale {
		if err := routeCommandRunner(routeCommandTimeout, "ip", "route", "del", r.wlIP+"/32"); err != nil {
			logDebugf("route del %s: %v", r.wlIP, err)
		}
		logInfof("Removed route for %s (was via %s)", r.wlIP, shortID(r.ownerID, 8))
	}

	// Add or refresh. `ip route replace` rather than `add` so kernel state
	// converges regardless of what is already installed - that is what makes
	// this self-healing when a route is wiped out-of-band (TUN flap, manual
	// `ip route del`, a container restart that recreated the link). Trusting
	// a.workloadRoutes as authoritative instead would leave a node with a
	// populated map and no actual routes, which we have seen happen.
	for wlIP, info := range snap.desired {
		var err error
		var routeDesc string

		switch {
		case snap.tunnelMode != "":
			tunName := a.getTunnelName(info.ownerID)
			err = routeCommandRunner(routeCommandTimeout, "ip", "route", "replace", wlIP+"/32", "dev", tunName)
			routeDesc = "tunnel " + tunName

		case snap.hasTun:
			// `src <warpIP>` pins the source address the kernel picks. Without
			// it, jetty_tun has no IPv4 address so the kernel falls back to
			// the default route's source (eth0's public IP). Packets leave
			// fine, but the peer's reply is addressed to a non-mesh IP and
			// gets routed via its public default route instead of back
			// through the tunnel - the connection silently fails.
			args := []string{"route", "replace", wlIP + "/32", "dev", "jetty_tun"}
			if snap.warpIP != "" {
				args = append(args, "src", snap.warpIP)
			}
			err = routeCommandRunner(routeCommandTimeout, "ip", args...)
			routeDesc = "userspace tunnel to " + info.ownerIP

		default:
			// No transport. Installing a route would silently black-hole
			// traffic, which is worse than not installing one.
			logWarnf("no transport for remote workload %s on owner %s - install the ipip kernel module or check userspace tunnel init",
				wlIP, shortID(info.ownerID, 8))
			continue
		}

		if err != nil {
			logWarnf("failed to replace route for %s via %s: %v", wlIP, routeDesc, err)
			continue
		}
		// Steady-state replaces are silent; only owner changes and first
		// installs are worth a line.
		if existingOwner, ok := prev[wlIP]; !ok || existingOwner != info.ownerID {
			logInfof("Added route for %s via %s", wlIP, routeDesc)
		}
		// Publish each route as soon as the kernel has it, so one slow
		// replace does not delay traffic to every other workload.
		a.workloadRoutesMu.Lock()
		a.workloadRoutes[wlIP] = info.ownerID
		a.workloadRoutesMu.Unlock()
	}
}
