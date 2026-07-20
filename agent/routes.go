package agent

import (
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

// =============================================================================
// Routing & Name Resolution
// =============================================================================
//
// Two functions, both called periodically (after gossip ticks, sync rounds,
// memberlist events, etc.) and after any state change that affects what's
// reachable from this node:
//
//   updateHosts          - rewrites /etc/hosts so workload names and peer
//                          hostnames resolve to mesh / WARP IPs.
//
//   updateWorkloadRoutes - installs/removes /32 routes for remote workload
//                          IPs and ensures we have a kernel tunnel to every
//                          healthy peer. Choice of route target depends on
//                          which transport mode the host supports (IPIP,
//                          GRE, userspace, or last-resort direct WARP).
//
// See docs/networking.md for the bigger picture.

// hostsField sanitizes a value before it lands in /etc/hosts. The
// upstream ingest paths (sync.go::validIngestedWorkload,
// validIngestedPeer, memberlist::validIngestedNodeMeta) already reject
// control characters, but this is the last line of defense: a single
// stray newline here would let an attacker inject arbitrary host->IP
// mappings on every node and every container (--net host).
func hostsField(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		// Drop anything that could break out of the line: whitespace
		// other than nothing, comment chars, NULs.
		if c == '\n' || c == '\r' || c == '\t' || c == ' ' || c == '#' || c == 0 {
			continue
		}
		out = append(out, c)
	}
	return string(out)
}

// updateHosts rewrites the JETTY-managed block of /etc/hosts.
//
// The block is delimited by "# JETTY START" and "# JETTY END" comments;
// anything between them is regenerated on every call. Anything outside
// the block is preserved verbatim, so hand-written entries survive.
//
// Contents of the block:
//   - This node's hostname  -> our WARP IP
//   - Each peer's name      -> peer WARP IP    (with healthy/unhealthy comment)
//   - Each workload's name  -> workload mesh IP (with local/remote comment)
//
// Because Jetty runs --net host, /etc/hosts is the host's file and is
// shared with every workload container. That is what makes hostname-based
// resolution between workloads work without DNS.
//
// Quirk: the rewrite is unconditional and runs on every gossip tick.
// Future optimization: hash the generated block and skip the write if
// nothing changed.
//
// Also calls updateWorkloadRoutes at the end so route state stays in sync
// with the host names we just published.
func (a *Agent) updateHosts() {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()

	// Read existing hosts
	data, _ := os.ReadFile(a.hostsFile)
	lines := strings.Split(string(data), "\n")

	// Filter out jetty entries
	var newLines []string
	inJettyBlock := false
	for _, line := range lines {
		if strings.Contains(line, "# JETTY START") {
			inJettyBlock = true
			continue
		}
		if strings.Contains(line, "# JETTY END") {
			inJettyBlock = false
			continue
		}
		if !inJettyBlock {
			newLines = append(newLines, line)
		}
	}

	// Build jetty block
	var jettyLines []string
	jettyLines = append(jettyLines, "# JETTY START - managed by jetty, do not edit")
	jettyLines = append(jettyLines, "# Mode: WARP mesh")
	if a.tunnelDomain != "" {
		jettyLines = append(jettyLines, fmt.Sprintf("# Tunnel: %s", a.tunnelDomain))
	}

	// Add self
	jettyLines = append(jettyLines, fmt.Sprintf("%s\t%s\t# this node", hostsField(a.ip), hostsField(a.hostname)))

	// Add peers
	for _, p := range a.state.Peers {
		status := "healthy"
		if !p.Healthy {
			status = "unhealthy"
		}
		jettyLines = append(jettyLines, fmt.Sprintf("%s\t%s\t# peer (%s)", hostsField(p.IP), hostsField(p.Name), status))
	}

	// Add workloads
	for _, w := range a.state.Workloads {
		if w.IP != "" && w.Name != "" {
			location := "local"
			if w.Owner != a.hwid {
				location = "remote"
			}
			jettyLines = append(jettyLines, fmt.Sprintf("%s\t%s\t# workload (%s)", hostsField(w.IP), hostsField(w.Name), location))
		}
	}

	jettyLines = append(jettyLines, "# JETTY END")

	// Skip the write if the JETTY block hasn't changed since last time. This
	// kills the per-gossip-tick write amplification - the file is only
	// touched when peers, workloads, or status actually changes.
	blockBytes := []byte(strings.Join(jettyLines, "\n"))
	hash := sha256.Sum256(blockBytes)
	if hash == a.hostsBlockHash {
		// Still need to reconcile remote-workload routes; that doesn't write
		// to /etc/hosts but it does need to run after state changes.
		a.updateWorkloadRoutes()
		return
	}

	// Combine
	newLines = append(newLines, jettyLines...)

	if err := os.WriteFile(a.hostsFile, []byte(strings.Join(newLines, "\n")), 0644); err != nil {
		log.Printf("Warning: failed to update %s: %v", a.hostsFile, err)
	} else {
		a.hostsBlockHash = hash
	}

	// Update routes for remote workloads
	a.updateWorkloadRoutes()
}

// updateWorkloadRoutes reconciles the host's routing table with the desired
// state of remote workloads.
//
// Required state:
//   - Every healthy peer has a kernel tunnel from us (when tunnelMode is set).
//     Tunnels are bidirectional in IPIP/GRE: encapsulated traffic arriving at
//     a peer with no return tunnel is rejected, so we maintain tunnels to all
//     healthy peers, not just owners of workloads we currently route to.
//   - Every remote workload has a /32 route pointing at the right transport.
//   - Stale routes (workload moved owners, peer went unhealthy) are removed.
//
// Transport selection (in priority order):
//   1. IPIP/GRE tunnel (a.tunnelMode == "ipip" | "gre"):
//        ip route add <wlIP>/32 dev tun_<peerID>
//   2. Userspace WS tunnel (a.tunDevice != nil): all remote workloads route
//      to the same TUN device; the TUN reader picks the destination peer
//      from a.workloadRoutes:
//        ip route add <wlIP>/32 dev jetty_tun
//   3. Last resort - direct WARP routing:
//        ip route add <wlIP>/32 via <peerWARPip> dev CloudflareWARP
//      This requires WARP to know how to forward 10.100.x.x to the peer,
//      which by default it doesn't (WARP only routes 100.96.0.0/16). This
//      branch is effectively a dead path; left in place as a "if you've
//      configured WARP yourself" escape hatch.
//
// Caller must hold a.stateMu (read or write). Internally takes
// a.workloadRoutesMu for the route-table mutation.
func (a *Agent) updateWorkloadRoutes() {
	// Skip if WARP is not connected
	if a.ip == "" {
		return
	}

	// IPIP/GRE: maintain tunnels to all known peers.
	// Userspace: keep peer-IP cache up-to-date so the TUN reader knows where
	// to ship packets.
	// Ensure/refresh the transport to EVERY known peer, healthy or not - only
	// gate on having a known WARP IP. In tunnel mode the peer health check
	// runs *through* this tunnel, so gating tunnel setup on health creates a
	// deadlock: a peer whose path broke (e.g. it moved networks and its
	// underlay changed) can never recover, because the tunnel that health
	// depends on is only rebuilt for already-healthy peers. Keeping the
	// tunnel/peer-addr current for unhealthy peers lets the health check - and
	// therefore routing - heal on its own. (Route *installation* below still
	// only targets healthy peers, so we never point a route at a dead node.)
	if a.tunnelMode != "" {
		for _, peer := range a.state.Peers {
			if peer.IP != "" {
				if err := a.ensurePeerTunnel(peer.ID, peer.IP); err != nil {
					log.Printf("Warning: failed to ensure tunnel to %s: %v", peer.Name, err)
				}
			}
		}
	} else if a.tunDevice != nil {
		for _, peer := range a.state.Peers {
			if peer.IP != "" {
				a.updateTunPeerAddr(peer.ID, peer.IP)
			}
		}
	}

	// Build map of desired routes: workload IP -> owner peer.
	type routeInfo struct {
		ownerID string
		ownerIP string
	}
	desiredRoutes := make(map[string]routeInfo) // wlIP -> routeInfo
	for _, wl := range a.state.Workloads {
		if wl.Owner == a.hwid {
			// Local workload - no route needed (handled by local iptables DNAT).
			continue
		}
		// Only route through healthy peers with known IPs.
		if peer, ok := a.state.Peers[wl.Owner]; ok && peer.IP != "" && peer.Healthy {
			desiredRoutes[wl.IP] = routeInfo{ownerID: peer.ID, ownerIP: peer.IP}
		}
	}

	a.workloadRoutesMu.Lock()
	defer a.workloadRoutesMu.Unlock()

	// Remove stale routes - either no longer needed or owner changed.
	for wlIP, ownerID := range a.workloadRoutes {
		if desired, ok := desiredRoutes[wlIP]; !ok || desired.ownerID != ownerID {
			exec.Command("ip", "route", "del", wlIP+"/32").Run()
			delete(a.workloadRoutes, wlIP)
			log.Printf("Removed route for %s (was via %s)", wlIP, shortID(ownerID, 8))
		}
	}

	// Add or refresh routes. Uses `ip route replace` (not `add`) so the
	// kernel state converges to what we want regardless of whether a
	// matching route is already installed. This makes the reconciler
	// self-healing: if a route was wiped out-of-band (TUN flap, manual
	// `ip route del`, container restart that recreated the link without
	// the agent restarting), the next reconcile re-installs it instead
	// of trusting a.workloadRoutes as authoritative.
	//
	// Without this, an entry surviving in a.workloadRoutes whose kernel
	// counterpart had been removed would never be re-added: the function
	// would short-circuit on the in-memory match and skip the kernel
	// write entirely. We observed this exact failure mode on a peer that
	// had been up for days with no IPv4 mesh routes in `ip route show`
	// despite a.workloadRoutes being populated.
	for wlIP, info := range desiredRoutes {
		var err error
		var routeDesc string

		if a.tunnelMode != "" {
			tunName := a.getTunnelName(info.ownerID)
			err = exec.Command("ip", "route", "replace", wlIP+"/32", "dev", tunName).Run()
			routeDesc = "tunnel " + tunName
		} else if a.tunDevice != nil {
			// `src <warpIP>` pins the source address the kernel picks for
			// packets via this route. Without it, jetty_tun has no IPv4
			// address so the kernel falls back to the default route's
			// source (eth0's public IP). Packets still leave correctly,
			// but the *response* from the peer's workload container is
			// addressed to a non-mesh IP - the peer's kernel routes that
			// reply via its public default route instead of back through
			// the tunnel, and the connection silently fails. Pinning src
			// to our mesh IP keeps the whole round-trip on the mesh.
			args := []string{"route", "replace", wlIP + "/32", "dev", "jetty_tun"}
			if a.ip != "" {
				args = append(args, "src", a.ip)
			}
			err = exec.Command("ip", args...).Run()
			routeDesc = "userspace tunnel to " + info.ownerIP
		} else {
			// No transport available. Don't install a route - it would silently
			// black-hole traffic. The previous "direct WARP via <peerIP>"
			// fallback didn't actually work without Cloudflare-side routing
			// configuration we don't perform, so removing it is a downgrade
			// from "looks like it works but doesn't" to "obviously doesn't".
			log.Printf("Warning: no transport for remote workload %s on owner %s - install ipip kernel module or check userspace tunnel init",
				wlIP, shortID(info.ownerID, 8))
			continue
		}

		if err == nil {
			// Only log on owner change or first install. Steady-state
			// replaces are silent to avoid log spam every reconcile.
			if existingOwner, ok := a.workloadRoutes[wlIP]; !ok || existingOwner != info.ownerID {
				log.Printf("Added route for %s via %s", wlIP, routeDesc)
			}
			a.workloadRoutes[wlIP] = info.ownerID
		} else {
			log.Printf("Warning: failed to replace route for %s via %s: %v", wlIP, routeDesc, err)
		}
	}
}
