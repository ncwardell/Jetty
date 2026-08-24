package agent

import (
	"crypto/sha256"
	"fmt"
	"os"
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
	// Serialize the whole function: stateMu.RLock below is SHARED, so
	// concurrent callers (e.g. per-workload bulk-action goroutines) would
	// otherwise race on hostsBlockHash and interleave the read-modify-write
	// of /etc/hosts.
	a.hostsMu.Lock()
	defer a.hostsMu.Unlock()

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
		a.triggerRouteReconcile()
		return
	}

	// Combine
	newLines = append(newLines, jettyLines...)

	if err := os.WriteFile(a.hostsFile, []byte(strings.Join(newLines, "\n")), 0644); err != nil {
		logWarnf("failed to update %s: %v", a.hostsFile, err)
	} else {
		a.hostsBlockHash = hash
	}

	// Update routes for remote workloads
	a.triggerRouteReconcile()
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
//  1. IPIP/GRE tunnel (a.tunnelMode == "ipip" | "gre"):
//     ip route add <wlIP>/32 dev tun_<peerID>
//  2. Userspace WS tunnel (a.tunDevice != nil): all remote workloads route
//     to the same TUN device; the TUN reader picks the destination peer
//     from a.workloadRoutes:
//     ip route add <wlIP>/32 dev jetty_tun
//  3. Last resort - direct WARP routing:
//     ip route add <wlIP>/32 via <peerWARPip> dev CloudflareWARP
//     This requires WARP to know how to forward 10.100.x.x to the peer,
//     which by default it doesn't (WARP only routes 100.96.0.0/12). This
//     branch is effectively a dead path; left in place as a "if you've
//     configured WARP yourself" escape hatch.
//
