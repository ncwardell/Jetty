package agent

import (
	"crypto/sha256"
	"fmt"
	"os"
	"sort"
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
// The generated block is hashed and the write skipped when unchanged, so
// steady-state gossip ticks do not touch the file.
//
// Triggers a route reconcile at the end so routing stays in sync with the
// names just published.
func (a *Agent) updateHosts() {
	// Serialize the whole function: stateMu.RLock below is SHARED, so
	// concurrent callers (e.g. per-workload bulk-action goroutines) would
	// otherwise race on hostsBlockHash and interleave the read-modify-write
	// of /etc/hosts.
	a.hostsMu.Lock()
	defer a.hostsMu.Unlock()

	// Snapshot first, then do the file I/O with stateMu released.
	//
	// This used to hold stateMu.RLock across os.ReadFile and os.WriteFile.
	// A read lock does not block other readers directly, but a writer queuing
	// behind it does - and Go's RWMutex then excludes every subsequent reader,
	// including apiKeyMiddleware on the API path. So a slow or hung /etc/hosts
	// write wedged the control plane the same way route programming did, just
	// one hop further round. Same invariant: no I/O under a lock.
	snap := a.snapshotHosts()

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
	if snap.tunnelDomain != "" {
		jettyLines = append(jettyLines, fmt.Sprintf("# Tunnel: %s", snap.tunnelDomain))
	}
	for _, e := range snap.entries {
		jettyLines = append(jettyLines, fmt.Sprintf("%s\t%s\t# %s",
			hostsField(e.ip), hostsField(e.name), e.note))
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

// hostsEntry is one line of the managed /etc/hosts block.
type hostsEntry struct{ ip, name, note string }

// hostsSnapshot is everything the block needs, copied out of cluster state so
// the file read-modify-write can run with no lock held.
type hostsSnapshot struct {
	tunnelDomain string
	entries      []hostsEntry
}

// snapshotHosts copies state for the /etc/hosts block. Takes stateMu.RLock
// briefly - no I/O of any kind in here, which is the whole point.
//
// Entries are emitted in a stable order (self, then peers by name, then
// workloads by name). Map iteration order is random in Go, so without this the
// rendered block differs between runs, the content hash never matches, and the
// "skip the write if nothing changed" optimisation never fires - rewriting
// /etc/hosts on every gossip tick.
func (a *Agent) snapshotHosts() hostsSnapshot {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()

	snap := hostsSnapshot{tunnelDomain: a.tunnelDomain}
	snap.entries = append(snap.entries, hostsEntry{ip: a.warpIP(), name: a.hostname, note: "this node"})

	peers := make([]hostsEntry, 0, len(a.state.Peers))
	for _, p := range a.state.Peers {
		status := "healthy"
		if !p.Healthy {
			status = "unhealthy"
		}
		peers = append(peers, hostsEntry{ip: p.IP, name: p.Name, note: "peer (" + status + ")"})
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].name < peers[j].name })

	workloads := make([]hostsEntry, 0, len(a.state.Workloads))
	for _, w := range a.state.Workloads {
		if w.IP == "" || w.Name == "" {
			continue
		}
		location := "local"
		if w.Owner != a.hwid {
			location = "remote"
		}
		workloads = append(workloads, hostsEntry{ip: w.IP, name: w.Name, note: "workload (" + location + ")"})
	}
	sort.Slice(workloads, func(i, j int) bool { return workloads[i].name < workloads[j].name })

	snap.entries = append(snap.entries, peers...)
	snap.entries = append(snap.entries, workloads...)
	return snap
}
