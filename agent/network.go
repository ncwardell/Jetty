package agent

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// =============================================================================
// Host Network Setup
// =============================================================================
//
// This file owns everything that touches the host's network state below the
// HTTP API: the jetty0 dummy interface, iptables/nftables rules, the
// IPIP/GRE peer tunnel pool, and the mesh-IP allocator.
//
// See docs/networking.md for the full picture of how packets traverse the
// cluster. The short version:
//
//   - jetty0 is a dummy interface where every local workload's mesh IP is
//     bound. The kernel claims those IPs as "ours", iptables DNAT then
//     forwards them to the actual container bridge IP.
//
//   - Cross-node workload traffic uses the userspace tunnel by default
//     (see tunnel.go). Packets to remote workloads route to the jetty_tun
//     TUN device; the reader ships them to the owning node over a
//     WebSocket on the API port. This works through anything that passes
//     TCP - Cloudflare WARP, residential ISPs, corporate firewalls.
//
//   - JETTY_TUNNEL_MODE=ipip (or gre) opts into kernel encapsulation when
//     the operator knows the network between peers passes raw IP. Higher
//     throughput, lower CPU. Doesn't survive WARP or most public-internet
//     paths because raw-IP-protocol forwarding is widely filtered.

// =============================================================================
// Network Interface (Dummy interface for mesh IP binding)
// =============================================================================

// initNetwork builds the host-side networking scaffolding the rest of the
// agent depends on. Called once during Start() after WARP has come up.
//
// What it sets up, in order:
//
//  1. jetty0 - a "dummy" interface (a kernel concept: claims IPs but
//     forwards nothing). Every local workload's mesh IP gets bound to this
//     interface so the kernel says "yes, this host owns 10.100.x.y" for
//     incoming packets.
//
//  2. /proc/sys/net/ipv4/ip_forward = 1 - lets the kernel route between
//     Docker bridges, jetty0, and the inter-node tunnels.
//
//  3. iptables FORWARD ACCEPT for serviceCIDR (both directions) - inserted
//     at positions 1-2 so they sit ahead of Docker's bridge-isolation rules,
//     which would otherwise drop cross-bridge mesh traffic.
//
//  4. iptables nat POSTROUTING MASQUERADE for serviceCIDR - rewrites the
//     source IP of mesh-bound packets from the container's bridge IP to the
//     host IP so the return path works.
//
//  5. JETTY_TUNNEL_MODE - picks userspace (default), IPIP, or GRE for
//     cross-node workload traffic.
//
//  6. initUserspaceTunnel - always started, including when an opt-in
//     kernel mode is selected, because peers running in userspace mode
//     need a server to ship packets to.
//
// Caveat: rule ordering is fragile. Docker can re-insert FORWARD rules on
// daemon restart and push ours down the chain. There is no periodic
// re-application; on Docker restart, mesh-to-mesh forwarding may break
// until the agent is bounced.
func (a *Agent) initNetwork() error {
	// Verify we have a WARP IP (should be detected in Start())
	if a.ip == "" {
		return fmt.Errorf("WARP IP not detected - ensure WARP is connected")
	}

	// Create dummy interface for workload IPs
	// Workloads get IPs from serviceCIDR (10.100.x.x) that are routed via WARP
	// This interface is where those IPs are bound so traffic can be DNATed to containers
	exec.Command("ip", "link", "del", "jetty0").Run() // Clean up any existing
	if err := exec.Command("ip", "link", "add", "dev", "jetty0", "type", "dummy").Run(); err != nil {
		return fmt.Errorf("create dummy interface for workload IPs: %w", err)
	}
	exec.Command("ip", "link", "set", "up", "dev", "jetty0").Run()

	// Enable IP forwarding so the host can route between Docker bridges,
	// the jetty0 dummy interface, and the IPIP/GRE/userspace tunnels.
	// Without this, the kernel drops packets that need to traverse interfaces.
	os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1"), 0644)

	// ---------------------------------------------------------------
	// Inter-workload mesh routing rules (see docs/networking.md)
	// ---------------------------------------------------------------
	//
	// Each workload runs in its own docker-compose project on its own bridge
	// network. A container in workload A reaches workload B by its mesh IP
	// (10.100.x.y), which appears in /etc/hosts as the workload's name. For that
	// to work end-to-end:
	//
	//   1. FORWARD must permit traffic to/from the service CIDR. Docker installs
	//      its own FORWARD rules that drop cross-bridge traffic by default, so we
	//      insert ACCEPT rules at positions 1-2 (before Docker's DROPs). Order
	//      matters: -I FORWARD 1 puts us first in the chain.
	//
	//   2. POSTROUTING must MASQUERADE traffic destined for the service CIDR.
	//      When the packet leaves the host (either through a tunnel to a remote
	//      workload, or via DNAT to a local container), the source must be the
	//      host's address - not the originating container's bridge IP - so the
	//      response can route back. Without this, the response gets stuck because
	//      the remote side has no route to 172.x.x.x bridge ranges.
	//
	// Caveat: containers see the host's IP as the source, not the original
	// client. Real client IP isn't preserved. Acceptable trade-off for routing
	// simplicity in a mesh.
	a.applyMeshIptablesRules(true /* verbose */)

	// Cross-node tunnel mode. Default is the userspace tunnel
	// (jetty_tun) - it ships packets over a WebSocket on the API port,
	// so it survives any carrier that passes TCP, including Cloudflare
	// WARP. Kernel IPIP/GRE are higher-throughput but require the
	// path between peers to forward raw IP protocols (4 and 47), which
	// WARP and most ISPs/firewalls don't. Operators with end-to-end
	// control of the network can opt in via JETTY_TUNNEL_MODE.
	switch strings.ToLower(getEnv("JETTY_TUNNEL_MODE", "")) {
	case "ipip":
		a.tunnelMode = "ipip"
		logInfof("Tunnel mode: IPIP (opt-in; requires the path between peers to forward IP protocol 4)")
	case "gre":
		a.tunnelMode = "gre"
		logInfof("Tunnel mode: GRE (opt-in; requires the path between peers to forward IP protocol 47)")
	default:
		a.tunnelMode = ""
		logInfof("Tunnel mode: userspace (jetty_tun)")
	}

	// ALWAYS start userspace tunnel listener - even if we have IPIP for sending,
	// we need to receive packets from peers that don't have IPIP (e.g., ChromeOS)
	if err := a.initUserspaceTunnel(); err != nil {
		logWarnf("userspace tunnel failed: %v", err)
		if a.tunnelMode == "" {
			logWarnf("no tunnel mode available - cross-node workload routing disabled")
		}
	}

	// Re-apply iptables rules periodically. Docker can re-insert its own
	// FORWARD rules above ours on daemon restart, which silently breaks
	// inter-workload mesh traffic. Idempotent: applyMeshIptablesRules is a
	// no-op when rules already exist.
	goSupervised("iptablesMaintenanceLoop", a.iptablesMaintenanceLoop)

	logInfof("Network ready: %s (WARP), workload interface: jetty0", a.ip)
	return nil
}

// applyMeshIptablesRules ensures the FORWARD ACCEPT and POSTROUTING
// MASQUERADE rules for serviceCIDR exist. Idempotent - uses iptables -C
// to test before inserting, so it's safe to call repeatedly.
//
// Called once at startup with verbose=true and from iptablesMaintenanceLoop
// every 60s with verbose=false.
func (a *Agent) applyMeshIptablesRules(verbose bool) {
	// FORWARD: -d serviceCIDR ACCEPT  (rule slot 1)
	if err := exec.Command("iptables", "-C", "FORWARD", "-d", a.serviceCIDR, "-j", "ACCEPT").Run(); err != nil {
		if err := exec.Command("iptables", "-I", "FORWARD", "1", "-d", a.serviceCIDR, "-j", "ACCEPT").Run(); err != nil {
			if verbose {
				logWarnf("failed to add FORWARD ACCEPT (dst=%s): %v", a.serviceCIDR, err)
			}
		} else if !verbose {
			logInfof("Re-inserted FORWARD ACCEPT (dst=%s) - had been removed", a.serviceCIDR)
		}
	}

	// FORWARD: -s serviceCIDR ACCEPT  (rule slot 2)
	if err := exec.Command("iptables", "-C", "FORWARD", "-s", a.serviceCIDR, "-j", "ACCEPT").Run(); err != nil {
		if err := exec.Command("iptables", "-I", "FORWARD", "2", "-s", a.serviceCIDR, "-j", "ACCEPT").Run(); err != nil {
			if verbose {
				logWarnf("failed to add FORWARD ACCEPT (src=%s): %v", a.serviceCIDR, err)
			}
		} else if !verbose {
			logInfof("Re-inserted FORWARD ACCEPT (src=%s) - had been removed", a.serviceCIDR)
		}
	}

	// nat POSTROUTING: -d serviceCIDR MASQUERADE
	if err := exec.Command("iptables", "-t", "nat", "-C", "POSTROUTING", "-d", a.serviceCIDR, "-j", "MASQUERADE").Run(); err != nil {
		if err := exec.Command("iptables", "-t", "nat", "-A", "POSTROUTING", "-d", a.serviceCIDR, "-j", "MASQUERADE").Run(); err != nil {
			if verbose {
				logWarnf("failed to add MASQUERADE for container-to-mesh traffic: %v", err)
			}
		} else if !verbose {
			logInfof("Re-inserted POSTROUTING MASQUERADE (dst=%s) - had been removed", a.serviceCIDR)
		}
	}
}

// iptablesMaintenanceLoop re-applies our mesh iptables rules every minute.
// Docker can re-insert its own FORWARD rules ahead of ours on daemon restart;
// without periodic re-application the mesh silently breaks until the agent
// is bounced.
func (a *Agent) iptablesMaintenanceLoop() {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-tick.C:
			a.applyMeshIptablesRules(false /* verbose=false: only log on actual reapply */)
		}
	}
}

// detectTunnelMode checks which tunnel modes are available and returns the best one.
// Tries to load kernel modules if needed, then tests IPIP first (most efficient), then GRE.
// Returns "ipip", "gre", or "" if no tunnel mode is available.
func (a *Agent) detectTunnelMode() string {
	// Try to load kernel modules (requires CAP_SYS_MODULE or running as root)
	// This is often needed in containers where modules aren't auto-loaded
	exec.Command("modprobe", "ipip").Run()
	exec.Command("modprobe", "ip_gre").Run()

	testName := "jetty_tun_test"
	exec.Command("ip", "tunnel", "del", testName).Run() // Clean up any existing

	// Try IPIP first (most efficient)
	err := exec.Command("ip", "tunnel", "add", testName, "mode", "ipip",
		"local", "127.0.0.1", "remote", "127.0.0.2").Run()
	if err == nil {
		exec.Command("ip", "tunnel", "del", testName).Run()
		return "ipip"
	}

	// Try GRE as fallback (uses different kernel module)
	err = exec.Command("ip", "tunnel", "add", testName, "mode", "gre",
		"local", "127.0.0.1", "remote", "127.0.0.2").Run()
	if err == nil {
		exec.Command("ip", "tunnel", "del", testName).Run()
		logInfof("Using GRE tunnels for cross-node routing (IPIP unavailable)")
		return "gre"
	}

	// No tunnel mode available
	return ""
}

// initWarpRules sets up nftables rules for WARP traffic routing.
// This includes:
// 1. Masquerade rule for response routing through CloudflareWARP
// 2. Route for WARP client IP range (100.96.0.0/12)
// 3. Rules to allow cloudflared QUIC traffic through WARP firewall
func (a *Agent) initWarpRules() error {
	// First, clean up any existing Jetty nft rules to avoid duplicates
	a.cleanupWarpRules()

	// Create Jetty's own nft table for our rules (easier to clean up)
	exec.Command("nft", "add", "table", "ip", "jetty").Run()
	exec.Command("nft", "add", "chain", "ip", "jetty", "postrouting",
		"{ type nat hook postrouting priority srcnat; }").Run()

	// Add masquerade rule for traffic going out through CloudflareWARP
	// This ensures responses from our mesh IPs route back through WARP
	if err := exec.Command("nft", "add", "rule", "ip", "jetty", "postrouting",
		"oifname", "CloudflareWARP", "masquerade").Run(); err != nil {
		logWarnf("failed to add WARP masquerade rule: %v", err)
	}

	// Add route for WARP client IP range to CloudflareWARP interface
	// This ensures we can reach other WARP clients
	exec.Command("ip", "route", "add", "100.96.0.0/12", "dev", "CloudflareWARP").Run()

	// WARP installs a default-DROP firewall that breaks SSH, git, cloudflared,
	// etc. We only need WARP for routing to the mesh CIDR, not for firewalling,
	// so normally we delete the cloudflare-warp nft table outright.
	//
	// Exception: if WARP was already running before Jetty started, the
	// operator owns it. Don't touch their firewall - they may depend on it.
	if a.warpPreexisting {
		logInfof("Leaving WARP firewall table alone (WARP was pre-existing)")
	} else if _, err := exec.Command("nft", "list", "table", "inet", "cloudflare-warp").Output(); err == nil {
		logInfof("Removing WARP firewall table (routing still works without it)...")
		if err := exec.Command("nft", "delete", "table", "inet", "cloudflare-warp").Run(); err != nil {
			logWarnf("failed to delete cloudflare-warp table: %v", err)
		} else {
			logInfof("WARP firewall table removed")
		}
	}

	logInfof("WARP nft rules initialized (table: ip jetty)")
	return nil
}

// cleanupWarpRules removes all Jetty nftables rules
func (a *Agent) cleanupWarpRules() {
	// Delete our entire table - this removes all our rules cleanly
	exec.Command("nft", "delete", "table", "ip", "jetty").Run()
}

// =============================================================================
// Peer-to-peer tunnels (kernel IPIP / GRE)
// =============================================================================
//
// Each healthy peer gets one tunnel from us, named tun_<first-8-of-peerID>.
// updateWorkloadRoutes installs /32 routes for remote workload IPs that
// point at the appropriate tun_<peerID>; the kernel encapsulates outbound
// packets in IPIP (or GRE) headers addressed to the peer's WARP IP, the
// peer kernel decapsulates them, and host iptables DNATs the inner packet
// to the local container.
//
// IPIP is preferred (20-byte overhead, simplest); GRE is the fallback when
// the ipip module isn't available. The userspace WebSocket tunnel
// (tunnel.go) is the last resort when neither kernel module is loadable.
//
// Tunnels are bidirectional by necessity: an IPIP-encapsulated packet
// arriving at a peer with no return tunnel back to us is rejected by the
// kernel. We create tunnels to every healthy peer, not just owners of
// workloads we route to.

// ensurePeerTunnel creates a kernel tunnel to a peer if one doesn't already
// exist with the right remote. Idempotent: cheap to call repeatedly.
//
// Returns nil if there's no work to do (peer has no IP, or we have no
// tunnel mode). Returns the underlying `ip tunnel` error if creation
// fails - callers log and continue.
func (a *Agent) ensurePeerTunnel(peerID, peerIP string) error {
	if a.ip == "" || peerIP == "" {
		return nil // Can't create tunnel without IPs
	}

	// Skip if no tunnel mode available (kernel modules not loaded)
	if a.tunnelMode == "" {
		return nil
	}

	// Tunnel name based on peer ID (truncated for interface name limit)
	tunName := "tun_" + shortID(peerID, 8)

	// Check if tunnel already exists with correct config
	out, _ := runBoundedOutput(routeCommandTimeout, "ip", "tunnel", "show", tunName)
	if strings.Contains(string(out), peerIP) {
		return nil // Tunnel exists with correct remote
	}

	// Delete existing tunnel if it has wrong config
	routeCommandRunner(routeCommandTimeout, "ip", "tunnel", "del", tunName)

	// Create tunnel using detected mode (ipip or gre): local=our WARP IP, remote=peer's WARP IP
	if out, err := runBoundedOutput(routeCommandTimeout, "ip", "tunnel", "add", tunName,
		"mode", a.tunnelMode, "local", a.ip, "remote", peerIP); err != nil {
		return fmt.Errorf("create %s tunnel to %s: %s", a.tunnelMode, peerIP, strings.TrimSpace(string(out)))
	}

	// Bring up the tunnel interface
	if out, err := runBoundedOutput(routeCommandTimeout, "ip", "link", "set", "up", "dev", tunName); err != nil {
		routeCommandRunner(routeCommandTimeout, "ip", "tunnel", "del", tunName)
		return fmt.Errorf("bring up tunnel %s: %s", tunName, strings.TrimSpace(string(out)))
	}

	logInfof("Created %s tunnel %s to peer %s (%s)", strings.ToUpper(a.tunnelMode), tunName, shortID(peerID, 8), peerIP)
	return nil
}

// removePeerTunnel removes the IPIP tunnel to a peer.
func (a *Agent) removePeerTunnel(peerID string) {
	tunName := "tun_" + shortID(peerID, 8)
	if err := routeCommandRunner(routeCommandTimeout, "ip", "tunnel", "del", tunName); err == nil {
		logInfof("Removed tunnel %s", tunName)
	}
}

// getTunnelName returns the tunnel interface name for a peer.
// Truncates the peer's HWID to 8 hex chars to fit the kernel's IFNAMSIZ
// (15-char limit on interface names). HWIDs are 32 hex chars from
// machine-id, so collisions in the first 8 are vanishingly unlikely but
// not strictly impossible across thousands of nodes.
func (a *Agent) getTunnelName(peerID string) string {
	return "tun_" + shortID(peerID, 8)
}

// deriveMeshIP picks a deterministic candidate mesh IP for a node based on
// a hash of its ID. Used as a starting point during cluster join; the
// caller should run the result through isIPTaken before committing
// (deriveMeshIPWithCollisionCheck does this).
//
// The hash is FNV-style "h = h*31 + c", reduced modulo the host range of
// the service CIDR. Two different IDs can hash to the same IP; the
// collision-checked variant walks forward to find an unused slot.
func (a *Agent) deriveMeshIP(id string) string {
	_, network, _ := net.ParseCIDR(a.serviceCIDR)
	if network == nil {
		return "10.100.0.1"
	}

	h := 0
	for _, c := range id {
		h = h*31 + int(c)
	}
	if h < 0 {
		h = -h
	}

	ones, bits := network.Mask.Size()
	max := (1 << (bits - ones)) - 2
	host := (h % max) + 1

	ip := network.IP.To4()
	ipInt := int(ip[0])<<24 | int(ip[1])<<16 | int(ip[2])<<8 | int(ip[3])
	ipInt += host

	return fmt.Sprintf("%d.%d.%d.%d", (ipInt>>24)&0xff, (ipInt>>16)&0xff, (ipInt>>8)&0xff, ipInt&0xff)
}

// isIPInCIDR checks if an IP address is within the mesh CIDR range.
func (a *Agent) isIPInCIDR(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	_, network, err := net.ParseCIDR(a.serviceCIDR)
	if err != nil {
		return false
	}
	return network.Contains(ip)
}

// isIPTaken checks if an IP is already used by this node, a peer, or a workload.
// Caller must hold stateMu lock (read or write).
func (a *Agent) isIPTaken(ipStr string) bool {
	// Check if it's our own mesh IP
	if ipStr == a.ip {
		return true
	}

	// Check peers
	for _, p := range a.state.Peers {
		if p.IP == ipStr {
			return true
		}
	}

	// Check workloads
	if _, exists := a.state.Workloads[ipStr]; exists {
		return true
	}

	return false
}

// isNodeAllowed checks if a node (by ID or name) is allowed to run a workload.
// Checks both the allowed_nodes list AND architecture compatibility.
// Empty or ["*"] allowed_nodes means all nodes are allowed (but arch must still match).
func (a *Agent) isNodeAllowed(wl *Workload, nodeID, nodeName, nodeArch string) bool {
	// First check architecture compatibility
	if nodeArch != "" && !wl.canRunOnArch(nodeArch) {
		return false
	}

	// Empty list or nil means all nodes allowed
	if len(wl.AllowedNodes) == 0 {
		return true
	}

	for _, allowed := range wl.AllowedNodes {
		// Wildcard means all nodes allowed
		if allowed == "*" || allowed == "all" {
			return true
		}
		// Check by ID or name
		if allowed == nodeID || allowed == nodeName {
			return true
		}
	}
	return false
}

// isThisNodeAllowed checks if this node is allowed to run a workload.
func (a *Agent) isThisNodeAllowed(wl *Workload) bool {
	return a.isNodeAllowed(wl, a.hwid, a.hostname, runtime.GOARCH)
}

// isWorkloadNameTaken checks if a workload name is already in use.
// Caller must hold stateMu lock (read or write).
func (a *Agent) isWorkloadNameTaken(name string) bool {
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			return true
		}
	}
	return false
}

// findAllowedNode finds a healthy peer that is allowed to run this workload.
// Checks both allowed_nodes list and architecture compatibility.
// Returns nil if no suitable node is found.
// Caller must hold stateMu lock (read).
func (a *Agent) findAllowedNode(wl *Workload) *Peer {
	for _, p := range a.state.Peers {
		if p.Healthy && a.isNodeAllowed(wl, p.ID, p.Name, p.Arch) {
			return p
		}
	}
	return nil
}

// allocateServiceIP finds the next available IP in the service CIDR for a workload.
// Workloads get their own IPs that are routed via WARP.
// Returns empty string if no IPs are available.
// Caller must hold stateMu lock (read or write).
func (a *Agent) allocateServiceIP() string {
	_, network, err := net.ParseCIDR(a.serviceCIDR)
	if err != nil {
		return ""
	}

	ones, bits := network.Mask.Size()
	maxHosts := (1 << (bits - ones)) - 2 // Exclude network and broadcast

	baseIP := network.IP.To4()
	baseInt := int(baseIP[0])<<24 | int(baseIP[1])<<16 | int(baseIP[2])<<8 | int(baseIP[3])

	// Try to find an available IP, starting from host 1
	for host := 1; host <= maxHosts; host++ {
		ipInt := baseInt + host
		ipStr := fmt.Sprintf("%d.%d.%d.%d", (ipInt>>24)&0xff, (ipInt>>16)&0xff, (ipInt>>8)&0xff, ipInt&0xff)

		if !a.isIPTaken(ipStr) {
			return ipStr
		}
	}

	return "" // No available IPs
}

// deriveMeshIPWithCollisionCheck derives a mesh IP for a node, checking for collisions.
// If the derived IP is already taken, it tries sequential IPs until finding an available one.
// Caller must hold stateMu lock (read or write).
func (a *Agent) deriveMeshIPWithCollisionCheck(id string) string {
	_, network, _ := net.ParseCIDR(a.serviceCIDR)
	if network == nil {
		return "10.100.0.1"
	}

	// Start with hash-derived IP
	h := 0
	for _, c := range id {
		h = h*31 + int(c)
	}
	if h < 0 {
		h = -h
	}

	ones, bits := network.Mask.Size()
	maxHosts := (1 << (bits - ones)) - 2
	startHost := (h % maxHosts) + 1

	baseIP := network.IP.To4()
	baseInt := int(baseIP[0])<<24 | int(baseIP[1])<<16 | int(baseIP[2])<<8 | int(baseIP[3])

	// Try the hash-derived IP first, then scan for available IPs
	for i := 0; i < maxHosts; i++ {
		host := ((startHost + i - 1) % maxHosts) + 1
		ipInt := baseInt + host
		ipStr := fmt.Sprintf("%d.%d.%d.%d", (ipInt>>24)&0xff, (ipInt>>16)&0xff, (ipInt>>8)&0xff, ipInt&0xff)

		// For node IPs, only check against other peers (not our own meshIP which isn't set yet)
		taken := false
		for _, p := range a.state.Peers {
			if p.IP == ipStr {
				taken = true
				break
			}
		}
		if !taken {
			return ipStr
		}
	}

	// Fallback (shouldn't happen unless network is full)
	return fmt.Sprintf("%d.%d.%d.%d", (baseInt+1)>>24&0xff, (baseInt+1)>>16&0xff, (baseInt+1)>>8&0xff, (baseInt+1)&0xff)
}
