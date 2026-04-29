package agent

import (
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Embedded dashboard UI - automatically synced from web-ui/index.html during build
//
//go:generate cp ../web-ui/index.html dashboard.html
//go:embed dashboard.html
var dashboardHTML []byte

// Version is the current Jetty agent version (set at build time via -ldflags or defaults to "dev")
var Version = "2.0.0"

func New() (*Agent, error) {
	dataDir := getEnv("JETTY_DATA_DIR", "/data")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", dataDir, err)
	}

	a := &Agent{
		hostname:        getHostname(),
		dataDir:         dataDir,
		apiPort:         getEnvInt("JETTY_API_PORT", 6880),
		serviceCIDR:     getEnv("JETTY_SERVICE_CIDR", "10.100.0.0/16"), // CIDR for workload IPs
		joinURL:         getEnv("JETTY_JOIN", ""),
		joinToken:       getEnv("JETTY_JOIN_TOKEN", ""),
		clusterSecret:   getEnv("JETTY_SECRET", ""),
		tunnelDomain:    getEnv("JETTY_TUNNEL_DOMAIN", ""), // e.g., "cluster.example.com" - Cloudflare tunnel for API access
		tunnelHost:      getEnv("JETTY_TUNNEL_HOST", ""),   // e.g., "node1.cluster.example.com" - this node's specific subdomain
		hostShellEnabled: strings.EqualFold(getEnv("JETTY_HOST_SHELL", "false"), "true"),
		cfTunnelID:      getEnv("JETTY_CF_TUNNEL_ID", ""),  // WARP connector tunnel ID for route management
		composeDir:      filepath.Join(dataDir, "compose"),
		hostsFile:       "/etc/hosts",
		workloadRoutes:     make(map[string]string),
		ipipWarnedPeers:    make(map[string]bool),
		failoverInProgress: make(map[string]time.Time),
		maroonedLogged:     make(map[string]time.Time),
		state: &State{
			Peers:            make(map[string]*Peer),
			Workloads:        make(map[string]*Workload),
			DeletedWorkloads: make(map[string]*DeletedWorkload),        // Tombstones for deleted workloads
			CFToken:          getEnv("JETTY_CF_TOKEN", ""),             // Bootstrap tunnel token
			WarpToken:        getEnv("JETTY_WARP_CONNECTOR_TOKEN", ""), // Bootstrap WARP connector token
			EnvData:          make(map[string]string),                  // Encrypted environment variables
		},
		stopCh: make(chan struct{}),
	}

	if err := os.MkdirAll(a.composeDir, 0755); err != nil {
		return nil, fmt.Errorf("create compose dir %s: %w", a.composeDir, err)
	}

	// Load or generate HWID
	a.hwid = a.loadOrCreateHWID()

	return a, nil
}

// cleanupOrphanedState cleans up any leftover state from previous unclean shutdowns.
// This prevents accumulation of orphaned WARP devices in Cloudflare dashboard.
func (a *Agent) cleanupOrphanedState() {
	// Clean up old Jetty container from previous update
	// The update process renames the old container to {name}-old before stopping it
	output, err := exec.Command("docker", "ps", "-a", "--filter", "name=-old$", "--format", "{{.Names}}").CombinedOutput()
	if err == nil {
		for _, name := range strings.Split(strings.TrimSpace(string(output)), "\n") {
			if name != "" && strings.HasSuffix(name, "-old") {
				log.Printf("Removing old container from previous update: %s", name)
				if err := exec.Command("docker", "rm", "-f", name).Run(); err != nil {
					log.Printf("Warning: failed to remove old container %s: %v", name, err)
				}
			}
		}
	}

	// Check if there's orphaned jetty0 interface from previous run
	if err := exec.Command("ip", "link", "show", "jetty0").Run(); err == nil {
		log.Printf("Found orphaned jetty0 interface from previous run, cleaning up...")
		if err := exec.Command("ip", "link", "del", "jetty0").Run(); err != nil {
			log.Printf("Warning: failed to delete orphaned jetty0 interface: %v", err)
		}
	}

	// Clean up any orphaned IPIP tunnels (named tun_*)
	output, _ = exec.Command("ip", "tunnel", "show").CombinedOutput()
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, "tun_") {
			parts := strings.Fields(line)
			if len(parts) > 0 {
				tunName := strings.TrimSuffix(parts[0], ":")
				if err := exec.Command("ip", "tunnel", "del", tunName).Run(); err != nil {
					log.Printf("Warning: failed to delete orphaned tunnel %s: %v", tunName, err)
				} else {
					log.Printf("Cleaned up orphaned tunnel: %s", tunName)
				}
			}
		}
	}

	// Clean up any orphaned workload /32 routes inside our service CIDR.
	// Scoping to serviceCIDR avoids deleting unrelated /32 routes that other
	// software on the host may own.
	_, svcNet, err := net.ParseCIDR(a.serviceCIDR)
	if err != nil {
		log.Printf("Warning: invalid service CIDR %q, skipping orphaned route cleanup: %v", a.serviceCIDR, err)
	} else {
		output, _ = exec.Command("ip", "route", "show").CombinedOutput()
		for _, line := range strings.Split(string(output), "\n") {
			line = strings.TrimSpace(line)
			if !strings.Contains(line, "/32") {
				continue
			}
			parts := strings.Fields(line)
			if len(parts) == 0 {
				continue
			}
			ipStr, _, err := net.ParseCIDR(parts[0])
			if err != nil || ipStr == nil || !svcNet.Contains(ipStr) {
				continue
			}
			if err := exec.Command("ip", "route", "del", parts[0]).Run(); err != nil {
				log.Printf("Warning: failed to delete orphaned route %s: %v", parts[0], err)
			} else {
				log.Printf("Cleaned up orphaned route: %s", parts[0])
			}
		}
	}

	// Clean up any orphaned nftables rules from previous runs
	// Delete our jetty table if it exists (will be recreated in initWarpRules)
	// Ignore error as table may not exist
	_ = exec.Command("nft", "delete", "table", "ip", "jetty").Run()

	// Also clean up any old-style masquerade rules left in the main nat table
	// These were added before we moved to using our own table
	a.cleanupLegacyNftRules()
}

// cleanupLegacyNftRules removes old masquerade rules that were added directly to nat POSTROUTING
func (a *Agent) cleanupLegacyNftRules() {
	// Get all rules in nat POSTROUTING chain and remove CloudflareWARP masquerade rules
	output, err := exec.Command("nft", "-a", "list", "chain", "ip", "nat", "POSTROUTING").CombinedOutput()
	if err != nil {
		return // Chain might not exist
	}

	// Parse output and find handles of CloudflareWARP masquerade rules
	for _, line := range strings.Split(string(output), "\n") {
		if strings.Contains(line, "CloudflareWARP") && strings.Contains(line, "masquerade") {
			// Extract handle number from line like: "oifname "CloudflareWARP" masquerade # handle 123"
			parts := strings.Split(line, "handle")
			if len(parts) == 2 {
				handle := strings.TrimSpace(parts[1])
				if handle != "" {
					if err := exec.Command("nft", "delete", "rule", "ip", "nat", "POSTROUTING", "handle", handle).Run(); err != nil {
						log.Printf("Warning: failed to delete legacy nft rule handle %s: %v", handle, err)
					} else {
						log.Printf("Cleaned up legacy nft rule handle %s", handle)
					}
				}
			}
		}
	}
}

func (a *Agent) Start() error {
	// Record start time for uptime tracking
	a.startTime = time.Now()

	// Warn if the node has no admin key configured AND no peers (i.e.
	// fresh first-ever node and JETTY_SECRET wasn't set). On a joiner
	// the admin key arrives in the join response, so JETTY_SECRET on
	// the joining node is optional.
	a.stateMu.RLock()
	hasAdmin := a.state.AdminKey != ""
	hasPeers := len(a.state.Peers) > 0
	a.stateMu.RUnlock()
	if !hasAdmin && !hasPeers && a.joinURL == "" && a.clusterSecret == "" {
		log.Printf("!!! WARNING: JETTY_SECRET is not set and no cluster state exists - the API will be UNAUTHENTICATED. !!!")
		log.Printf("!!! Set JETTY_SECRET to a strong random value before exposing this node to a network.              !!!")
	}

	if a.hostShellEnabled {
		log.Printf("!!! Host shell endpoint /api/host/shell is ENABLED - anyone with the cluster admin key can run shell commands on this host. !!!")
	}

	// Record whether WARP was already running before we touched anything.
	// If it was, the operator owns it and we must not tear it down on Stop().
	if _, err := net.InterfaceByName("CloudflareWARP"); err == nil {
		a.warpPreexisting = true
		log.Printf("CloudflareWARP interface already present - Jetty will not tear it down on shutdown")
	}

	// Clean up any orphaned state from previous unclean shutdown
	a.cleanupOrphanedState()

	// Cache public IP at startup (avoid slow lookups on every health check)
	a.publicIP = getPublicIP()

	// Try to detect WARP IP (may not be available yet if joining)
	a.detectWarpIP()

	// Load saved state
	a.loadState()

	// One-shot upgrade from old (Argon2id-salt-derived) to new (raw
	// EncryptionKey) env-data encryption. No-op on fresh state and on
	// already-migrated state.
	a.migrateLegacyEnvData()

	// Bootstrap the keys this node owns. ensureSelfAPIKey + ensureEncryptionKey
	// are no-ops on a state that already has them (e.g. after a join). The
	// AdminKey is bootstrapped from JETTY_SECRET on the first node only;
	// joiners get it from the /api/join response.
	a.bootstrapKeys()

	// Check if we need to join a cluster first (before WARP is configured)
	a.stateMu.RLock()
	hasClusterState := a.state.CFToken != "" || a.state.WarpToken != "" || len(a.state.Peers) > 0
	needsJoin := a.joinURL != "" && !hasClusterState
	a.stateMu.RUnlock()

	if needsJoin {
		// Join cluster first - this will give us the WARP token
		// We join via the tunnel URL, which doesn't require WARP
		log.Printf("Joining cluster to obtain WARP configuration...")
		if err := a.joinCluster(); err != nil {
			return fmt.Errorf("join: %w", err)
		}
		// After join, WARP should be configured - detect IP
		a.detectWarpIP()
	}

	// Now we should have WARP IP (either from startup or after join)
	if a.ip == "" {
		return fmt.Errorf("WARP IP not detected - ensure WARP is connected")
	}

	// Initialize network (dummy interface for workload IPs)
	if err := a.initNetwork(); err != nil {
		return fmt.Errorf("network: %w", err)
	}

	// Init WARP nft rules
	if err := a.initWarpRules(); err != nil {
		log.Printf("Warning: failed to init WARP rules: %v", err)
	}

	// Sync with existing peers if not a fresh join
	if !needsJoin {
		a.syncStateOnStartup()
	}

	// Announce our current IP to peers (handles IP changes during restart/update)
	go a.announceOurIP()

	// Auto-start owned workloads (only those we still own after sync)
	a.autostartWorkloads()

	// Update hosts file
	a.updateHosts()

	// Start API first (so cloudflared can connect to it)
	go a.runAPI()

	// Wait for API to be ready before starting cloudflared
	a.waitForAPI()

	// Start Cloudflare tunnel if configured
	if err := a.startCloudflared(); err != nil {
		log.Printf("Warning: failed to start cloudflared: %v", err)
	}

	// Initialize memberlist for cluster membership and failure detection
	ml, err := a.initMemberlist()
	if err != nil {
		log.Printf("Warning: memberlist init failed: %v - falling back to HTTP gossip", err)
		// Fall back to HTTP-based gossip if memberlist fails
		go a.gossipLoop()
	} else {
		a.memberlist = ml
		// Join known peers
		a.joinMemberlistPeers()
		// Start sync loop (for periodic full state sync, tombstone GC)
		go a.memberlistSyncLoop()
	}

	// Start failover monitor
	go a.failoverLoop()

	// Start IP monitor (detects WARP IP changes and re-announces)
	go a.ipMonitorLoop()

	// Start CPU sampling (background updates for accurate metrics)
	go a.cpuSampleLoop()

	// Keep compose override files (for cross-workload DNS) in sync with the
	// cluster's workload set. Cheap when the set hasn't changed.
	go a.hostsOverrideReconcileLoop()

	mode := "warp (" + a.ip + ")"
	if a.tunnelDomain != "" {
		mode += " + tunnel (" + a.tunnelDomain + ")"
	}
	log.Printf("Jetty started: %s (%s) @ %s [mode: %s]", a.hostname, shortID(a.hwid, 12), a.ip, mode)
	return nil
}


func (a *Agent) Stop() {
	log.Printf("Shutting down Jetty...")

	// Hand off owned, revivable workloads to peers BEFORE we tear down the
	// network. Each workload is "released" by clearing our ownership and
	// bumping the version - on receipt, peers see an orphaned revivable
	// workload and the deterministic election picks a new owner. Combined
	// with the memberlist-driven NotifyLeave hook on peers, this typically
	// results in a workload being picked up sub-second after we drain it.
	a.gracefulDrain()

	close(a.stopCh)
	a.stopCloudflared()
	a.saveState()

	// Clean up network resources
	a.cleanupNetwork()
}

// gracefulDrain announces our departure to peers immediately so they can
// start claiming our owned workloads while we still serve them, instead
// of waiting for the heartbeat-based dead-detection.
//
// The bulk of the work is done by memberlist.Leave(), which broadcasts a
// "I'm leaving" notification to peers. Peers' NotifyLeave hook (in
// memberlist.go) calls checkFailover immediately, so peers see our
// workloads as orphans and claim them via the deterministic election.
// Combined with image pre-pull on those peers, the typical handoff is
// sub-second from the operator's perspective.
//
// Bounded by a short deadline so a hung cluster can't block planned
// shutdowns. If memberlist isn't available we just rely on the existing
// HTTP-gossip backstop, which is slower but still correct.
func (a *Agent) gracefulDrain() {
	a.stateMu.RLock()
	var ownedRevivable int
	for _, wl := range a.state.Workloads {
		if wl.Owner == a.hwid && wl.Revive {
			ownedRevivable++
		}
	}
	a.stateMu.RUnlock()

	if a.memberlist == nil {
		if ownedRevivable > 0 {
			log.Printf("Graceful drain: %d revivable workload(s) will be picked up by peers via gossip (memberlist unavailable)", ownedRevivable)
		}
		return
	}

	if ownedRevivable > 0 {
		log.Printf("Graceful drain: announcing departure to peers (%d revivable workload(s) to hand off)", ownedRevivable)
	} else {
		log.Printf("Graceful drain: announcing departure (no revivable workloads to hand off)")
	}

	// Leave() blocks until the broadcast is sent to a few peers; bounded
	// by the deadline. After this returns, peers have already received
	// our departure and started claiming.
	if err := a.memberlist.Leave(2 * time.Second); err != nil {
		log.Printf("Graceful drain: memberlist Leave failed: %v (peers will detect via timeout)", err)
		return
	}

	// Give peers a moment to actually start their replacement deploys
	// before we tear our own network down. Without this we'd remove our
	// /32 routes and DNAT rules right as peers are getting their image
	// pulled, potentially cratering in-flight requests.
	time.Sleep(500 * time.Millisecond)
}

// cleanupNetwork removes all Jetty-created network resources
func (a *Agent) cleanupNetwork() {
	log.Printf("Cleaning up network resources...")

	// Clean up workload routes first
	a.workloadRoutesMu.Lock()
	for wlIP := range a.workloadRoutes {
		exec.Command("ip", "route", "del", wlIP+"/32").Run()
		log.Printf("Removed route for %s", wlIP)
	}
	a.workloadRoutes = make(map[string]string)
	a.workloadRoutesMu.Unlock()

	// Clean up IPIP tunnels to peers
	a.stateMu.RLock()
	for _, peer := range a.state.Peers {
		tunName := a.getTunnelName(peer.ID)
		if err := exec.Command("ip", "tunnel", "del", tunName).Run(); err == nil {
			log.Printf("Removed tunnel %s", tunName)
		}
	}

	// Clean up iptables rules for all workloads
	for _, wl := range a.state.Workloads {
		if wl.Owner == a.hwid {
			// Find container IP and remove DNAT rules
			out, _ := exec.Command("docker", "ps", "-q", "-f", "label=com.docker.compose.project=jetty_"+wl.Name).Output()
			if len(out) > 0 {
				containerID := strings.Split(strings.TrimSpace(string(out)), "\n")[0]
				if containerID != "" {
					out, _ = exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerID).Output()
					containerIP := strings.TrimSpace(string(out))
					if containerIP != "" {
						exec.Command("iptables", "-t", "nat", "-D", "PREROUTING", "-d", wl.IP, "-j", "DNAT", "--to", containerIP).Run()
						exec.Command("iptables", "-t", "nat", "-D", "OUTPUT", "-d", wl.IP, "-j", "DNAT", "--to", containerIP).Run()
						log.Printf("Removed iptables rules for %s (%s -> %s)", wl.Name, wl.IP, containerIP)
					}
				}
			}
		}
	}
	a.stateMu.RUnlock()

	// Clean up userspace tunnel if active
	a.cleanupUserspaceTunnel()

	// Clean up nftables rules
	a.cleanupWarpRules()

	// Remove jetty0 interface (this also removes all IPs bound to it)
	if err := exec.Command("ip", "link", "del", "jetty0").Run(); err != nil {
		log.Printf("Warning: could not remove jetty0 interface: %v", err)
	} else {
		log.Printf("Removed jetty0 interface")
	}

	// If WARP was already running before Jetty started, the operator owns it -
	// leave it alone. Otherwise, tear down the WARP state we set up.
	if a.warpPreexisting {
		log.Printf("Leaving WARP intact (was already running before Jetty started)")
		return
	}

	// Disconnect WARP (don't delete registration - state is persisted in /data/warp and reused on restart)
	log.Printf("Disconnecting WARP...")
	if output, err := exec.Command("warp-cli", "--accept-tos", "disconnect").CombinedOutput(); err != nil {
		log.Printf("WARP disconnect: %v (%s)", err, strings.TrimSpace(string(output)))
	} else {
		log.Printf("WARP disconnected")
	}
	// Registration is preserved in /data/warp for reuse across restarts/updates

	// Clean up WARP network modifications (important for --net host mode)
	// These persist on the host after container stops, breaking SSH/git
	exec.Command("ip", "link", "delete", "CloudflareWARP").Run()
	exec.Command("nft", "delete", "table", "inet", "cloudflare-warp").Run()
	exec.Command("ip", "route", "del", "100.96.0.0/12").Run()
	log.Printf("Removed WARP network modifications")
}

// =============================================================================
// HWID
// =============================================================================

func (a *Agent) loadOrCreateHWID() string {
	hwidFile := filepath.Join(a.dataDir, "hwid")

	// Try to load existing
	if data, err := os.ReadFile(hwidFile); err == nil {
		return strings.TrimSpace(string(data))
	}

	// Try machine-id
	if data, err := os.ReadFile("/etc/machine-id"); err == nil {
		hwid := strings.TrimSpace(string(data))
		os.WriteFile(hwidFile, []byte(hwid), 0600)
		return hwid
	}

	// Generate random
	b := make([]byte, 16)
	rand.Read(b)
	hwid := hex.EncodeToString(b)
	os.WriteFile(hwidFile, []byte(hwid), 0600)
	return hwid
}








































