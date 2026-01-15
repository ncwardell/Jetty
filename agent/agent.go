package agent

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/ncwardell/jetty/docs"
	"github.com/songgao/water"
	httpSwagger "github.com/swaggo/http-swagger"
)

// Embedded dashboard UI - automatically synced from web-ui/index.html during build
//go:generate cp ../web-ui/index.html dashboard.html
//go:embed dashboard.html
var dashboardHTML []byte

// Shared HTTP client with timeout to prevent blocking on hung peers
var httpClient = &http.Client{
	Timeout: 30 * time.Second,
}

// Shorter timeout clients for peer health checks
var peerClient = &http.Client{
	Timeout: 5 * time.Second, // Normal peer query timeout
}
var unhealthyPeerClient = &http.Client{
	Timeout: 1 * time.Second, // Very short timeout for known-unhealthy peers
}

// Valid workload name pattern (alphanumeric, dash, underscore only)
var validNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// Version is the current Jetty agent version (set at build time via -ldflags or defaults to "dev")
var Version = "2.0.0"

// =============================================================================
// Types
// =============================================================================

type Peer struct {
	ID       string    `json:"id"`        // HWID
	Name     string    `json:"name"`      // Hostname
	IP       string    `json:"ip"`        // WARP IP (100.96.x.x) - primary address for node communication
	Healthy  bool      `json:"healthy"`
	LastSeen time.Time `json:"last_seen"`
	Version  string    `json:"version"`   // Agent version
	Arch     string    `json:"arch"`      // CPU architecture (amd64, arm64, etc.)
}

type Workload struct {
	Name         string   `json:"name"`                    // DNS hostname
	IP           string   `json:"ip"`                      // Service IP (routed via WARP)
	Compose      string   `json:"compose"`                 // Default compose (used if no arch-specific)
	ComposeAmd64 string   `json:"compose_amd64,omitempty"` // Optional: amd64-specific compose
	ComposeArm64 string   `json:"compose_arm64,omitempty"` // Optional: arm64-specific compose
	Revive       bool     `json:"revive"`                  // Auto-failover to another node if owner dies
	Autostart    bool     `json:"autostart"`               // Auto-start when Jetty starts up
	AllowedNodes []string `json:"allowed_nodes,omitempty"` // Node whitelist: empty/["*"] = all, otherwise node names/IDs
	Owner        string   `json:"owner"`                   // Node HWID
	Version      int64    `json:"version"`                 // Unix timestamp
}

// DeletedWorkload represents a tombstone for a deleted workload to propagate deletions
type DeletedWorkload struct {
	IP      string `json:"ip"`      // The IP of the deleted workload
	Version int64  `json:"version"` // Timestamp when deleted (must be > workload version to take effect)
}

type State struct {
	Peers            map[string]*Peer            `json:"peers"`                       // ID -> Peer
	Workloads        map[string]*Workload        `json:"workloads"`                   // IP -> Workload
	DeletedWorkloads map[string]*DeletedWorkload `json:"deleted_workloads,omitempty"` // IP -> tombstone (for sync propagation)
	CFToken          string                      `json:"cf_token,omitempty"`          // Cloudflare tunnel token (shared cluster-wide)
	WarpToken        string                      `json:"warp_token,omitempty"`        // Cloudflare WARP connector token (shared cluster-wide)
	EnvData          map[string]string           `json:"env_data,omitempty"`          // Encrypted environment variables (key -> encrypted value)
}

// =============================================================================
// Agent
// =============================================================================

type Agent struct {
	// Identity
	hwid     string
	hostname string
	ip       string // WARP IP (100.96.x.x) - primary node address

	// Cloudflare
	cfTunnelID string // WARP connector tunnel ID (for workload route management)

	// Config
	dataDir       string
	apiPort       int
	serviceCIDR   string // CIDR for workload service IPs (routed via WARP)
	joinURL       string
	clusterSecret string // Shared secret for cluster authentication
	tunnelDomain  string // Cloudflare tunnel domain for API access
	tunnelHost    string // This node's specific tunnel hostname (e.g., "node1.cluster.example.com")

	// State
	state   *State
	stateMu sync.RWMutex

	// Paths
	composeDir string
	hostsFile  string

	// Cloudflare tunnel
	cfCmd    *exec.Cmd
	cfMu     sync.Mutex
	cfStopCh chan struct{}

	// Runtime
	startTime            time.Time // When Jetty started (for uptime tracking)
	lastHeartbeatErrLog  time.Time // Last time we logged a heartbeat error (to reduce spam)
	publicIP             string    // Cached public IP (set at startup to avoid slow lookups)
	cachedCPUPercent     float64   // Cached CPU usage (updated by background goroutine)
	cachedCPUMu          sync.RWMutex

	// Route management
	workloadRoutes   map[string]string // workload IP -> owner WARP IP (for remote workloads)
	workloadRoutesMu sync.Mutex

	// Tunnel support for cross-node routing
	tunnelMode        string            // "ipip", "gre", or "" (none available)
	ipipWarnedPeers   map[string]bool   // Peers we've already warned about tunnel failure
	ipipWarnedPeersMu sync.Mutex

	// Userspace tunnel (fallback when kernel IPIP/GRE not available)
	tunDevice   *water.Interface // TUN device for capturing workload traffic
	tunIPConn   *net.IPConn      // Raw IP socket for IPIP packets (protocol 4)
	tunPeerIPs  sync.Map         // peerID -> string (peer WARP IP for IPIP encapsulation)

	stopCh chan struct{}
}

func New() (*Agent, error) {
	dataDir := getEnv("JETTY_DATA_DIR", "/data")
	os.MkdirAll(dataDir, 0755)

	a := &Agent{
		hostname:        getHostname(),
		dataDir:         dataDir,
		apiPort:         getEnvInt("JETTY_API_PORT", 6880),
		serviceCIDR:     getEnv("JETTY_SERVICE_CIDR", "10.100.0.0/16"), // CIDR for workload IPs
		joinURL:         getEnv("JETTY_JOIN", ""),
		clusterSecret:   getEnv("JETTY_SECRET", ""),
		tunnelDomain:    getEnv("JETTY_TUNNEL_DOMAIN", ""),            // e.g., "cluster.example.com" - Cloudflare tunnel for API access
		tunnelHost:      getEnv("JETTY_TUNNEL_HOST", ""),              // e.g., "node1.cluster.example.com" - this node's specific subdomain
		cfTunnelID:      getEnv("JETTY_CF_TUNNEL_ID", ""),             // WARP connector tunnel ID for route management
		composeDir:      filepath.Join(dataDir, "compose"),
		hostsFile:       "/etc/hosts",
		workloadRoutes:  make(map[string]string),
		ipipWarnedPeers: make(map[string]bool),
		state: &State{
			Peers:            make(map[string]*Peer),
			Workloads:        make(map[string]*Workload),
			DeletedWorkloads: make(map[string]*DeletedWorkload), // Tombstones for deleted workloads
			CFToken:          getEnv("JETTY_CF_TOKEN", ""),      // Bootstrap tunnel token
			WarpToken:        getEnv("JETTY_WARP_CONNECTOR_TOKEN", ""), // Bootstrap WARP connector token
			EnvData:          make(map[string]string),           // Encrypted environment variables
		},
		stopCh: make(chan struct{}),
	}

	os.MkdirAll(a.composeDir, 0755)

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
				exec.Command("docker", "rm", "-f", name).Run()
			}
		}
	}

	// Check if there's orphaned jetty0 interface from previous run
	if err := exec.Command("ip", "link", "show", "jetty0").Run(); err == nil {
		log.Printf("Found orphaned jetty0 interface from previous run, cleaning up...")
		exec.Command("ip", "link", "del", "jetty0").Run()
	}

	// Clean up any orphaned IPIP tunnels (named tun_*)
	output, _ = exec.Command("ip", "tunnel", "show").CombinedOutput()
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, "tun_") {
			parts := strings.Fields(line)
			if len(parts) > 0 {
				tunName := strings.TrimSuffix(parts[0], ":")
				exec.Command("ip", "tunnel", "del", tunName).Run()
				log.Printf("Cleaned up orphaned tunnel: %s", tunName)
			}
		}
	}

	// Clean up any orphaned workload routes (routes to 10.x.x.x/32)
	output, _ = exec.Command("ip", "route", "show").CombinedOutput()
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "10.") && strings.Contains(line, "/32") {
			parts := strings.Fields(line)
			if len(parts) > 0 {
				exec.Command("ip", "route", "del", parts[0]).Run()
				log.Printf("Cleaned up orphaned route: %s", parts[0])
			}
		}
	}
}

func (a *Agent) Start() error {
	// Record start time for uptime tracking
	a.startTime = time.Now()

	// Clean up any orphaned state from previous unclean shutdown
	a.cleanupOrphanedState()

	// Cache public IP at startup (avoid slow lookups on every health check)
	a.publicIP = getPublicIP()

	// Try to detect WARP IP (may not be available yet if joining)
	a.detectWarpIP()

	// Load saved state
	a.loadState()

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

	// Start gossip
	go a.gossipLoop()

	// Start failover monitor
	go a.failoverLoop()

	// Start IP monitor (detects WARP IP changes and re-announces)
	go a.ipMonitorLoop()

	// Start CPU sampling (background updates for accurate metrics)
	go a.cpuSampleLoop()

	mode := "warp (" + a.ip + ")"
	if a.tunnelDomain != "" {
		mode += " + tunnel (" + a.tunnelDomain + ")"
	}
	log.Printf("Jetty started: %s (%s) @ %s [mode: %s]", a.hostname, a.hwid[:12], a.ip, mode)
	return nil
}

// =============================================================================
// WARP Detection
// =============================================================================

// detectWarpIP checks for a Cloudflare WARP interface and extracts its IP.
// WARP provides Layer 3 connectivity through Cloudflare's network.
func (a *Agent) detectWarpIP() {
	// Check environment variable first (set by entrypoint script)
	if ip := os.Getenv("JETTY_WARP_IP"); ip != "" {
		a.ip = ip
		log.Printf("WARP IP from environment: %s", ip)
		return
	}

	// Try to find CloudflareWARP interface
	iface, err := net.InterfaceByName("CloudflareWARP")
	if err != nil {
		// WARP not running
		return
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.To4() != nil {
			a.ip = ipnet.IP.String()
			log.Printf("WARP IP detected: %s", a.ip)
			return
		}
	}
}

// getWarpIP returns the current WARP IP without modifying agent state.
func (a *Agent) getWarpIP() string {
	// Try to find CloudflareWARP interface
	iface, err := net.InterfaceByName("CloudflareWARP")
	if err != nil {
		return ""
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return ""
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.To4() != nil {
			return ipnet.IP.String()
		}
	}
	return ""
}

// ipMonitorLoop periodically checks for WARP IP changes and re-announces if needed.
// WARP can assign new IPs when reconnecting, so we need to propagate changes to peers.
func (a *Agent) ipMonitorLoop() {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-a.stopCh:
			return
		case <-tick.C:
			newIP := a.getWarpIP()
			if newIP == "" || newIP == a.ip {
				continue
			}

			oldIP := a.ip
			a.ip = newIP
			log.Printf("WARP IP changed: %s -> %s", oldIP, newIP)

			// Re-announce our new IP to all peers
			go a.announceOurIP()

			// Update IPIP tunnels (they use our local IP)
			a.stateMu.Lock()
			a.updateWorkloadRoutes()
			a.stateMu.Unlock()
		}
	}
}

// =============================================================================
// Runtime WARP Configuration
// =============================================================================

// configureWarpRuntime sets up WARP at runtime after receiving token from cluster join.
// This handles the case where a node joins without a pre-configured WARP token.
func (a *Agent) configureWarpRuntime(token string) error {
	if token == "" {
		return fmt.Errorf("no WARP token provided")
	}

	log.Printf("Configuring WARP at runtime...")

	// Check if warp-svc is running, start it if not
	if err := exec.Command("warp-cli", "--accept-tos", "status").Run(); err != nil {
		log.Printf("Starting WARP service...")

		// Start warp-svc in background with suppressed output
		// RUST_LOG=error reduces verbose debug output that floods logs on some distros (e.g., Arch)
		cmd := exec.Command("warp-svc", "--accept-tos")
		cmd.Env = append(os.Environ(), "RUST_LOG=error")
		// Discard stdout/stderr to prevent log flooding
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("failed to start warp-svc: %w", err)
		}

		// Wait for warp-svc to be ready
		for i := 0; i < 10; i++ {
			time.Sleep(time.Second)
			if exec.Command("warp-cli", "--accept-tos", "status").Run() == nil {
				break
			}
		}
	}

	// Check if already registered
	output, _ := exec.Command("warp-cli", "--accept-tos", "registration", "show").CombinedOutput()
	if !strings.Contains(string(output), "Missing registration") {
		log.Printf("WARP already registered, connecting...")
	} else {
		// Register with connector token
		log.Printf("Registering WARP connector...")
		cmd := exec.Command("warp-cli", "--accept-tos", "connector", "new", token)
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("failed to register WARP connector: %s", output)
		}
	}

	// Connect WARP
	log.Printf("Connecting WARP...")
	if err := exec.Command("warp-cli", "--accept-tos", "connect").Run(); err != nil {
		return fmt.Errorf("failed to connect WARP: %w", err)
	}

	// Wait for connection and detect IP
	for i := 0; i < 30; i++ {
		time.Sleep(time.Second)
		output, _ := exec.Command("warp-cli", "--accept-tos", "status").CombinedOutput()
		if strings.Contains(strings.ToLower(string(output)), "connected") {
			// Detect WARP IP
			a.detectWarpIP()
			log.Printf("WARP connected successfully: %s", a.ip)

			// Initialize WARP nft rules
			if err := a.initWarpRules(); err != nil {
				log.Printf("Warning: failed to init WARP rules: %v", err)
			}

			// Announce our IP to other peers so they can reach us
			go a.announceOurIP()

			return nil
		}
	}

	return fmt.Errorf("WARP connection timeout")
}

func (a *Agent) Stop() {
	log.Printf("Shutting down Jetty...")
	close(a.stopCh)
	a.stopCloudflared()
	a.saveState()

	// Clean up network resources
	a.cleanupNetwork()
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

	// Remove jetty0 interface (this also removes all IPs bound to it)
	if err := exec.Command("ip", "link", "del", "jetty0").Run(); err != nil {
		log.Printf("Warning: could not remove jetty0 interface: %v", err)
	} else {
		log.Printf("Removed jetty0 interface")
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
		os.WriteFile(hwidFile, []byte(hwid), 0644)
		return hwid
	}

	// Generate random
	b := make([]byte, 16)
	rand.Read(b)
	hwid := hex.EncodeToString(b)
	os.WriteFile(hwidFile, []byte(hwid), 0644)
	return hwid
}

// =============================================================================
// Network Interface (Dummy interface for mesh IP binding)
// =============================================================================

// initNetwork verifies WARP connectivity and enables forwarding.
// Node IP is the WARP IP assigned by Cloudflare.
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

	// Enable forwarding for workload traffic
	os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1"), 0644)

	// Check which tunnel mode is available for cross-node routing
	// Try IPIP first (most efficient), then GRE as fallback
	a.tunnelMode = a.detectTunnelMode()
	if a.tunnelMode == "" {
		log.Printf("IPIP/GRE tunnels not available - will use userspace tunnel for sending")
	}

	// ALWAYS start userspace tunnel listener - even if we have IPIP for sending,
	// we need to receive packets from peers that don't have IPIP (e.g., ChromeOS)
	if err := a.initUserspaceTunnel(); err != nil {
		log.Printf("Warning: userspace tunnel failed: %v", err)
		if a.tunnelMode == "" {
			log.Printf("Warning: no tunnel mode available - cross-node workload routing disabled")
		}
	}

	log.Printf("Network ready: %s (WARP), workload interface: jetty0", a.ip)
	return nil
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
		log.Printf("Using GRE tunnels for cross-node routing (IPIP unavailable)")
		return "gre"
	}

	// No tunnel mode available
	return ""
}

// =============================================================================
// Userspace Tunnel (fallback when kernel IPIP/GRE not available)
// =============================================================================

// initUserspaceTunnel creates a TUN device and raw IPIP socket for userspace tunneling.
// This is used when kernel IPIP/GRE modules aren't available (e.g., ChromeOS).
// Traffic to remote workloads (10.100.x.x) is captured via TUN, encapsulated in IPIP,
// and sent to the peer's WARP IP (100.96.x.x). No additional port needed - uses IP protocol 4.
func (a *Agent) initUserspaceTunnel() error {
	// Create TUN device
	config := water.Config{
		DeviceType: water.TUN,
	}
	config.Name = "jetty_tun"

	tun, err := water.New(config)
	if err != nil {
		return fmt.Errorf("create TUN device: %w", err)
	}
	a.tunDevice = tun

	// Configure TUN device
	if err := exec.Command("ip", "link", "set", "up", "dev", "jetty_tun").Run(); err != nil {
		tun.Close()
		return fmt.Errorf("bring up TUN device: %w", err)
	}

	// Create raw IP socket for IPIP (protocol 4) packets
	// This allows us to send/receive IPIP-encapsulated packets in userspace
	conn, err := net.ListenIP("ip4:4", &net.IPAddr{IP: net.IPv4zero})
	if err != nil {
		tun.Close()
		return fmt.Errorf("create raw IPIP socket: %w", err)
	}
	a.tunIPConn = conn

	// Start packet forwarding goroutines
	go a.tunReadLoop()  // TUN -> IPIP (outgoing)
	go a.tunRecvLoop()  // IPIP -> local (incoming)

	log.Printf("Userspace tunnel initialized (TUN: jetty_tun, IPIP protocol 4)")
	return nil
}

// tunReadLoop reads packets from TUN device and sends them via IPIP to the appropriate peer.
// The kernel wraps our inner packet with an outer IP header (protocol 4 = IPIP).
func (a *Agent) tunReadLoop() {
	buf := make([]byte, 65535)
	for {
		n, err := a.tunDevice.Read(buf)
		if err != nil {
			select {
			case <-a.stopCh:
				return
			default:
				log.Printf("TUN read error: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		if n < 20 {
			continue // Too small to be valid IP packet
		}

		// Parse destination IP from IP header (bytes 16-19 for IPv4)
		packet := buf[:n]
		if packet[0]>>4 != 4 {
			continue // Not IPv4
		}
		dstIP := net.IP(packet[16:20])
		srcIP := net.IP(packet[12:16])

		// Find peer that owns this workload IP
		a.workloadRoutesMu.Lock()
		ownerID, ok := a.workloadRoutes[dstIP.String()]
		a.workloadRoutesMu.Unlock()

		if !ok {
			log.Printf("IPIP send: no route for %s (src=%s)", dstIP, srcIP)
			continue // No route for this destination
		}

		// Get peer's WARP IP for IPIP encapsulation
		peerIPVal, ok := a.tunPeerIPs.Load(ownerID)
		if !ok {
			log.Printf("IPIP send: no peer IP for owner %s (dst=%s)", ownerID[:8], dstIP)
			continue // Peer IP not known
		}
		peerIP := net.ParseIP(peerIPVal.(string))
		if peerIP == nil {
			continue
		}

		log.Printf("IPIP send: %s -> %s via %s (%d bytes)", srcIP, dstIP, peerIP, n)

		// Send inner packet - kernel wraps with outer IP header (dst=peerIP, proto=4)
		_, err = a.tunIPConn.WriteToIP(packet, &net.IPAddr{IP: peerIP})
		if err != nil {
			log.Printf("IPIP send: failed to send to %s: %v", peerIP, err)
		}
	}
}

// tunRecvLoop receives IPIP packets from peers and injects the inner packet into local network.
// IPIP packets have: [outer IP header (20+ bytes)] [inner IP packet]
func (a *Agent) tunRecvLoop() {
	buf := make([]byte, 65535)
	for {
		n, srcAddr, err := a.tunIPConn.ReadFromIP(buf)
		if err != nil {
			select {
			case <-a.stopCh:
				return
			default:
				log.Printf("IPIP tunnel recv error: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		if n < 40 {
			continue // Too small: need outer header (20) + inner header (20)
		}

		// The raw socket gives us the full packet including outer IP header
		// Calculate outer header length (IHL field * 4)
		outerIHL := int(buf[0]&0x0f) * 4
		if n < outerIHL+20 {
			continue // Not enough data for inner packet
		}

		// Extract outer src IP for logging
		outerSrcIP := net.IP(buf[12:16])

		// Extract inner packet (skip outer header)
		innerPacket := buf[outerIHL:n]
		if innerPacket[0]>>4 != 4 {
			continue // Inner packet not IPv4
		}

		// Get destination IP from inner packet
		dstIP := net.IP(innerPacket[16:20])
		srcIP := net.IP(innerPacket[12:16])

		log.Printf("IPIP recv: outer src=%s, inner src=%s -> dst=%s (%d bytes)", outerSrcIP, srcIP, dstIP, n)

		// Check if this is for a local workload
		a.stateMu.RLock()
		var localWorkload *Workload
		for _, wl := range a.state.Workloads {
			if wl.Owner == a.hwid && wl.IP == dstIP.String() {
				localWorkload = wl
				break
			}
		}
		a.stateMu.RUnlock()

		if localWorkload != nil {
			log.Printf("IPIP recv: injecting packet for local workload %s (%s)", localWorkload.Name, dstIP)
			// Write inner packet to local network stack
			// The iptables DNAT rules will forward to the container
			if _, err := a.tunDevice.Write(innerPacket); err != nil {
				log.Printf("IPIP recv: failed to write to TUN: %v", err)
			}
		} else {
			log.Printf("IPIP recv: no local workload for %s (from %s)", dstIP, srcAddr)
		}
	}
}

// updateTunPeerAddr updates the WARP IP for a peer's IPIP tunnel endpoint.
func (a *Agent) updateTunPeerAddr(peerID, peerIP string) {
	if a.tunIPConn == nil || peerIP == "" {
		return
	}
	a.tunPeerIPs.Store(peerID, peerIP)
}

// cleanupUserspaceTunnel closes the TUN device and raw IPIP socket.
func (a *Agent) cleanupUserspaceTunnel() {
	if a.tunIPConn != nil {
		a.tunIPConn.Close()
	}
	if a.tunDevice != nil {
		a.tunDevice.Close()
		exec.Command("ip", "link", "del", "jetty_tun").Run()
	}
}

// initWarpRules sets up nftables rules for WARP traffic routing.
// This includes:
// 1. Masquerade rule for response routing through CloudflareWARP
// 2. Route for WARP client IP range (100.96.0.0/12)
// 3. Rules to allow cloudflared QUIC traffic through WARP firewall
func (a *Agent) initWarpRules() error {
	// Add masquerade rule for traffic going out through CloudflareWARP
	// This ensures responses from our mesh IPs route back through WARP
	if err := exec.Command("nft", "add", "rule", "ip", "nat", "POSTROUTING",
		"oifname", "CloudflareWARP", "masquerade").Run(); err != nil {
		log.Printf("Warning: failed to add WARP masquerade rule: %v", err)
	}

	// Add route for WARP client IP range to CloudflareWARP interface
	// This ensures we can reach other WARP clients
	exec.Command("ip", "route", "add", "100.96.0.0/12", "dev", "CloudflareWARP").Run()

	// Remove WARP's overly restrictive firewall entirely
	// WARP creates a firewall with policy DROP that breaks SSH, git, cloudflared, etc.
	// We only need WARP for routing to mesh CIDR (100.96.0.0/12), not the firewall
	if _, err := exec.Command("nft", "list", "table", "inet", "cloudflare-warp").Output(); err == nil {
		log.Printf("Removing WARP firewall table (routing still works without it)...")
		if err := exec.Command("nft", "delete", "table", "inet", "cloudflare-warp").Run(); err != nil {
			log.Printf("Warning: failed to delete cloudflare-warp table: %v", err)
		} else {
			log.Printf("WARP firewall table removed")
		}
	}

	log.Printf("WARP nft rules initialized")
	return nil
}

// =============================================================================
// IPIP Tunnels (for workload routing between nodes)
// =============================================================================

// ensurePeerTunnel creates a tunnel to a peer if it doesn't exist.
// Uses IPIP or GRE depending on what's available (detected at startup).
// This allows routing workload IPs (10.100.x.x) through WARP by encapsulating
// them in packets addressed to the peer's WARP IP (100.96.x.x).
func (a *Agent) ensurePeerTunnel(peerID, peerIP string) error {
	if a.ip == "" || peerIP == "" {
		return nil // Can't create tunnel without IPs
	}

	// Skip if no tunnel mode available (kernel modules not loaded)
	if a.tunnelMode == "" {
		return nil
	}

	// Tunnel name based on peer ID (truncated for interface name limit)
	tunName := "tun_" + peerID[:8]

	// Check if tunnel already exists with correct config
	out, _ := exec.Command("ip", "tunnel", "show", tunName).CombinedOutput()
	if strings.Contains(string(out), peerIP) {
		return nil // Tunnel exists with correct remote
	}

	// Delete existing tunnel if it has wrong config
	exec.Command("ip", "tunnel", "del", tunName).Run()

	// Create tunnel using detected mode (ipip or gre): local=our WARP IP, remote=peer's WARP IP
	cmd := exec.Command("ip", "tunnel", "add", tunName, "mode", a.tunnelMode,
		"local", a.ip, "remote", peerIP)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("create %s tunnel to %s: %s", a.tunnelMode, peerIP, strings.TrimSpace(string(out)))
	}

	// Bring up the tunnel interface
	cmd = exec.Command("ip", "link", "set", "up", "dev", tunName)
	if out, err := cmd.CombinedOutput(); err != nil {
		exec.Command("ip", "tunnel", "del", tunName).Run()
		return fmt.Errorf("bring up tunnel %s: %s", tunName, strings.TrimSpace(string(out)))
	}

	log.Printf("Created %s tunnel %s to peer %s (%s)", strings.ToUpper(a.tunnelMode), tunName, peerID[:8], peerIP)
	return nil
}

// removePeerTunnel removes the IPIP tunnel to a peer.
func (a *Agent) removePeerTunnel(peerID string) {
	tunName := "tun_" + peerID[:8]
	if err := exec.Command("ip", "tunnel", "del", tunName).Run(); err == nil {
		log.Printf("Removed tunnel %s", tunName)
	}
}

// getTunnelName returns the tunnel interface name for a peer.
func (a *Agent) getTunnelName(peerID string) string {
	return "tun_" + peerID[:8]
}

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

// =============================================================================
// /etc/hosts Management
// =============================================================================

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
	jettyLines = append(jettyLines, fmt.Sprintf("%s\t%s\t# this node", a.ip, a.hostname))

	// Add peers
	for _, p := range a.state.Peers {
		status := "healthy"
		if !p.Healthy {
			status = "unhealthy"
		}
		jettyLines = append(jettyLines, fmt.Sprintf("%s\t%s\t# peer (%s)", p.IP, p.Name, status))
	}

	// Add workloads
	for _, w := range a.state.Workloads {
		if w.IP != "" && w.Name != "" {
			location := "local"
			if w.Owner != a.hwid {
				location = "remote"
			}
			jettyLines = append(jettyLines, fmt.Sprintf("%s\t%s\t# workload (%s)", w.IP, w.Name, location))
		}
	}

	jettyLines = append(jettyLines, "# JETTY END")

	// Combine
	newLines = append(newLines, jettyLines...)

	// Write
	os.WriteFile(a.hostsFile, []byte(strings.Join(newLines, "\n")), 0644)

	// Update routes for remote workloads
	a.updateWorkloadRoutes()
}

// updateWorkloadRoutes adds/removes routes for remote workloads.
// Routes go through IPIP/GRE tunnels to the owner node, which then DNATs to the container.
// If tunnels aren't available, falls back to direct WARP routing (requires WARP to route 10.100.x.x).
// This must be called with stateMu held (at least RLock).
func (a *Agent) updateWorkloadRoutes() {
	// Skip if WARP is not connected
	if a.ip == "" {
		return
	}

	// If kernel tunnel mode is available, ensure tunnels exist to ALL healthy peers
	// IPIP/GRE requires bidirectional tunnels - if peer A routes to peer B, peer B needs a
	// tunnel back to A for decapsulation. By creating tunnels to all peers, any node
	// can receive encapsulated traffic from any other node.
	if a.tunnelMode != "" {
		for _, peer := range a.state.Peers {
			if peer.IP != "" && peer.Healthy {
				if err := a.ensurePeerTunnel(peer.ID, peer.IP); err != nil {
					log.Printf("Warning: failed to create tunnel to %s: %v", peer.Name, err)
				}
			}
		}
	} else if a.tunDevice != nil {
		// Using userspace tunnel - update peer addresses
		for _, peer := range a.state.Peers {
			if peer.IP != "" && peer.Healthy {
				a.updateTunPeerAddr(peer.ID, peer.IP)
			}
		}
	}

	// Build map of desired routes: workload IP -> owner peer
	type routeInfo struct {
		ownerID string
		ownerIP string
	}
	desiredRoutes := make(map[string]routeInfo) // wlIP -> routeInfo
	for _, wl := range a.state.Workloads {
		if wl.Owner == a.hwid {
			// Local workload - no route needed (handled by local iptables)
			continue
		}
		// Find owner peer - only route through healthy peers with IPs
		if peer, ok := a.state.Peers[wl.Owner]; ok && peer.IP != "" && peer.Healthy {
			desiredRoutes[wl.IP] = routeInfo{ownerID: peer.ID, ownerIP: peer.IP}
		}
	}

	a.workloadRoutesMu.Lock()
	defer a.workloadRoutesMu.Unlock()

	// Remove stale routes (routes that are no longer needed)
	for wlIP, ownerID := range a.workloadRoutes {
		if desired, ok := desiredRoutes[wlIP]; !ok || desired.ownerID != ownerID {
			// Route no longer needed or owner changed - remove it
			exec.Command("ip", "route", "del", wlIP+"/32").Run()
			delete(a.workloadRoutes, wlIP)
			log.Printf("Removed route for %s (was via %s)", wlIP, ownerID[:8])
		}
	}

	// Add new routes
	for wlIP, info := range desiredRoutes {
		if existingOwner, ok := a.workloadRoutes[wlIP]; ok && existingOwner == info.ownerID {
			// Route already exists with correct owner
			continue
		}

		var err error
		var routeDesc string

		if a.tunnelMode != "" {
			// Route through IPIP/GRE tunnel
			tunName := a.getTunnelName(info.ownerID)
			err = exec.Command("ip", "route", "add", wlIP+"/32", "dev", tunName).Run()
			routeDesc = "tunnel " + tunName
		} else if a.tunDevice != nil {
			// Route through userspace tunnel (TUN device)
			err = exec.Command("ip", "route", "add", wlIP+"/32", "dev", "jetty_tun").Run()
			routeDesc = "userspace tunnel to " + info.ownerIP
		} else {
			// Last resort: route directly through WARP interface
			// This works if WARP is configured to route 10.100.x.x traffic
			err = exec.Command("ip", "route", "add", wlIP+"/32", "via", info.ownerIP, "dev", "CloudflareWARP").Run()
			routeDesc = "WARP via " + info.ownerIP
		}

		if err == nil {
			a.workloadRoutes[wlIP] = info.ownerID
			log.Printf("Added route for %s via %s", wlIP, routeDesc)
		} else {
			log.Printf("Warning: failed to add route for %s via %s: %v", wlIP, routeDesc, err)
		}
	}
}

// =============================================================================
// Join Cluster
// =============================================================================

func (a *Agent) joinCluster() error {
	// Normalize join URL - allow both base URL and full /api/join URL
	joinEndpoint := a.joinURL
	if !strings.HasSuffix(joinEndpoint, "/api/join") {
		joinEndpoint = strings.TrimSuffix(joinEndpoint, "/") + "/api/join"
	}
	log.Printf("Joining cluster via %s", joinEndpoint)

	// Join request - IP may be empty if WARP not yet configured
	// (will be set after we receive WARP token and connect)
	req := map[string]string{
		"secret":  a.clusterSecret, // Cluster secret for authentication
		"id":      a.hwid,
		"name":    a.hostname,
		"ip":      a.ip, // WARP IP (may be empty, set after WARP connect)
		"version": Version,
		"arch":    runtime.GOARCH,
	}

	data, _ := json.Marshal(req)
	resp, err := httpClient.Post(joinEndpoint, "application/json", strings.NewReader(string(data)))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return fmt.Errorf("join failed: %s", body)
	}

	// Success - process response
	var result struct {
		Peers        []*Peer           `json:"peers"`
		Workloads    []*Workload       `json:"workloads"`
		CFToken      string            `json:"cf_token,omitempty"`
		WarpToken    string            `json:"warp_token,omitempty"`
		ServiceCIDR  string            `json:"service_cidr,omitempty"`
		TunnelDomain string            `json:"tunnel_domain,omitempty"`
		EnvData      map[string]string `json:"env_data,omitempty"` // Encrypted env vars
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		resp.Body.Close()
		return fmt.Errorf("decode join response: %w", err)
	}
	resp.Body.Close()

	// Update service CIDR if received (for workload IPs)
	if result.ServiceCIDR != "" && result.ServiceCIDR != a.serviceCIDR {
		log.Printf("Adopting cluster service CIDR: %s", result.ServiceCIDR)
		a.serviceCIDR = result.ServiceCIDR
	}

	// Update tunnel domain if received and not set locally
	if result.TunnelDomain != "" && a.tunnelDomain == "" {
		a.tunnelDomain = result.TunnelDomain
		log.Printf("Adopting cluster tunnel domain: %s", a.tunnelDomain)
	}

	a.stateMu.Lock()
	for _, p := range result.Peers {
		a.state.Peers[p.ID] = p
	}
	for _, w := range result.Workloads {
		a.state.Workloads[w.IP] = w
	}
	// Store tokens received from the cluster
	if result.CFToken != "" {
		a.state.CFToken = result.CFToken
	}
	if result.WarpToken != "" {
		a.state.WarpToken = result.WarpToken
	}
	// Store encrypted env data received from the cluster
	if len(result.EnvData) > 0 {
		for k, v := range result.EnvData {
			a.state.EnvData[k] = v
		}
	}
	a.stateMu.Unlock()

	a.saveState()

	// Configure WARP at runtime if we received a token and WARP isn't connected yet
	if result.WarpToken != "" && a.ip == "" {
		if err := a.configureWarpRuntime(result.WarpToken); err != nil {
			log.Printf("Warning: failed to configure WARP at runtime: %v", err)
		}
	}

	// Start cloudflared if we received a token
	if result.CFToken != "" {
		if err := a.startCloudflared(); err != nil {
			log.Printf("Warning: failed to start cloudflared: %v", err)
		}
	}

	log.Printf("Joined: %d peers, %d workloads, tunnel=%v, warp=%v",
		len(result.Peers), len(result.Workloads), result.CFToken != "", result.WarpToken != "")
	return nil
}

// =============================================================================
// API
// =============================================================================

// corsMiddleware adds CORS headers to allow cross-origin requests from the web UI.
func (a *Agent) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Allow requests from any origin (web UI can be opened from file:// or any domain)
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = "*"
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-API-Key, Authorization")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Max-Age", "86400")

		// Handle preflight requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// apiKeyMiddleware checks for valid API key on protected endpoints.
// The API key is the cluster secret (JETTY_SECRET).
func (a *Agent) apiKeyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip authentication if no secret is configured
		if a.clusterSecret == "" {
			next.ServeHTTP(w, r)
			return
		}

		// Public endpoints that don't require API key
		publicPaths := []string{
			"/api/health",        // Monitoring
			"/api/join",          // Node joining (uses secret in body)
			"/api/sync",          // Internal cluster sync
			"/api/peer-announce", // Internal peer announcement
			"/api/heartbeat",     // Internal heartbeat
			"/api/tunnel/sync",   // Internal tunnel sync
			"/swagger/",          // API documentation
		}

		path := r.URL.Path

		// Dashboard root is public (exact match)
		if path == "/" {
			next.ServeHTTP(w, r)
			return
		}

		for _, p := range publicPaths {
			if strings.HasPrefix(path, p) {
				next.ServeHTTP(w, r)
				return
			}
		}

		// Check API key from header or query param
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			apiKey = r.URL.Query().Get("api_key")
		}

		if apiKey != a.clusterSecret {
			http.Error(w, "unauthorized: invalid or missing API key", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// waitForAPI waits for the API server to be ready by polling the health endpoint.
// This ensures cloudflared doesn't start before the API is actually listening.
func (a *Agent) waitForAPI() {
	addr := fmt.Sprintf("http://127.0.0.1:%d/api/health", a.apiPort)
	maxRetries := 50 // 5 seconds total (50 * 100ms)

	for i := 0; i < maxRetries; i++ {
		resp, err := http.Get(addr)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 || resp.StatusCode == 401 { // 401 is OK, means API is up but needs auth
				log.Printf("API server ready")
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Printf("Warning: API server not responding after 5s, continuing anyway")
}

func (a *Agent) runAPI() {
	// Set Swagger host to empty so it uses the current request's host
	// This makes it work automatically for both localhost and cloudflared tunnels
	docs.SwaggerInfo.Host = ""

	r := mux.NewRouter()

	r.HandleFunc("/api/status", a.apiStatus).Methods("GET")
	r.HandleFunc("/api/workloads", a.apiListWorkloads).Methods("GET")
	r.HandleFunc("/api/workloads", a.apiCreateWorkload).Methods("POST")
	r.HandleFunc("/api/workloads/{name}", a.apiGetWorkload).Methods("GET")
	r.HandleFunc("/api/workloads/{name}", a.apiUpdateWorkload).Methods("PATCH")
	r.HandleFunc("/api/workloads/{name}", a.apiDeleteWorkload).Methods("DELETE")
	r.HandleFunc("/api/workloads/{name}/move", a.apiMoveWorkload).Methods("POST")
	r.HandleFunc("/api/workloads/{name}/logs", a.apiWorkloadLogs).Methods("GET")
	r.HandleFunc("/api/workloads/{name}/start", a.apiStartWorkload).Methods("POST")
	r.HandleFunc("/api/workloads/{name}/stop", a.apiStopWorkload).Methods("POST")
	r.HandleFunc("/api/join", a.apiJoin).Methods("POST")
	r.HandleFunc("/api/nodes", a.apiListNodes).Methods("GET")
	r.HandleFunc("/api/nodes/{id}", a.apiRemoveNode).Methods("DELETE")
	r.HandleFunc("/api/nodes/{id}/update", a.apiUpdateNode).Methods("POST")
	r.HandleFunc("/api/health", a.apiHealth).Methods("GET")
	r.HandleFunc("/api/sync", a.apiSync).Methods("GET")
	r.HandleFunc("/api/tunnel", a.apiGetTunnel).Methods("GET")
	r.HandleFunc("/api/tunnel", a.apiSetTunnel).Methods("POST")
	r.HandleFunc("/api/tunnel", a.apiDeleteTunnel).Methods("DELETE")
	r.HandleFunc("/api/tunnel/sync", a.apiTunnelSync).Methods("POST")
	r.HandleFunc("/api/peer-announce", a.apiPeerAnnounce).Methods("POST")
	r.HandleFunc("/api/heartbeat", a.apiHeartbeat).Methods("POST")
	r.HandleFunc("/api/env", a.apiListEnv).Methods("GET")
	r.HandleFunc("/api/env", a.apiSetEnv).Methods("POST")
	r.HandleFunc("/api/env/{key}", a.apiGetEnv).Methods("GET")
	r.HandleFunc("/api/env/{key}", a.apiDeleteEnv).Methods("DELETE")
	r.PathPrefix("/api/proxy/").HandlerFunc(a.apiWorkloadProxy)

	// Dashboard UI (embedded)
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(dashboardHTML)
	}).Methods("GET")

	// Swagger UI
	r.PathPrefix("/swagger/").Handler(httpSwagger.WrapHandler)

	// Wrap router with API key middleware and CORS middleware
	handler := a.corsMiddleware(a.apiKeyMiddleware(r))

	addr := fmt.Sprintf(":%d", a.apiPort)
	log.Printf("API on %s (auth=%v)", addr, a.clusterSecret != "")
	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatalf("API server failed: %v", err)
	}
}

// apiStatus godoc
// @Summary Get cluster status
// @Description Returns full cluster status including node info, peers, workloads, and connectivity status
// @Tags cluster
// @Produce json
// @Success 200 {object} StatusResponse
// @Router /status [get]
func (a *Agent) apiStatus(w http.ResponseWriter, r *http.Request) {
	a.stateMu.RLock()
	peers := make([]*Peer, 0, len(a.state.Peers))
	for _, p := range a.state.Peers {
		peers = append(peers, p)
	}

	// Build peer info maps for enrichment
	peerIDToInfo := make(map[string]map[string]string)
	for _, p := range a.state.Peers {
		peerIDToInfo[p.ID] = map[string]string{
			"id":   p.ID,
			"name": p.Name,
			"ip":   p.IP,
		}
	}
	// Add local node to owner info map
	peerIDToInfo[a.hwid] = map[string]string{
		"id":   a.hwid,
		"name": a.hostname,
		"ip":   a.ip,
	}

	// Helper to get workload status
	getStatus := func(wl *Workload) string {
		if wl.Owner != a.hwid {
			return "remote" // Can't check status of remote workloads
		}
		out, err := exec.Command("docker", "ps", "-q", "-f", "label=com.docker.compose.project=jetty_"+wl.Name).Output()
		if err != nil {
			return "unknown"
		}
		if len(strings.TrimSpace(string(out))) > 0 {
			return "running"
		}
		return "stopped"
	}

	// Build enriched workloads with owner info and status
	type EnrichedWorkload struct {
		Name         string            `json:"name"`
		IP           string            `json:"ip"`
		Compose      string            `json:"compose"`
		Revive       bool              `json:"revive"`
		Autostart    bool              `json:"autostart"`
		AllowedNodes []string          `json:"allowed_nodes,omitempty"`
		Owner        map[string]string `json:"owner"`
		Version      int64             `json:"version"`
		Status       string            `json:"status"`
	}

	workloads := make([]EnrichedWorkload, 0, len(a.state.Workloads))
	for _, wl := range a.state.Workloads {
		ownerInfo := peerIDToInfo[wl.Owner]
		if ownerInfo == nil {
			ownerInfo = map[string]string{"id": wl.Owner, "name": "unknown", "ip": "unknown"}
		}
		workloads = append(workloads, EnrichedWorkload{
			Name:         wl.Name,
			IP:           wl.IP,
			Compose:      wl.Compose,
			Revive:       wl.Revive,
			Autostart:    wl.Autostart,
			AllowedNodes: wl.AllowedNodes,
			Owner:        ownerInfo,
			Version:      wl.Version,
			Status:       getStatus(wl),
		})
	}

	hasTunnel := a.state.CFToken != ""
	a.stateMu.RUnlock()

	resp := map[string]interface{}{
		"node": map[string]interface{}{
			"id":        a.hwid,
			"name":      a.hostname,
			"ip":        a.ip,
			"arch":      runtime.GOARCH,
			"healthy":   true, // Self is always healthy if we're responding
			"last_seen": time.Now(),
			"is_self":   true,
		},
		"peers":        peers,
		"workloads":    workloads,
		"service_cidr": a.serviceCIDR,
		"tunnel": map[string]interface{}{
			"configured": hasTunnel,
			"running":    a.isTunnelRunning(),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// apiListWorkloads godoc
// @Summary List all workloads
// @Description Returns all workloads in the cluster, optionally filtered by node
// @Tags workloads
// @Produce json
// @Param node query string false "Filter by node name or ID (use 'local' for this node only)"
// @Success 200 {array} Workload
// @Router /workloads [get]
func (a *Agent) apiListWorkloads(w http.ResponseWriter, r *http.Request) {
	nodeFilter := r.URL.Query().Get("node")

	a.stateMu.RLock()

	// Build peer info maps for enrichment and filtering
	peerNameToID := make(map[string]string)
	peerIDToInfo := make(map[string]map[string]string)
	for _, p := range a.state.Peers {
		peerNameToID[p.Name] = p.ID
		peerIDToInfo[p.ID] = map[string]string{
			"id":      p.ID,
			"name":    p.Name,
			"ip": p.IP,
		}
	}

	// Add local node to owner info map
	peerIDToInfo[a.hwid] = map[string]string{
		"id":      a.hwid,
		"name":    a.hostname,
		"ip": a.ip,
	}

	type WorkloadResponse struct {
		Name         string            `json:"name"`
		IP           string            `json:"ip"`
		Compose      string            `json:"compose"`
		Revive       bool              `json:"revive"`
		Autostart    bool              `json:"autostart"`
		AllowedNodes []string          `json:"allowed_nodes,omitempty"`
		Owner        map[string]string `json:"owner"`
		Version      int64             `json:"version"`
		Status       string            `json:"status"`
	}

	// Helper to get workload status
	getStatus := func(wl *Workload) string {
		if wl.Owner != a.hwid {
			return "remote" // Can't check status of remote workloads
		}
		out, err := exec.Command("docker", "ps", "-q", "-f", "label=com.docker.compose.project=jetty_"+wl.Name).Output()
		if err != nil {
			return "unknown"
		}
		if len(strings.TrimSpace(string(out))) > 0 {
			return "running"
		}
		return "stopped"
	}

	var workloads []WorkloadResponse

	for _, wl := range a.state.Workloads {
		// Apply node filter if specified
		if nodeFilter != "" {
			// Handle special "local" filter
			if nodeFilter == "local" {
				if wl.Owner != a.hwid {
					continue
				}
			} else if nodeFilter == a.hwid || nodeFilter == a.hostname {
				// Filter for this node by ID or name
				if wl.Owner != a.hwid {
					continue
				}
			} else {
				// Filter for a specific peer by ID or name
				matchesFilter := wl.Owner == nodeFilter
				if !matchesFilter {
					// Check if filter matches a peer name
					if peerID, ok := peerNameToID[nodeFilter]; ok {
						matchesFilter = wl.Owner == peerID
					}
				}
				if !matchesFilter {
					continue
				}
			}
		}

		// Build enriched owner info
		ownerInfo := peerIDToInfo[wl.Owner]
		if ownerInfo == nil {
			ownerInfo = map[string]string{"id": wl.Owner, "name": "unknown", "ip": "unknown"}
		}

		workloads = append(workloads, WorkloadResponse{
			Name:         wl.Name,
			IP:           wl.IP,
			Compose:      wl.Compose,
			Revive:       wl.Revive,
			Autostart:    wl.Autostart,
			AllowedNodes: wl.AllowedNodes,
			Owner:        ownerInfo,
			Version:      wl.Version,
			Status:       getStatus(wl),
		})
	}
	a.stateMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(workloads)
}

// apiCreateWorkload godoc
// @Summary Deploy a new workload
// @Description Creates and deploys a new Docker Compose workload with a mesh IP
// @Tags workloads
// @Accept json
// @Produce json
// @Param workload body WorkloadRequest true "Workload configuration"
// @Success 201 {object} Workload
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 409 {object} ErrorResponse "Mesh IP already in use"
// @Failure 500 {object} ErrorResponse "Deployment failed"
// @Router /workloads [post]
func (a *Agent) apiCreateWorkload(w http.ResponseWriter, r *http.Request) {
	// Check if this is a move operation (allows IP overlap during migration)
	isMove := r.URL.Query().Get("move") == "true"

	var wl Workload
	if err := json.NewDecoder(r.Body).Decode(&wl); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// Require at least one compose file (default or architecture-specific)
	if wl.Name == "" {
		http.Error(w, "name required", 400)
		return
	}
	if wl.Compose == "" && wl.ComposeAmd64 == "" && wl.ComposeArm64 == "" {
		http.Error(w, "at least one compose file required (compose, compose_amd64, or compose_arm64)", 400)
		return
	}

	// Validate workload name (prevent path traversal attacks)
	if !validNamePattern.MatchString(wl.Name) {
		http.Error(w, "invalid name: must be alphanumeric with dash/underscore only", 400)
		return
	}

	// Check if this node is allowed to run this workload
	if !a.isThisNodeAllowed(&wl) {
		// Find an allowed node and proxy the deployment
		a.stateMu.RLock()
		targetPeer := a.findAllowedNode(&wl)
		a.stateMu.RUnlock()

		if targetPeer == nil {
			http.Error(w, "no allowed nodes available for this workload", 503)
			return
		}

		// Proxy deployment to allowed node
		data, _ := json.Marshal(wl)
		url := a.getPeerAPIURL(targetPeer, "/api/workloads")
		if isMove {
			url += "?move=true"
		}
		req, _ := a.peerRequest("POST", url, strings.NewReader(string(data)))
		resp, err := httpClient.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to proxy to allowed node: %v", err), 502)
			return
		}
		defer resp.Body.Close()

		// Forward the response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Lock for entire check-and-set to prevent race condition
	a.stateMu.Lock()

	// Check for duplicate workload name (skip during move operation)
	if !isMove && a.isWorkloadNameTaken(wl.Name) {
		a.stateMu.Unlock()
		http.Error(w, "workload name already in use", 409)
		return
	}

	// Auto-allocate mesh IP if not provided
	if wl.IP == "" {
		wl.IP = a.allocateServiceIP()
		if wl.IP == "" {
			a.stateMu.Unlock()
			http.Error(w, "no available IPs in mesh CIDR", 507)
			return
		}
	} else {
		// Validate provided mesh IP format
		if net.ParseIP(wl.IP) == nil {
			a.stateMu.Unlock()
			http.Error(w, "invalid mesh_ip: must be valid IP address", 400)
			return
		}

		// Validate IP is within mesh CIDR
		if !a.isIPInCIDR(wl.IP) {
			a.stateMu.Unlock()
			http.Error(w, fmt.Sprintf("mesh_ip must be within %s", a.serviceCIDR), 400)
			return
		}

		// Check if IP is already taken (skip during move for blue-green deployment)
		if !isMove && a.isIPTaken(wl.IP) {
			a.stateMu.Unlock()
			http.Error(w, "mesh_ip already in use", 409)
			return
		}
	}

	wl.Owner = a.hwid
	wl.Version = time.Now().Unix()
	a.state.Workloads[wl.IP] = &wl
	a.stateMu.Unlock()

	// Deploy (outside lock to avoid blocking other operations)
	if err := a.deployWorkload(&wl); err != nil {
		// Rollback on failure
		a.stateMu.Lock()
		delete(a.state.Workloads, wl.IP)
		a.stateMu.Unlock()
		http.Error(w, err.Error(), 500)
		return
	}

	a.updateHosts()
	a.saveState()
	a.broadcastState()

	// Build response with enriched owner info
	response := map[string]interface{}{
		"name":          wl.Name,
		"ip":       wl.IP,
		"compose":       wl.Compose,
		"revive":        wl.Revive,
		"autostart":     wl.Autostart,
		"allowed_nodes": wl.AllowedNodes,
		"owner": map[string]string{
			"id":      a.hwid,
			"name":    a.hostname,
			"ip": a.ip,
		},
		"version": wl.Version,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// apiGetWorkload godoc
// @Summary Get workload details
// @Description Returns details for a specific workload by name
// @Tags workloads
// @Produce json
// @Param name path string true "Workload name"
// @Success 200 {object} Workload
// @Failure 404 {object} ErrorResponse "Workload not found"
// @Router /workloads/{name} [get]
func (a *Agent) apiGetWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	a.stateMu.RLock()
	var found *Workload
	var ownerPeer *Peer
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			// Find the owner peer if not local
			if wl.Owner != a.hwid {
				ownerPeer = a.state.Peers[wl.Owner]
			}
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	// Helper to build owner info object
	buildOwnerInfo := func(id, peerName, meshIP string) map[string]string {
		return map[string]string{
			"id":      id,
			"name":    peerName,
			"ip": meshIP,
		}
	}

	// If workload is remote, proxy the request to the owner node
	if found.Owner != a.hwid {
		if ownerPeer == nil || !ownerPeer.Healthy {
			// Owner not reachable, return basic info without container details
			ownerInfo := buildOwnerInfo(found.Owner, "unknown", "unknown")
			if ownerPeer != nil {
				ownerInfo = buildOwnerInfo(ownerPeer.ID, ownerPeer.Name, ownerPeer.IP)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"name":          found.Name,
				"ip":       found.IP,
				"compose":       found.Compose,
				"revive":        found.Revive,
				"autostart":     found.Autostart,
				"allowed_nodes": found.AllowedNodes,
				"owner":         ownerInfo,
				"version":       found.Version,
				"is_local":      false,
				"error":         "owner node unreachable",
			})
			return
		}

		// Proxy request to owner
		url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name)
		req, _ := a.peerRequest("GET", url, nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			ownerInfo := buildOwnerInfo(ownerPeer.ID, ownerPeer.Name, ownerPeer.IP)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"name":          found.Name,
				"ip":       found.IP,
				"compose":       found.Compose,
				"revive":        found.Revive,
				"autostart":     found.Autostart,
				"allowed_nodes": found.AllowedNodes,
				"owner":         ownerInfo,
				"version":       found.Version,
				"is_local":      false,
				"error":         fmt.Sprintf("failed to reach owner: %v", err),
			})
			return
		}
		defer resp.Body.Close()

		// Forward the response from owner
		w.Header().Set("Content-Type", "application/json")
		body, _ := io.ReadAll(resp.Body)

		// Parse and update is_local field
		var remoteResp map[string]interface{}
		if err := json.Unmarshal(body, &remoteResp); err == nil {
			remoteResp["is_local"] = false // Override since we proxied
			json.NewEncoder(w).Encode(remoteResp)
		} else {
			w.Write(body)
		}
		return
	}

	// Build enriched response with Docker info for local workload
	ownerInfo := buildOwnerInfo(a.hwid, a.hostname, a.ip)
	response := map[string]interface{}{
		"name":          found.Name,
		"ip":       found.IP,
		"compose":       found.Compose,
		"revive":        found.Revive,
		"autostart":     found.Autostart,
		"allowed_nodes": found.AllowedNodes,
		"owner":         ownerInfo,
		"version":       found.Version,
		"is_local":      true,
	}

	containerInfo := a.getContainerInfo(found.Name)
	response["containers"] = containerInfo

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// apiUpdateWorkload godoc
// @Summary Update a workload
// @Description Updates workload configuration. Some fields require redeploy.
// @Tags workloads
// @Accept json
// @Produce json
// @Param name path string true "Workload name"
// @Param update body object true "Fields to update"
// @Success 200 {object} Workload
// @Failure 404 {object} ErrorResponse "Workload not found"
// @Failure 409 {object} ErrorResponse "Mesh IP conflict"
// @Failure 500 {object} ErrorResponse "Update failed"
// @Router /workloads/{name} [patch]
func (a *Agent) apiUpdateWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	// Parse update request
	var update struct {
		Compose      *string   `json:"compose,omitempty"`
		ComposeAmd64 *string   `json:"compose_amd64,omitempty"`
		ComposeArm64 *string   `json:"compose_arm64,omitempty"`
		IP           *string   `json:"ip,omitempty"`
		Revive       *bool     `json:"revive,omitempty"`
		Autostart    *bool     `json:"autostart,omitempty"`
		AllowedNodes *[]string `json:"allowed_nodes,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// Find workload
	a.stateMu.RLock()
	var found *Workload
	var foundIP string
	var ownerPeer *Peer
	for ip, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			foundIP = ip
			if wl.Owner != a.hwid {
				ownerPeer = a.state.Peers[wl.Owner]
			}
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	// If workload is remote, proxy to owner
	if found.Owner != a.hwid {
		if ownerPeer == nil || !ownerPeer.Healthy {
			http.Error(w, "owner node unreachable", 502)
			return
		}

		// Proxy PATCH to owner
		body, _ := json.Marshal(update)
		url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name)
		req, _ := a.peerRequest("PATCH", url, strings.NewReader(string(body)))
		resp, err := httpClient.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to reach owner: %v", err), 502)
			return
		}
		defer resp.Body.Close()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Local workload - apply updates
	needsRedeploy := false
	newMeshIP := foundIP

	a.stateMu.Lock()

	// Handle mesh IP change
	if update.IP != nil && *update.IP != found.IP {
		newIP := *update.IP

		// Validate new IP
		if net.ParseIP(newIP) == nil {
			a.stateMu.Unlock()
			http.Error(w, "invalid mesh_ip: must be valid IP address", 400)
			return
		}
		if !a.isIPInCIDR(newIP) {
			a.stateMu.Unlock()
			http.Error(w, fmt.Sprintf("mesh_ip must be within %s", a.serviceCIDR), 400)
			return
		}
		if a.isIPTaken(newIP) {
			a.stateMu.Unlock()
			http.Error(w, "mesh_ip already in use", 409)
			return
		}

		// Remove old entry, will add new one
		delete(a.state.Workloads, foundIP)
		newMeshIP = newIP
		found.IP = newIP
		needsRedeploy = true
	}

	// Handle compose changes
	if update.Compose != nil && *update.Compose != found.Compose {
		found.Compose = *update.Compose
		needsRedeploy = true
	}
	if update.ComposeAmd64 != nil && *update.ComposeAmd64 != found.ComposeAmd64 {
		found.ComposeAmd64 = *update.ComposeAmd64
		// Only redeploy if we're on amd64 and this is the active compose
		if runtime.GOARCH == "amd64" {
			needsRedeploy = true
		}
	}
	if update.ComposeArm64 != nil && *update.ComposeArm64 != found.ComposeArm64 {
		found.ComposeArm64 = *update.ComposeArm64
		// Only redeploy if we're on arm64 and this is the active compose
		if runtime.GOARCH == "arm64" {
			needsRedeploy = true
		}
	}

	// Handle metadata updates (no redeploy needed)
	if update.Revive != nil {
		found.Revive = *update.Revive
	}
	if update.Autostart != nil {
		found.Autostart = *update.Autostart
	}
	if update.AllowedNodes != nil {
		found.AllowedNodes = *update.AllowedNodes
	}

	// Update version
	found.Version = time.Now().Unix()

	// Store with (potentially new) mesh IP
	a.state.Workloads[newMeshIP] = found
	a.stateMu.Unlock()

	// Redeploy if needed
	if needsRedeploy {
		// Remove old deployment
		a.removeWorkload(found)

		// Deploy with new config
		if err := a.deployWorkload(found); err != nil {
			// Rollback on failure
			a.stateMu.Lock()
			delete(a.state.Workloads, newMeshIP)
			if newMeshIP != foundIP {
				found.IP = foundIP
				a.state.Workloads[foundIP] = found
			}
			a.stateMu.Unlock()
			http.Error(w, fmt.Sprintf("redeploy failed: %v", err), 500)
			return
		}
	}

	a.updateHosts()
	a.saveState()
	a.broadcastState()

	// Build response
	response := map[string]interface{}{
		"name":          found.Name,
		"ip":       found.IP,
		"compose":       found.Compose,
		"revive":        found.Revive,
		"autostart":     found.Autostart,
		"allowed_nodes": found.AllowedNodes,
		"owner": map[string]string{
			"id":      a.hwid,
			"name":    a.hostname,
			"ip": a.ip,
		},
		"version":    found.Version,
		"redeployed": needsRedeploy,
	}

	log.Printf("Updated workload %s (redeploy=%v)", found.Name, needsRedeploy)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// getContainerInfo retrieves Docker container details for a workload
func (a *Agent) getContainerInfo(workloadName string) []map[string]interface{} {
	var containers []map[string]interface{}

	// Get container IDs for this project
	out, err := exec.Command("docker", "ps", "-a", "-q", "-f", "label=com.docker.compose.project=jetty_"+workloadName).Output()
	if err != nil || len(out) == 0 {
		return containers
	}

	containerIDs := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, containerID := range containerIDs {
		if containerID == "" {
			continue
		}

		// Get detailed container info using docker inspect
		inspectOut, err := exec.Command("docker", "inspect", "--format", `{{json .}}`, containerID).Output()
		if err != nil {
			continue
		}

		var inspectData map[string]interface{}
		if err := json.Unmarshal(inspectOut, &inspectData); err != nil {
			continue
		}

		info := make(map[string]interface{})
		info["id"] = containerID[:12] // Short ID

		// Extract container name
		if name, ok := inspectData["Name"].(string); ok {
			info["name"] = strings.TrimPrefix(name, "/")
		}

		// Extract state info
		if state, ok := inspectData["State"].(map[string]interface{}); ok {
			info["status"] = state["Status"]
			info["running"] = state["Running"]
			if startedAt, ok := state["StartedAt"].(string); ok && startedAt != "0001-01-01T00:00:00Z" {
				if t, err := time.Parse(time.RFC3339Nano, startedAt); err == nil {
					info["started_at"] = t.Format(time.RFC3339)
					info["uptime"] = time.Since(t).Round(time.Second).String()
				}
			}
			if finishedAt, ok := state["FinishedAt"].(string); ok && finishedAt != "0001-01-01T00:00:00Z" {
				if t, err := time.Parse(time.RFC3339Nano, finishedAt); err == nil && !t.IsZero() {
					info["finished_at"] = t.Format(time.RFC3339)
				}
			}
			if exitCode, ok := state["ExitCode"].(float64); ok {
				info["exit_code"] = int(exitCode)
			}
			if health, ok := state["Health"].(map[string]interface{}); ok {
				info["health"] = health["Status"]
			}
		}

		// Extract image info
		if config, ok := inspectData["Config"].(map[string]interface{}); ok {
			info["image"] = config["Image"]
		}

		// Extract network info
		if netSettings, ok := inspectData["NetworkSettings"].(map[string]interface{}); ok {
			if networks, ok := netSettings["Networks"].(map[string]interface{}); ok {
				var ips []string
				for netName, netData := range networks {
					if netInfo, ok := netData.(map[string]interface{}); ok {
						if ip, ok := netInfo["IPAddress"].(string); ok && ip != "" {
							ips = append(ips, fmt.Sprintf("%s:%s", netName, ip))
						}
					}
				}
				info["networks"] = ips
			}
			if ports, ok := netSettings["Ports"].(map[string]interface{}); ok {
				var portList []string
				for port := range ports {
					portList = append(portList, port)
				}
				info["ports"] = portList
			}
		}

		// Get resource usage using docker stats (quick snapshot)
		statsOut, _ := exec.Command("docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}", containerID).Output()
		if len(statsOut) > 0 {
			parts := strings.Split(strings.TrimSpace(string(statsOut)), "|")
			if len(parts) == 3 {
				info["cpu_percent"] = strings.TrimSpace(parts[0])
				info["memory_usage"] = strings.TrimSpace(parts[1])
				info["memory_percent"] = strings.TrimSpace(parts[2])
			}
		}

		containers = append(containers, info)
	}

	return containers
}

// apiDeleteWorkload godoc
// @Summary Delete a workload
// @Description Stops and removes a workload from the cluster
// @Tags workloads
// @Param name path string true "Workload name"
// @Success 204 "Workload deleted"
// @Failure 404 {object} ErrorResponse "Workload not found"
// @Router /workloads/{name} [delete]
func (a *Agent) apiDeleteWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	a.stateMu.RLock()
	var found *Workload
	var foundIP string
	var ownerPeer *Peer
	for ip, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			foundIP = ip
			if wl.Owner != a.hwid {
				ownerPeer = a.state.Peers[wl.Owner]
			}
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	// If workload is remote and owner is healthy, proxy the delete request
	if found.Owner != a.hwid {
		if ownerPeer != nil && ownerPeer.Healthy {
			// Proxy delete to owner node
			url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name)
			req, _ := a.peerRequest("DELETE", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				http.Error(w, fmt.Sprintf("failed to reach owner: %v", err), 502)
				return
			}
			defer resp.Body.Close()

			// Forward response from owner
			w.WriteHeader(resp.StatusCode)
			if resp.StatusCode != 204 {
				body, _ := io.ReadAll(resp.Body)
				w.Write(body)
			}
			return
		}
		// Owner is dead - allow cleanup of orphaned workload from our state
		log.Printf("Cleaning up orphaned workload %s (owner %s unreachable)", name, found.Owner)
	}

	// Local workload or orphaned cleanup - delete from state and add tombstone
	a.stateMu.Lock()
	delete(a.state.Workloads, foundIP)
	// Add tombstone with version greater than the deleted workload's version
	// This ensures the deletion propagates to peers even if they have older copies
	a.state.DeletedWorkloads[foundIP] = &DeletedWorkload{
		IP:      foundIP,
		Version: time.Now().Unix(),
	}
	a.stateMu.Unlock()

	log.Printf("Deleted workload %s (IP %s), created tombstone for sync propagation", name, foundIP)

	// Remove if we're running it
	if found.Owner == a.hwid {
		a.removeWorkload(found)
	}

	a.updateHosts()
	a.saveState()
	a.broadcastState()

	w.WriteHeader(204)
}

// apiMoveWorkload godoc
// @Summary Move workload to another node
// @Description Migrates a workload to a different node in the cluster
// @Tags workloads
// @Accept json
// @Param name path string true "Workload name"
// @Param target body MoveRequest true "Target node"
// @Success 200 "Workload moved"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 404 {object} ErrorResponse "Workload or target not found"
// @Router /workloads/{name}/move [post]
func (a *Agent) apiMoveWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	var req struct {
		To string `json:"to"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	// Find workload and current owner
	a.stateMu.RLock()
	var found *Workload
	var currentOwner *Peer
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			if wl.Owner != a.hwid {
				currentOwner = a.state.Peers[wl.Owner]
			}
			break
		}
	}

	// Find target (check if moving to self first, then search peers)
	var target *Peer
	var targetIsSelf bool
	if req.To == a.hwid || req.To == a.hostname {
		target = &Peer{
			ID:      a.hwid,
			Name:    a.hostname,
			IP:      a.ip,
			Healthy: true,
			Arch:    runtime.GOARCH,
		}
		targetIsSelf = true
	} else {
		for _, p := range a.state.Peers {
			if p.Name == req.To || p.ID == req.To {
				target = p
				break
			}
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "workload not found", 404)
		return
	}
	if target == nil {
		http.Error(w, "target not found", 404)
		return
	}
	if !target.Healthy {
		http.Error(w, "target node is not healthy", 503)
		return
	}

	// Check if we already own it (no-op)
	if found.Owner == a.hwid && targetIsSelf {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"moved": "no-op", "reason": "already on target node"})
		return
	}

	// Check if target is allowed to run this workload (includes arch compatibility)
	if !a.isNodeAllowed(found, target.ID, target.Name, target.Arch) {
		// Provide a more specific error message
		if !found.canRunOnArch(target.Arch) {
			http.Error(w, fmt.Sprintf("workload has no compose file for target architecture (%s)", target.Arch), 400)
		} else {
			http.Error(w, "target node is not in allowed_nodes for this workload", 403)
		}
		return
	}

	// Blue-green deployment: deploy on target first
	var newVersion int64
	if targetIsSelf {
		// Moving to self - deploy locally
		// First, tell the current owner to remove it
		if currentOwner != nil && currentOwner.Healthy {
			deleteURL := a.getPeerAPIURL(currentOwner, "/api/workloads/"+name)
			delReq, _ := a.peerRequest("DELETE", deleteURL, nil)
			delResp, err := httpClient.Do(delReq)
			if err != nil {
				log.Printf("Warning: failed to remove workload from original owner: %v", err)
			} else {
				delResp.Body.Close()
			}
		}

		// Deploy locally
		newVersion = time.Now().Unix()
		localWl := *found // Copy workload
		localWl.Owner = a.hwid
		localWl.Version = newVersion

		if err := a.deployWorkload(&localWl); err != nil {
			http.Error(w, fmt.Sprintf("local deploy failed: %v", err), 500)
			return
		}

		// Update state
		a.stateMu.Lock()
		a.state.Workloads[found.IP] = &localWl
		a.stateMu.Unlock()
	} else {
		// Moving to remote peer - deploy via API
		data, _ := json.Marshal(found)
		url := a.getPeerAPIURL(target, "/api/workloads?move=true")
		deployReq, _ := a.peerRequest("POST", url, strings.NewReader(string(data)))
		resp, err := httpClient.Do(deployReq)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to deploy on target: %v", err), 502)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			http.Error(w, fmt.Sprintf("target rejected deployment (status %d): %s", resp.StatusCode, body), 500)
			return
		}

		// Parse response to get updated workload info (new owner, version)
		var deployResult struct {
			Name    string `json:"name"`
			IP      string `json:"ip"`
			Version int64  `json:"version"`
			Owner   struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"owner"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&deployResult); err != nil {
			log.Printf("Warning: could not parse deploy response: %v", err)
		}
		newVersion = deployResult.Version

		// Target is now running - remove from source
		// If we own it, remove locally (but don't delete from state yet - we'll update it)
		if found.Owner == a.hwid {
			a.removeWorkload(found)
		} else if currentOwner != nil && currentOwner.Healthy {
			// Proxy delete to current owner
			deleteURL := a.getPeerAPIURL(currentOwner, "/api/workloads/"+name)
			delReq, _ := a.peerRequest("DELETE", deleteURL, nil)
			delResp, err := httpClient.Do(delReq)
			if err != nil {
				log.Printf("Warning: failed to remove workload from original owner: %v", err)
			} else {
				delResp.Body.Close()
			}
		}

		// Update local state immediately to reflect new owner
		// This prevents stale state issues during rapid moves
		a.stateMu.Lock()
		if wl, exists := a.state.Workloads[found.IP]; exists {
			wl.Owner = target.ID
			if newVersion > 0 {
				wl.Version = newVersion
			} else {
				wl.Version = time.Now().Unix()
			}
		}
		a.stateMu.Unlock()
	}

	a.updateHosts()
	a.saveState()
	a.broadcastState()

	log.Printf("Moved workload %s to %s", found.Name, target.Name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"moved": "ok", "to": target.Name})
}

// apiWorkloadLogs godoc
// @Summary Get workload logs
// @Description Returns container logs for a workload
// @Tags workloads
// @Produce plain
// @Param name path string true "Workload name"
// @Success 200 {string} string "Container logs"
// @Failure 404 {object} ErrorResponse "Workload not found"
// @Router /workloads/{name}/logs [get]
func (a *Agent) apiWorkloadLogs(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	a.stateMu.RLock()
	var found *Workload
	var ownerPeer *Peer
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			if wl.Owner != a.hwid {
				ownerPeer = a.state.Peers[wl.Owner]
			}
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	// If workload is remote, proxy to owner
	if found.Owner != a.hwid {
		if ownerPeer == nil || !ownerPeer.Healthy {
			http.Error(w, "owner node unreachable", 502)
			return
		}
		// Proxy logs request to owner
		url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name+"/logs")
		req, _ := a.peerRequest("GET", url, nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to reach owner: %v", err), 502)
			return
		}
		defer resp.Body.Close()

		w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	out, _ := a.composeCmd(found.Name, "logs", "--tail", "200")
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(out))
}

// apiStartWorkload godoc
// @Summary Start a stopped workload
// @Description Starts the containers for a workload that was previously stopped
// @Tags workloads
// @Param name path string true "Workload name"
// @Success 200 {object} map[string]string
// @Failure 404 {object} ErrorResponse "Workload not found"
// @Failure 500 {object} ErrorResponse "Start failed"
// @Router /workloads/{name}/start [post]
func (a *Agent) apiStartWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	a.stateMu.RLock()
	var found *Workload
	var ownerPeer *Peer
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			if wl.Owner != a.hwid {
				ownerPeer = a.state.Peers[wl.Owner]
			}
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	// If workload is remote, proxy to owner
	if found.Owner != a.hwid {
		if ownerPeer == nil || !ownerPeer.Healthy {
			http.Error(w, "owner node unreachable", 502)
			return
		}
		// Proxy start request to owner
		url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name+"/start")
		req, _ := a.peerRequest("POST", url, nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to reach owner: %v", err), 502)
			return
		}
		defer resp.Body.Close()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	if out, err := a.composeCmd(found.Name, "start"); err != nil {
		http.Error(w, fmt.Sprintf("start failed: %s", out), 500)
		return
	}

	// Re-setup mesh IP routing (container IP may have changed)
	a.setupWorkloadIP(found)

	log.Printf("Started workload: %s", found.Name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "started", "name": found.Name})
}

// apiStopWorkload godoc
// @Summary Stop a running workload
// @Description Stops the containers for a workload without removing them
// @Tags workloads
// @Param name path string true "Workload name"
// @Success 200 {object} map[string]string
// @Failure 404 {object} ErrorResponse "Workload not found"
// @Failure 500 {object} ErrorResponse "Stop failed"
// @Router /workloads/{name}/stop [post]
func (a *Agent) apiStopWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	a.stateMu.RLock()
	var found *Workload
	var ownerPeer *Peer
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			if wl.Owner != a.hwid {
				ownerPeer = a.state.Peers[wl.Owner]
			}
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	// If workload is remote, proxy to owner
	if found.Owner != a.hwid {
		if ownerPeer == nil || !ownerPeer.Healthy {
			http.Error(w, "owner node unreachable", 502)
			return
		}
		// Proxy stop request to owner
		url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name+"/stop")
		req, _ := a.peerRequest("POST", url, nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to reach owner: %v", err), 502)
			return
		}
		defer resp.Body.Close()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Clean up iptables before stopping (need container IP)
	a.cleanupWorkloadIP(found)

	if out, err := a.composeCmd(found.Name, "stop"); err != nil {
		http.Error(w, fmt.Sprintf("stop failed: %s", out), 500)
		return
	}

	log.Printf("Stopped workload: %s", found.Name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "stopped", "name": found.Name})
}

// apiJoin godoc
// @Summary Join cluster
// @Description Allows a new node to join the cluster using the cluster secret
// @Tags cluster
// @Accept json
// @Produce json
// @Param request body JoinRequest true "Join request"
// @Success 200 {object} JoinResponse
// @Failure 401 {object} ErrorResponse "Invalid secret"
// @Failure 409 {object} ErrorResponse "Mesh IP collision"
// @Router /join [post]
func (a *Agent) apiJoin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Secret  string `json:"secret"` // Cluster secret for authentication
		ID      string `json:"id"`
		Name    string `json:"name"`
		IP      string `json:"ip"`
		Version string `json:"version"`
		Arch    string `json:"arch"` // CPU architecture (amd64, arm64, etc.)
	}
	json.NewDecoder(r.Body).Decode(&req)

	// Validate cluster secret
	if a.clusterSecret == "" {
		http.Error(w, "cluster has no secret configured", 500)
		return
	}
	if req.Secret != a.clusterSecret {
		http.Error(w, "invalid secret", 401)
		return
	}

	// Check for mesh IP collision before creating peer
	a.stateMu.RLock()
	// Check against our own IP
	if req.IP == a.ip {
		a.stateMu.RUnlock()
		http.Error(w, "mesh_ip collision with existing node", 409)
		return
	}
	// Check against existing peers
	for _, p := range a.state.Peers {
		if p.IP == req.IP && p.ID != req.ID {
			a.stateMu.RUnlock()
			http.Error(w, "mesh_ip collision with existing node", 409)
			return
		}
	}
	// Check against workloads
	if _, exists := a.state.Workloads[req.IP]; exists {
		a.stateMu.RUnlock()
		http.Error(w, "mesh_ip collision with existing workload", 409)
		return
	}
	a.stateMu.RUnlock()

	// Create peer
	peer := &Peer{
		ID:       req.ID,
		Name:     req.Name,
		IP:       req.IP,
		Healthy:  true,
		LastSeen: time.Now(),
		Version:  req.Version,
		Arch:     req.Arch,
	}

	a.stateMu.Lock()
	a.state.Peers[peer.ID] = peer

	// Build response with all peers (including self)
	allPeers := []*Peer{{
		ID:      a.hwid,
		Name:    a.hostname,
		IP:      a.ip,
		Healthy: true,
		Version: Version,
		Arch:    runtime.GOARCH,
	}}
	for _, p := range a.state.Peers {
		if p.ID != req.ID {
			allPeers = append(allPeers, p)
		}
	}

	allWorkloads := make([]*Workload, 0, len(a.state.Workloads))
	for _, w := range a.state.Workloads {
		allWorkloads = append(allWorkloads, w)
	}
	cfToken := a.state.CFToken
	warpToken := a.state.WarpToken
	// Copy env data (already encrypted, safe to share)
	envData := make(map[string]string)
	for k, v := range a.state.EnvData {
		envData[k] = v
	}
	a.stateMu.Unlock()

	a.updateHosts()
	a.saveState()

	// Create IPIP tunnel to this peer (for receiving their traffic)
	if peer.IP != "" {
		if err := a.ensurePeerTunnel(peer.ID, peer.IP); err != nil {
			log.Printf("Warning: failed to create tunnel to %s: %v", peer.Name, err)
		}
	}

	// Notify other peers
	go a.announcePeer(peer)

	resp := map[string]interface{}{
		"peers":     allPeers,
		"workloads": allWorkloads,
		"mesh_cidr": a.serviceCIDR, // So joining node uses same CIDR
	}

	// Include CF token so new peer can start its tunnel
	if cfToken != "" {
		resp["cf_token"] = cfToken
	}

	// Include WARP connector token so new peer can join WARP network
	if warpToken != "" {
		resp["warp_token"] = warpToken
	}

	// Include tunnel domain if configured
	if a.tunnelDomain != "" {
		resp["tunnel_domain"] = a.tunnelDomain
	}

	// Include encrypted env data so joining node has access
	if len(envData) > 0 {
		resp["env_data"] = envData
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)

	log.Printf("Peer joined: %s (%s)", peer.Name, peer.IP)
}

// apiListNodes godoc
// @Summary List all nodes
// @Description Returns all nodes in the cluster (self + peers)
// @Tags nodes
// @Produce json
// @Success 200 {array} Peer
// @Router /nodes [get]
func (a *Agent) apiListNodes(w http.ResponseWriter, r *http.Request) {
	a.stateMu.RLock()
	nodes := []map[string]interface{}{
		{
			"id":        a.hwid,
			"name":      a.hostname,
			"ip":        a.ip,
			"healthy":   true,
			"last_seen": time.Now(),
			"is_self":   true,
			"version":   Version,
			"arch":      runtime.GOARCH,
		},
	}
	for _, p := range a.state.Peers {
		nodes = append(nodes, map[string]interface{}{
			"id":        p.ID,
			"name":      p.Name,
			"ip":        p.IP,
			"healthy":   p.Healthy,
			"last_seen": p.LastSeen,
			"is_self":   false,
			"version":   p.Version,
			"arch":      p.Arch,
		})
	}
	a.stateMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodes)
}

// apiRemoveNode godoc
// @Summary Remove a node from the cluster
// @Description Removes a peer node from the cluster. Workloads owned by that node will be orphaned and eligible for failover if revive is enabled.
// @Tags nodes
// @Produce json
// @Param id path string true "Node ID (HWID) or name"
// @Success 200 {object} map[string]string
// @Failure 400 {object} ErrorResponse "Cannot remove self"
// @Failure 404 {object} ErrorResponse "Node not found"
// @Router /nodes/{id} [delete]
func (a *Agent) apiRemoveNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["id"]

	// Can't remove self
	if nodeID == a.hwid || nodeID == a.hostname {
		http.Error(w, "cannot remove self from cluster", 400)
		return
	}

	a.stateMu.Lock()

	// Find peer by ID or name
	var found *Peer
	var foundID string
	for id, p := range a.state.Peers {
		if p.ID == nodeID || p.Name == nodeID {
			found = p
			foundID = id
			break
		}
	}

	if found == nil {
		a.stateMu.Unlock()
		http.Error(w, "node not found", 404)
		return
	}

	// Count workloads owned by this node
	var orphanedWorkloads []string
	for _, wl := range a.state.Workloads {
		if wl.Owner == found.ID {
			orphanedWorkloads = append(orphanedWorkloads, wl.Name)
		}
	}

	// Remove the peer
	delete(a.state.Peers, foundID)
	a.stateMu.Unlock()

	// Clean up IPIP tunnel to this peer
	a.removePeerTunnel(found.ID)

	// Clean up routes for workloads that were owned by this peer
	a.stateMu.Lock()
	a.updateWorkloadRoutes()
	a.stateMu.Unlock()

	a.updateHosts()
	a.saveState()
	a.broadcastState()

	log.Printf("Removed node: %s (%s), orphaned workloads: %v", found.Name, found.ID, orphanedWorkloads)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"removed":            found.Name,
		"id":                 found.ID,
		"orphaned_workloads": orphanedWorkloads,
		"message":            "node removed; orphaned workloads will failover if revive is enabled",
	})
}

// apiUpdateNode godoc
// @Summary Update a node (self-update)
// @Description Triggers a self-update of the node by pulling a new Docker image and restarting. Only works on the target node itself - requests to other nodes will be proxied.
// @Tags nodes
// @Accept json
// @Produce json
// @Param id path string true "Node ID (HWID), name, or 'self'"
// @Param request body NodeUpdateRequest true "Update request with image to pull"
// @Success 200 {object} NodeUpdateResponse
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 404 {object} ErrorResponse "Node not found"
// @Failure 500 {object} ErrorResponse "Update failed"
// @Router /nodes/{id}/update [post]
func (a *Agent) apiUpdateNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["id"]

	// Parse request body
	var req struct {
		Image string `json:"image"` // Docker image to pull (e.g., "ghcr.io/ncwardell/jetty:2.1.0")
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", 400)
		return
	}

	if req.Image == "" {
		http.Error(w, "image is required", 400)
		return
	}

	// Check if this is for self or another node
	isSelf := nodeID == "self" || nodeID == a.hwid || nodeID == a.hostname

	if !isSelf {
		// Find the peer and proxy the request
		a.stateMu.RLock()
		var targetPeer *Peer
		for _, p := range a.state.Peers {
			if p.ID == nodeID || p.Name == nodeID {
				targetPeer = p
				break
			}
		}
		a.stateMu.RUnlock()

		if targetPeer == nil {
			http.Error(w, "node not found", 404)
			return
		}

		// Proxy request to target node
		proxyURL := fmt.Sprintf("http://%s:%d/api/nodes/self/update", targetPeer.IP, a.apiPort)
		reqBody, _ := json.Marshal(req)
		proxyReq, _ := http.NewRequest("POST", proxyURL, strings.NewReader(string(reqBody)))
		proxyReq.Header.Set("Content-Type", "application/json")
		if a.clusterSecret != "" {
			proxyReq.Header.Set("X-API-Key", a.clusterSecret)
		}

		resp, err := peerClient.Do(proxyReq)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to reach node: %v", err), 503)
			return
		}
		defer resp.Body.Close()

		// Forward response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Self-update: pull image and restart
	log.Printf("Starting self-update to image: %s", req.Image)

	// Step 1: Pull the new image
	pullCmd := exec.Command("docker", "pull", req.Image)
	pullOutput, err := pullCmd.CombinedOutput()
	if err != nil {
		log.Printf("Failed to pull image %s: %v\nOutput: %s", req.Image, err, pullOutput)
		http.Error(w, fmt.Sprintf("failed to pull image: %v", err), 500)
		return
	}
	log.Printf("Pulled image: %s", req.Image)

	// Step 2: Get our container ID
	containerID, err := a.getSelfContainerID()
	if err != nil {
		log.Printf("Failed to get own container ID: %v", err)
		http.Error(w, fmt.Sprintf("failed to get container ID: %v", err), 500)
		return
	}
	log.Printf("Self container ID: %s", containerID)

	// Step 3: Inspect self to get mounts and config
	inspectCmd := exec.Command("docker", "inspect", containerID)
	inspectOutput, err := inspectCmd.Output()
	if err != nil {
		log.Printf("Failed to inspect container: %v", err)
		http.Error(w, fmt.Sprintf("failed to inspect container: %v", err), 500)
		return
	}

	var containers []struct {
		HostConfig struct {
			Binds       []string `json:"Binds"`
			NetworkMode string   `json:"NetworkMode"`
			Privileged  bool     `json:"Privileged"`
			CapAdd      []string `json:"CapAdd"`
		} `json:"HostConfig"`
		Config struct {
			Env []string `json:"Env"`
		} `json:"Config"`
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(inspectOutput, &containers); err != nil {
		log.Printf("Failed to parse inspect output: %v", err)
		http.Error(w, fmt.Sprintf("failed to parse container config: %v", err), 500)
		return
	}

	if len(containers) == 0 {
		http.Error(w, "container not found", 500)
		return
	}

	container := containers[0]
	containerName := strings.TrimPrefix(container.Name, "/")

	// Step 4: Build docker run command for new container
	args := []string{"run", "-d", "--name", containerName + "-update"}

	// Add bind mounts (WARP state is persisted in /data/warp via symlink)
	hasLibModules := false
	for _, bind := range container.HostConfig.Binds {
		args = append(args, "-v", bind)
		if strings.HasPrefix(bind, "/lib/modules:") {
			hasLibModules = true
		}
	}

	// Ensure /lib/modules is mounted for kernel module loading (IPIP/GRE tunnels)
	if !hasLibModules {
		args = append(args, "-v", "/lib/modules:/lib/modules:ro")
	}

	// Add network mode
	if container.HostConfig.NetworkMode != "" {
		args = append(args, "--network", string(container.HostConfig.NetworkMode))
	}

	// Add privileged if set
	if container.HostConfig.Privileged {
		args = append(args, "--privileged")
	}

	// Add capabilities
	for _, cap := range container.HostConfig.CapAdd {
		args = append(args, "--cap-add", cap)
	}

	// Pass the cluster secret key to the new container
	// Other secrets (WARP token, etc.) are persisted in /data and read by entrypoint
	if a.clusterSecret != "" {
		args = append(args, "-e", "JETTY_SECRET="+a.clusterSecret)
	}

	// Add restart policy
	args = append(args, "--restart", "unless-stopped")

	// Add the image
	args = append(args, req.Image)

	log.Printf("Creating new container with: docker %v", args)

	// Step 5: Create new container (but don't start it yet - avoids port conflict)
	// Change "run" to "create" so we can control when it starts
	args[0] = "create" // Replace "run" with "create"
	// Remove -d flag since create doesn't use it
	newArgs := make([]string, 0, len(args))
	for _, arg := range args {
		if arg != "-d" {
			newArgs = append(newArgs, arg)
		}
	}

	createCmd := exec.Command("docker", newArgs...)
	createOutput, err := createCmd.CombinedOutput()
	if err != nil {
		log.Printf("Failed to create new container: %v\nOutput: %s", err, createOutput)
		http.Error(w, fmt.Sprintf("failed to create new container: %v", err), 500)
		return
	}
	newContainerID := strings.TrimSpace(string(createOutput))
	log.Printf("Created new container (not started): %s", newContainerID)

	// Respond before stopping self - the actual switch happens in the goroutine
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "updating",
		"message": "new container created, switching over",
		"image":   req.Image,
		"old_id":  containerID,
		"new_id":  newContainerID,
	})

	// Step 6: Perform the actual switch in a goroutine after response is sent
	// CRITICAL: Stop old container BEFORE starting new one to avoid port conflicts
	go func() {
		time.Sleep(500 * time.Millisecond) // Give time for response to be sent

		// Rename old container first (while it's still running)
		log.Printf("Renaming old container %s to %s-old", containerName, containerName)
		renameOldCmd := exec.Command("docker", "rename", containerName, containerName+"-old")
		if out, err := renameOldCmd.CombinedOutput(); err != nil {
			log.Printf("Warning: failed to rename old container: %v, output: %s", err, out)
		}

		// Rename new container to original name (while it's stopped)
		log.Printf("Renaming new container to %s", containerName)
		renameNewCmd := exec.Command("docker", "rename", containerName+"-update", containerName)
		if out, err := renameNewCmd.CombinedOutput(); err != nil {
			log.Printf("Warning: failed to rename new container: %v, output: %s", err, out)
		}

		// Stop old container FIRST to release the port
		log.Printf("Stopping old container %s", containerID)
		stopCmd := exec.Command("docker", "stop", "-t", "5", containerID)
		if out, err := stopCmd.CombinedOutput(); err != nil {
			log.Printf("Warning: failed to stop old container: %v, output: %s", err, out)
		}

		// NOW start the new container (port is free)
		log.Printf("Starting new container %s", newContainerID)
		startCmd := exec.Command("docker", "start", newContainerID)
		if out, err := startCmd.CombinedOutput(); err != nil {
			log.Printf("CRITICAL: Failed to start new container: %v, output: %s", err, out)
			// Try to restart old container as fallback
			log.Printf("Attempting to restart old container as fallback...")
			exec.Command("docker", "start", containerID).Run()
		} else {
			// New container started successfully - remove the old one
			log.Printf("Removing old container %s", containerID)
			exec.Command("docker", "rm", containerID).Run()
		}

		// If we get here and old container didn't stop us, force exit
		os.Exit(0)
	}()
}

// getSelfContainerID returns this container's ID
func (a *Agent) getSelfContainerID() (string, error) {
	// Method 1: Check hostname (often the container ID)
	hostname, _ := os.Hostname()
	if len(hostname) == 12 || len(hostname) == 64 {
		// Verify it's a valid container ID
		cmd := exec.Command("docker", "inspect", hostname, "--format", "{{.Id}}")
		if out, err := cmd.Output(); err == nil {
			return strings.TrimSpace(string(out)), nil
		}
	}

	// Method 2: Look for container with jetty in the name that's running
	cmd := exec.Command("docker", "ps", "-q", "--filter", "name=jetty", "--filter", "status=running")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("docker ps failed: %w", err)
	}

	containers := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(containers) == 0 || containers[0] == "" {
		return "", fmt.Errorf("no running jetty container found")
	}

	// If multiple, find the one whose PID 1 matches our process tree
	if len(containers) == 1 {
		return containers[0], nil
	}

	// Method 3: Check /proc/1/cpuset for container ID
	cpuset, err := os.ReadFile("/proc/1/cpuset")
	if err == nil {
		// Format: /docker/<container_id>
		parts := strings.Split(strings.TrimSpace(string(cpuset)), "/")
		if len(parts) >= 3 && parts[1] == "docker" {
			return parts[2], nil
		}
	}

	// Method 4: Check cgroup
	cgroup, err := os.ReadFile("/proc/self/cgroup")
	if err == nil {
		lines := strings.Split(string(cgroup), "\n")
		for _, line := range lines {
			// Look for docker in the cgroup path
			if strings.Contains(line, "docker") {
				parts := strings.Split(line, "/")
				for _, p := range parts {
					if len(p) == 64 {
						return p, nil
					}
				}
			}
		}
	}

	// Return first container as fallback
	return containers[0], nil
}

// apiHealth godoc
// @Summary Health check
// @Description Returns health status of this node or the entire cluster. Use node=local for just this node.
// @Tags cluster
// @Produce json
// @Param node query string false "Filter by node: 'local' for this node only, or node name/ID for specific node"
// @Success 200 {object} HealthResponse
// @Router /health [get]
func (a *Agent) apiHealth(w http.ResponseWriter, r *http.Request) {
	nodeFilter := r.URL.Query().Get("node")

	// Get list of peers
	a.stateMu.RLock()
	peers := make([]*Peer, 0, len(a.state.Peers))
	for _, p := range a.state.Peers {
		peers = append(peers, p)
	}
	a.stateMu.RUnlock()

	type NodeHealth struct {
		ID        string                 `json:"id"`
		Name      string                 `json:"name"`
		IP        string                 `json:"ip"`
		PublicIP  string                 `json:"public_ip,omitempty"`
		Healthy   bool                   `json:"healthy"`
		Status    string                 `json:"status"`
		Workloads []string               `json:"workloads"`
		System    map[string]interface{} `json:"system,omitempty"`
		Error     string                 `json:"error,omitempty"`
	}

	var results []NodeHealth
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Check if we should include this node
	// "local" is a special filter meaning "only this node"
	includeLocal := nodeFilter == "" || nodeFilter == "local" || nodeFilter == a.hwid || nodeFilter == a.hostname

	// Get local node health
	if includeLocal {
		// Gather workload info under lock, but run docker commands outside
		a.stateMu.RLock()
		type workloadInfo struct {
			name string
			ip   string
		}
		var localWLInfo []workloadInfo
		for _, wl := range a.state.Workloads {
			if wl.Owner == a.hwid {
				localWLInfo = append(localWLInfo, workloadInfo{name: wl.Name, ip: wl.IP})
			}
		}
		a.stateMu.RUnlock()

		// Check docker status outside the lock to avoid blocking
		var localWorkloads []string
		for _, wl := range localWLInfo {
			out, _ := exec.Command("docker", "ps", "-q", "-f", "label=com.docker.compose.project=jetty_"+wl.name).Output()
			status := "stopped"
			if len(strings.TrimSpace(string(out))) > 0 {
				status = "running"
			}
			localWorkloads = append(localWorkloads, fmt.Sprintf("%s:%s:%s", wl.name, wl.ip, status))
		}

		localHealth := NodeHealth{
			ID:        a.hwid,
			Name:      a.hostname,
			IP:        a.ip,
			PublicIP:  a.publicIP,
			Healthy:   true,
			Status:    getHealthStatus(),
			Workloads: localWorkloads,
			System:    a.getSystemStats(),
		}
		results = append(results, localHealth)
	}

	// Fetch health from peers concurrently
	for _, peer := range peers {
		// Check node filter
		if nodeFilter != "" && nodeFilter != peer.ID && nodeFilter != peer.Name {
			continue
		}

		wg.Add(1)
		go func(p *Peer) {
			defer wg.Done()

			health := NodeHealth{
				ID:      p.ID,
				Name:    p.Name,
				IP:      p.IP,
				Healthy: p.Healthy,
			}

			// Use shorter timeout for unhealthy peers
			client := peerClient
			if !p.Healthy {
				client = unhealthyPeerClient
				health.Status = "unreachable"
				health.Error = "peer marked unhealthy"
			}

			// Skip if peer has no IP
			url := a.getPeerAPIURL(p, "/api/health?node=local")
			if url == "" {
				health.Status = "unreachable"
				health.Error = "no route to peer"
				mu.Lock()
				results = append(results, health)
				mu.Unlock()
				return
			}

			// Fetch health from peer
			resp, err := client.Get(url)
			if err != nil {
				health.Status = "unreachable"
				health.Error = err.Error()
				health.Healthy = false
				mu.Lock()
				results = append(results, health)
				mu.Unlock()
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				health.Status = "error"
				health.Error = fmt.Sprintf("status %d", resp.StatusCode)
				mu.Lock()
				results = append(results, health)
				mu.Unlock()
				return
			}

			var peerHealth map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&peerHealth); err != nil {
				health.Status = "error"
				health.Error = "failed to decode response"
				mu.Lock()
				results = append(results, health)
				mu.Unlock()
				return
			}

			// Extract data from peer's local health response
			health.Healthy = true
			if nodes, ok := peerHealth["nodes"].([]interface{}); ok && len(nodes) > 0 {
				// Peer returned cluster format, extract first node's data
				if node, ok := nodes[0].(map[string]interface{}); ok {
					if status, ok := node["status"].(string); ok {
						health.Status = status
					}
					if pubIP, ok := node["public_ip"].(string); ok {
						health.PublicIP = pubIP
					}
					if wls, ok := node["workloads"].([]interface{}); ok {
						for _, wl := range wls {
							health.Workloads = append(health.Workloads, fmt.Sprintf("%v", wl))
						}
					}
					if sys, ok := node["system"].(map[string]interface{}); ok {
						health.System = sys
					}
				}
			}

			mu.Lock()
			results = append(results, health)
			mu.Unlock()
		}(peer)
	}

	wg.Wait()

	// Calculate cluster summary
	totalNodes := len(results)
	healthyNodes := 0
	totalWorkloads := 0
	for _, h := range results {
		if h.Healthy && h.Status != "error" && h.Status != "unreachable" {
			healthyNodes++
		}
		totalWorkloads += len(h.Workloads)
	}

	clusterStatus := "healthy"
	if healthyNodes < totalNodes {
		if healthyNodes == 0 {
			clusterStatus = "degraded"
		} else {
			clusterStatus = "partial"
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"cluster_status":  clusterStatus,
		"total_nodes":     totalNodes,
		"healthy_nodes":   healthyNodes,
		"total_workloads": totalWorkloads,
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
		"nodes":           results,
	})
}

// cpuSampleLoop runs in the background and continuously samples CPU usage.
// This provides accurate CPU metrics without blocking API requests.
func (a *Agent) cpuSampleLoop() {
	getCPUTimes := func() (idle, total uint64) {
		if data, err := os.ReadFile("/proc/stat"); err == nil {
			lines := strings.Split(string(data), "\n")
			if len(lines) > 0 && strings.HasPrefix(lines[0], "cpu ") {
				fields := strings.Fields(lines[0])
				if len(fields) >= 5 {
					var sum uint64
					for i := 1; i < len(fields); i++ {
						val, _ := strconv.ParseUint(fields[i], 10, 64)
						sum += val
						// idle (field 4) and iowait (field 5) = CPU not working
						if i == 4 || i == 5 {
							idle += val
						}
					}
					total = sum
				}
			}
		}
		return
	}

	// Sample every 2 seconds for smooth, accurate readings
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var lastIdle, lastTotal uint64
	lastIdle, lastTotal = getCPUTimes()

	for {
		select {
		case <-ticker.C:
			idle, total := getCPUTimes()
			if total > lastTotal {
				idleDelta := float64(idle - lastIdle)
				totalDelta := float64(total - lastTotal)
				cpuPercent := (1.0 - idleDelta/totalDelta) * 100

				a.cachedCPUMu.Lock()
				a.cachedCPUPercent = cpuPercent
				a.cachedCPUMu.Unlock()
			}
			lastIdle, lastTotal = idle, total
		case <-a.stopCh:
			return
		}
	}
}

// getSystemStats returns CPU, memory, and disk statistics for the node
func (a *Agent) getSystemStats() map[string]interface{} {
	stats := make(map[string]interface{})

	// Get memory info from /proc/meminfo
	if memInfo, err := os.ReadFile("/proc/meminfo"); err == nil {
		var memTotal, memAvail, memFree, buffers, cached uint64
		lines := strings.Split(string(memInfo), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			value, _ := strconv.ParseUint(fields[1], 10, 64)
			switch fields[0] {
			case "MemTotal:":
				memTotal = value * 1024 // Convert KB to bytes
			case "MemAvailable:":
				memAvail = value * 1024
			case "MemFree:":
				memFree = value * 1024
			case "Buffers:":
				buffers = value * 1024
			case "Cached:":
				cached = value * 1024
			}
		}
		memUsed := memTotal - memAvail
		if memAvail == 0 {
			// Fallback for older kernels without MemAvailable
			memUsed = memTotal - memFree - buffers - cached
		}
		stats["memory_total"] = formatBytes(memTotal)
		stats["memory_used"] = formatBytes(memUsed)
		stats["memory_available"] = formatBytes(memAvail)
		if memTotal > 0 {
			stats["memory_percent"] = fmt.Sprintf("%.1f%%", float64(memUsed)/float64(memTotal)*100)
		}
	}

	// Get cached CPU usage (updated by background cpuSampleLoop)
	a.cachedCPUMu.RLock()
	cpuPercent := a.cachedCPUPercent
	a.cachedCPUMu.RUnlock()
	stats["cpu_percent"] = fmt.Sprintf("%.1f%%", cpuPercent)

	// Get load average
	if loadAvg, err := os.ReadFile("/proc/loadavg"); err == nil {
		fields := strings.Fields(string(loadAvg))
		if len(fields) >= 3 {
			stats["load_average"] = fmt.Sprintf("%s %s %s", fields[0], fields[1], fields[2])
		}
	}

	// Get disk usage for /data
	if out, err := exec.Command("df", "-B1", a.dataDir).Output(); err == nil {
		lines := strings.Split(string(out), "\n")
		if len(lines) >= 2 {
			fields := strings.Fields(lines[1])
			if len(fields) >= 5 {
				total, _ := strconv.ParseUint(fields[1], 10, 64)
				used, _ := strconv.ParseUint(fields[2], 10, 64)
				avail, _ := strconv.ParseUint(fields[3], 10, 64)
				stats["disk_total"] = formatBytes(total)
				stats["disk_used"] = formatBytes(used)
				stats["disk_available"] = formatBytes(avail)
				stats["disk_percent"] = fields[4]
			}
		}
	}

	// Get Jetty uptime (not system uptime)
	stats["uptime"] = time.Since(a.startTime).Round(time.Second).String()

	return stats
}

// formatBytes converts bytes to human-readable format
func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// getHealthStatus returns "healthy", "medium", or "full" based on resource usage
// healthy: CPU < 50% and memory < 60%
// medium: CPU 50-80% or memory 60-85%
// full: CPU > 80% or memory > 85%
func getHealthStatus() string {
	var memPercent, cpuPercent float64

	// Get memory percentage
	if memInfo, err := os.ReadFile("/proc/meminfo"); err == nil {
		var memTotal, memAvail, memFree, buffers, cached uint64
		lines := strings.Split(string(memInfo), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			value, _ := strconv.ParseUint(fields[1], 10, 64)
			switch fields[0] {
			case "MemTotal:":
				memTotal = value
			case "MemAvailable:":
				memAvail = value
			case "MemFree:":
				memFree = value
			case "Buffers:":
				buffers = value
			case "Cached:":
				cached = value
			}
		}
		memUsed := memTotal - memAvail
		if memAvail == 0 {
			memUsed = memTotal - memFree - buffers - cached
		}
		if memTotal > 0 {
			memPercent = float64(memUsed) / float64(memTotal) * 100
		}
	}

	// Get CPU percentage (quick sample)
	getCPUTimes := func() (idle, total uint64) {
		if data, err := os.ReadFile("/proc/stat"); err == nil {
			lines := strings.Split(string(data), "\n")
			if len(lines) > 0 && strings.HasPrefix(lines[0], "cpu ") {
				fields := strings.Fields(lines[0])
				if len(fields) >= 5 {
					var sum uint64
					for i := 1; i < len(fields); i++ {
						val, _ := strconv.ParseUint(fields[i], 10, 64)
						sum += val
						if i == 4 {
							idle = val
						}
					}
					total = sum
				}
			}
		}
		return
	}

	idle1, total1 := getCPUTimes()
	time.Sleep(50 * time.Millisecond) // Brief sample
	idle2, total2 := getCPUTimes()

	if total2 > total1 {
		idleDelta := float64(idle2 - idle1)
		totalDelta := float64(total2 - total1)
		cpuPercent = (1.0 - idleDelta/totalDelta) * 100
	}

	// Determine status
	if cpuPercent > 80 || memPercent > 85 {
		return "full"
	}
	if cpuPercent >= 50 || memPercent >= 60 {
		return "medium"
	}
	return "healthy"
}

func (a *Agent) apiSync(w http.ResponseWriter, r *http.Request) {
	// Return all workloads, deleted workloads (tombstones), and env data for sync
	// This allows other nodes to get a complete view of cluster state including deletions
	a.stateMu.RLock()
	workloads := make([]*Workload, 0, len(a.state.Workloads))
	for _, wl := range a.state.Workloads {
		workloads = append(workloads, wl)
	}
	deletedWorkloads := make([]*DeletedWorkload, 0, len(a.state.DeletedWorkloads))
	for _, dw := range a.state.DeletedWorkloads {
		deletedWorkloads = append(deletedWorkloads, dw)
	}
	envData := make(map[string]string)
	for k, v := range a.state.EnvData {
		envData[k] = v
	}
	a.stateMu.RUnlock()

	// Return sync response with workloads, tombstones, and env data
	resp := map[string]interface{}{
		"workloads": workloads,
	}
	if len(deletedWorkloads) > 0 {
		resp["deleted_workloads"] = deletedWorkloads
	}
	if len(envData) > 0 {
		resp["env_data"] = envData
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// apiGetTunnel godoc
// @Summary Get tunnel status
// @Description Returns Cloudflare tunnel configuration status
// @Tags tunnel
// @Produce json
// @Success 200 {object} TunnelStatus
// @Router /tunnel [get]
func (a *Agent) apiGetTunnel(w http.ResponseWriter, r *http.Request) {
	a.stateMu.RLock()
	hasToken := a.state.CFToken != ""
	a.stateMu.RUnlock()

	resp := map[string]interface{}{
		"configured": hasToken,
		"running":    a.isTunnelRunning(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// apiSetTunnel godoc
// @Summary Configure tunnel
// @Description Sets the Cloudflare tunnel token (propagates to all nodes)
// @Tags tunnel
// @Accept json
// @Param token body TunnelRequest true "Tunnel token"
// @Success 200 "Tunnel configured"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Router /tunnel [post]
func (a *Agent) apiSetTunnel(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	if req.Token == "" {
		http.Error(w, "token required", 400)
		return
	}

	a.stateMu.Lock()
	a.state.CFToken = req.Token
	a.stateMu.Unlock()

	a.saveState()

	// Start or restart the tunnel
	if err := a.restartCloudflared(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Broadcast to peers so they also start their tunnels
	a.broadcastTunnelToken(req.Token)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"configured": true,
		"running":    a.isTunnelRunning(),
	})

	log.Printf("Cloudflare tunnel configured")
}

// apiDeleteTunnel godoc
// @Summary Remove tunnel
// @Description Removes the Cloudflare tunnel configuration
// @Tags tunnel
// @Success 204 "Tunnel removed"
// @Router /tunnel [delete]
func (a *Agent) apiDeleteTunnel(w http.ResponseWriter, r *http.Request) {
	a.stateMu.Lock()
	a.state.CFToken = ""
	a.stateMu.Unlock()

	a.stopCloudflared()
	a.saveState()

	// Notify peers to stop their tunnels
	a.broadcastTunnelToken("")

	w.WriteHeader(204)
	log.Printf("Cloudflare tunnel removed")
}

// apiListEnv godoc
// @Summary List environment variables
// @Description Returns all stored environment variable keys (values are encrypted)
// @Tags env
// @Produce json
// @Success 200 {object} EnvListResponse
// @Router /env [get]
func (a *Agent) apiListEnv(w http.ResponseWriter, r *http.Request) {
	a.stateMu.RLock()
	keys := make([]string, 0, len(a.state.EnvData))
	for key := range a.state.EnvData {
		keys = append(keys, key)
	}
	a.stateMu.RUnlock()

	sort.Strings(keys)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"keys":  keys,
		"count": len(keys),
	})
}

// apiSetEnv godoc
// @Summary Set environment variables
// @Description Stores encrypted environment variables (existing keys are overwritten)
// @Tags env
// @Accept json
// @Param env body EnvSetRequest true "Environment variables to set"
// @Success 200 {object} EnvSetResponse
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 500 {object} ErrorResponse "Encryption error"
// @Router /env [post]
func (a *Agent) apiSetEnv(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Env map[string]string `json:"env"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	if len(req.Env) == 0 {
		http.Error(w, "env map required", 400)
		return
	}

	// Validate keys (alphanumeric and underscore only)
	envKeyPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	for key := range req.Env {
		if !envKeyPattern.MatchString(key) {
			http.Error(w, fmt.Sprintf("invalid env key: %s (must start with letter or underscore, contain only alphanumerics and underscores)", key), 400)
			return
		}
	}

	// Encrypt and store values
	a.stateMu.Lock()
	added := make([]string, 0)
	updated := make([]string, 0)
	for key, value := range req.Env {
		encrypted, err := a.encryptValue(value)
		if err != nil {
			a.stateMu.Unlock()
			http.Error(w, fmt.Sprintf("encrypt %s: %v", key, err), 500)
			return
		}
		if _, exists := a.state.EnvData[key]; exists {
			updated = append(updated, key)
		} else {
			added = append(added, key)
		}
		a.state.EnvData[key] = encrypted
	}
	a.stateMu.Unlock()

	a.saveState()

	sort.Strings(added)
	sort.Strings(updated)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"added":   added,
		"updated": updated,
	})

	log.Printf("Env set: added=%v, updated=%v", added, updated)
}

// apiGetEnv godoc
// @Summary Get environment variable
// @Description Returns the decrypted value of a specific environment variable
// @Tags env
// @Produce json
// @Param key path string true "Environment variable key"
// @Success 200 {object} EnvGetResponse
// @Failure 404 {object} ErrorResponse "Key not found"
// @Failure 500 {object} ErrorResponse "Decryption error"
// @Router /env/{key} [get]
func (a *Agent) apiGetEnv(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]

	a.stateMu.RLock()
	encrypted, exists := a.state.EnvData[key]
	a.stateMu.RUnlock()

	if !exists {
		http.Error(w, "env key not found", 404)
		return
	}

	value, err := a.decryptValue(encrypted)
	if err != nil {
		http.Error(w, fmt.Sprintf("decrypt: %v", err), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"key":   key,
		"value": value,
	})
}

// apiDeleteEnv godoc
// @Summary Delete environment variable
// @Description Removes an environment variable from storage
// @Tags env
// @Param key path string true "Environment variable key"
// @Success 204 "Variable deleted"
// @Failure 404 {object} ErrorResponse "Key not found"
// @Router /env/{key} [delete]
func (a *Agent) apiDeleteEnv(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]

	a.stateMu.Lock()
	_, exists := a.state.EnvData[key]
	if exists {
		delete(a.state.EnvData, key)
	}
	a.stateMu.Unlock()

	if !exists {
		http.Error(w, "env key not found", 404)
		return
	}

	a.saveState()

	w.WriteHeader(204)
	log.Printf("Env deleted: %s", key)
}

func (a *Agent) apiPeerAnnounce(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Secret string `json:"secret"`
		Peer   Peer   `json:"peer"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// Validate cluster secret
	if a.clusterSecret != "" && req.Secret != a.clusterSecret {
		http.Error(w, "invalid cluster secret", 401)
		return
	}

	// Don't add ourselves as a peer
	if req.Peer.ID == a.hwid {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ignored", "reason": "self"})
		return
	}

	req.Peer.Healthy = true
	req.Peer.LastSeen = time.Now()

	// Check if peer IP changed
	a.stateMu.Lock()
	oldPeer := a.state.Peers[req.Peer.ID]
	oldIP := ""
	if oldPeer != nil {
		oldIP = oldPeer.IP
	}
	ipChanged := oldIP != "" && oldIP != req.Peer.IP
	a.state.Peers[req.Peer.ID] = &req.Peer
	a.stateMu.Unlock()

	a.updateHosts()
	a.saveState()

	// Always ensure we have an IPIP tunnel to this peer (for receiving their traffic)
	// IPIP requires both sides to have matching tunnels
	if req.Peer.IP != "" {
		if err := a.ensurePeerTunnel(req.Peer.ID, req.Peer.IP); err != nil {
			log.Printf("Warning: failed to create tunnel to %s: %v", req.Peer.Name, err)
		}
	}

	// If peer IP changed, also update workload routes
	if ipChanged {
		log.Printf("Peer %s IP changed: %s -> %s", req.Peer.Name, oldIP, req.Peer.IP)
		a.stateMu.Lock()
		a.updateWorkloadRoutes()
		a.stateMu.Unlock()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

	log.Printf("Peer announced: %s (%s)", req.Peer.Name, req.Peer.IP)
}

// apiHeartbeat receives heartbeats from peers in tunnel-only mode.
// This allows peers to track each other's health through the Cloudflare tunnel.
func (a *Agent) apiHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Secret string `json:"secret"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// Validate cluster secret
	if a.clusterSecret != "" && req.Secret != a.clusterSecret {
		http.Error(w, "invalid cluster secret", 401)
		return
	}

	// Ignore our own heartbeat (can happen when tunnel routes back to us)
	if req.ID == a.hwid {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "self"})
		return
	}

	// Update peer's LastSeen
	a.stateMu.Lock()
	if peer, ok := a.state.Peers[req.ID]; ok {
		peer.LastSeen = time.Now()
		peer.Healthy = true
	}
	a.stateMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "received_by": a.hostname})
}

// apiWorkloadProxy proxies HTTP requests to workloads.
// URL format: /api/proxy/{mesh_ip}/{path...}
// If the workload is local, forwards to the container directly.
// If remote, forwards through the tunnel or mesh IP.
func (a *Agent) apiWorkloadProxy(w http.ResponseWriter, r *http.Request) {
	// Parse path: /api/proxy/10.100.1.5/some/path
	path := strings.TrimPrefix(r.URL.Path, "/api/proxy/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "mesh_ip required in path", 400)
		return
	}

	meshIP := parts[0]
	targetPath := "/"
	if len(parts) > 1 {
		targetPath = "/" + parts[1]
	}

	// Validate mesh IP format
	if net.ParseIP(meshIP) == nil {
		http.Error(w, "invalid mesh_ip", 400)
		return
	}

	// Find the workload
	a.stateMu.RLock()
	workload := a.state.Workloads[meshIP]
	var owner *Peer
	if workload != nil && workload.Owner != a.hwid {
		owner = a.state.Peers[workload.Owner]
	}
	a.stateMu.RUnlock()

	if workload == nil {
		http.Error(w, "workload not found", 404)
		return
	}

	var targetURL string

	if workload.Owner == a.hwid {
		// Local workload - forward to mesh IP directly (DNAT handles it)
		targetURL = fmt.Sprintf("http://%s%s", meshIP, targetPath)
	} else if owner != nil {
		// Remote workload - forward to owner node
		if a.tunnelDomain != "" {
			// Tunnel mode: use tunnel domain with proxy path
			targetURL = fmt.Sprintf("https://%s/api/proxy/%s%s", a.tunnelDomain, meshIP, targetPath)
		} else {
			// Direct mode: use owner's mesh IP
			targetURL = fmt.Sprintf("http://%s:%d/api/proxy/%s%s", owner.IP, a.apiPort, meshIP, targetPath)
		}
	} else {
		http.Error(w, "workload owner not found", 503)
		return
	}

	// Create proxy request
	proxyReq, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Copy headers
	for k, v := range r.Header {
		// Skip hop-by-hop headers
		if k == "Connection" || k == "Keep-Alive" || k == "Proxy-Connection" {
			continue
		}
		proxyReq.Header[k] = v
	}

	// Add forwarding headers
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	proxyReq.Header.Set("X-Forwarded-Host", r.Host)

	// Execute request
	resp, err := httpClient.Do(proxyReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("proxy error: %v", err), 502)
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)

	// Copy response body
	io.Copy(w, resp.Body)
}

// broadcastTunnelToken sends the CF token to all peers so they can start their tunnels.
func (a *Agent) broadcastTunnelToken(token string) {
	data, _ := json.Marshal(map[string]string{"token": token})

	// In tunnel-only mode, broadcast to tunnel (one node receives, gossip propagates)
	if a.tunnelDomain != "" {
		url := a.getTunnelAPIURL("/api/tunnel/sync")
		resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
		if err != nil {
			log.Printf("Failed to broadcast tunnel token: %v", err)
		} else {
			resp.Body.Close()
		}
		return
	}

	// Direct mode: send to each peer
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	a.stateMu.RUnlock()

	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/tunnel/sync", peer.IP, a.apiPort)
		resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
		if err != nil {
			log.Printf("Failed to broadcast tunnel token to %s: %v", peer.Name, err)
			continue
		}
		resp.Body.Close()
	}
}

func (a *Agent) apiTunnelSync(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	a.stateMu.Lock()
	oldToken := a.state.CFToken
	a.state.CFToken = req.Token
	a.stateMu.Unlock()

	// Only restart if token changed
	if oldToken != req.Token {
		a.saveState()
		if req.Token == "" {
			a.stopCloudflared()
			log.Printf("Cloudflare tunnel removed via sync")
		} else {
			if err := a.restartCloudflared(); err != nil {
				log.Printf("Failed to start tunnel after sync: %v", err)
			} else {
				log.Printf("Cloudflare tunnel started via sync")
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// =============================================================================
// Docker Compose
// =============================================================================

// autostartWorkloads starts all owned workloads that have autostart enabled
func (a *Agent) autostartWorkloads() {
	a.stateMu.RLock()
	var toStart []*Workload
	for _, wl := range a.state.Workloads {
		if wl.Owner == a.hwid && wl.Autostart {
			toStart = append(toStart, wl)
		}
	}
	a.stateMu.RUnlock()

	if len(toStart) == 0 {
		return
	}

	log.Printf("Auto-starting %d workload(s)...", len(toStart))
	for _, wl := range toStart {
		if err := a.deployWorkload(wl); err != nil {
			log.Printf("Failed to auto-start %s: %v", wl.Name, err)
		}
	}
}

// getComposeForArch returns the appropriate compose content for the current architecture.
// Priority: arch-specific compose (compose_amd64/compose_arm64) > default compose
func (wl *Workload) getComposeForArch() string {
	arch := runtime.GOARCH
	switch arch {
	case "amd64":
		if wl.ComposeAmd64 != "" {
			return wl.ComposeAmd64
		}
	case "arm64":
		if wl.ComposeArm64 != "" {
			return wl.ComposeArm64
		}
	}
	// Fall back to default compose
	return wl.Compose
}

// canRunOnArch checks if a workload has a compose file that can run on the given architecture.
// Returns true if:
// - The default Compose field is set (works as fallback for any arch), OR
// - An architecture-specific compose file exists for the given arch
func (wl *Workload) canRunOnArch(arch string) bool {
	// If there's a default compose, it can run on any architecture
	if wl.Compose != "" {
		return true
	}
	// Otherwise, check for arch-specific compose
	switch arch {
	case "amd64":
		return wl.ComposeAmd64 != ""
	case "arm64":
		return wl.ComposeArm64 != ""
	}
	// Unknown architecture - no compose available
	return false
}

func (a *Agent) deployWorkload(wl *Workload) error {
	dir := filepath.Join(a.composeDir, wl.Name)
	os.MkdirAll(dir, 0755)

	// Select compose based on node architecture
	composeContent := wl.getComposeForArch()
	if composeContent == "" {
		return fmt.Errorf("no compose file available for architecture %s", runtime.GOARCH)
	}

	// Write compose
	path := filepath.Join(dir, "docker-compose.yml")
	if err := os.WriteFile(path, []byte(composeContent), 0644); err != nil {
		return err
	}

	// Validate
	if out, err := a.composeCmd(wl.Name, "config", "--quiet"); err != nil {
		return fmt.Errorf("invalid: %s", out)
	}

	// Pull
	a.composeCmd(wl.Name, "pull")

	// Up
	if out, err := a.composeCmd(wl.Name, "up", "-d", "--remove-orphans"); err != nil {
		return fmt.Errorf("failed: %s", out)
	}

	// Setup mesh IP routing
	if wl.IP != "" {
		a.setupWorkloadIP(wl)
	}

	log.Printf("Deployed: %s @ %s", wl.Name, wl.IP)
	return nil
}

func (a *Agent) removeWorkload(wl *Workload) {
	// Clean up iptables BEFORE stopping container (need container IP)
	if wl.IP != "" {
		a.cleanupWorkloadIP(wl)
	}

	a.composeCmd(wl.Name, "down", "-v", "--remove-orphans")

	dir := filepath.Join(a.composeDir, wl.Name)
	os.RemoveAll(dir)

	log.Printf("Removed: %s", wl.Name)
}

func (a *Agent) cleanupWorkloadIP(wl *Workload) {
	// Get container IP before it's gone
	out, _ := exec.Command("docker", "ps", "-q", "-f", "label=com.docker.compose.project=jetty_"+wl.Name).Output()
	if len(out) > 0 {
		containerID := strings.Split(strings.TrimSpace(string(out)), "\n")[0]
		if containerID != "" {
			out, _ = exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerID).Output()
			containerIP := strings.TrimSpace(string(out))
			if containerIP != "" {
				// Remove DNAT rules
				exec.Command("iptables", "-t", "nat", "-D", "PREROUTING", "-d", wl.IP, "-j", "DNAT", "--to", containerIP).Run()
				exec.Command("iptables", "-t", "nat", "-D", "OUTPUT", "-d", wl.IP, "-j", "DNAT", "--to", containerIP).Run()
			}
		}
	}

	// Remove mesh IP from interface
	exec.Command("ip", "addr", "del", wl.IP+"/32", "dev", "jetty0").Run()
}

func (a *Agent) setupWorkloadIP(wl *Workload) {
	// Add IP to interface
	if err := exec.Command("ip", "addr", "add", wl.IP+"/32", "dev", "jetty0").Run(); err != nil {
		// Ignore "already exists" errors
		log.Printf("Note: adding %s to jetty0: %v (may already exist)", wl.IP, err)
	}

	// Wait for container to be ready with retry logic
	var containerIP string
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		containerIP = a.getWorkloadContainerIP(wl.Name)
		if containerIP != "" {
			break
		}
		if i < maxRetries-1 {
			time.Sleep(time.Duration(500*(i+1)) * time.Millisecond) // 500ms, 1s, 1.5s, ...
		}
	}

	if containerIP == "" {
		log.Printf("Error: couldn't get container IP for %s after %d retries", wl.Name, maxRetries)
		return
	}

	// Set up DNAT rules
	if err := exec.Command("iptables", "-t", "nat", "-A", "PREROUTING", "-d", wl.IP, "-j", "DNAT", "--to", containerIP).Run(); err != nil {
		log.Printf("Error: PREROUTING DNAT for %s: %v", wl.Name, err)
	}
	if err := exec.Command("iptables", "-t", "nat", "-A", "OUTPUT", "-d", wl.IP, "-j", "DNAT", "--to", containerIP).Run(); err != nil {
		log.Printf("Error: OUTPUT DNAT for %s: %v", wl.Name, err)
	}

	log.Printf("Routed: %s -> %s", wl.IP, containerIP)
}

// getWorkloadContainerIP returns the container IP for a workload, or empty string if not found.
func (a *Agent) getWorkloadContainerIP(name string) string {
	// Get container ID
	out, err := exec.Command("docker", "ps", "-q", "-f", "label=com.docker.compose.project=jetty_"+name).Output()
	if err != nil || len(out) == 0 {
		return ""
	}

	containerID := strings.Split(strings.TrimSpace(string(out)), "\n")[0]
	if containerID == "" {
		return ""
	}

	// Get container IP
	out, err = exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerID).Output()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(out))
}

func (a *Agent) composeCmd(name string, args ...string) (string, error) {
	dir := filepath.Join(a.composeDir, name)
	path := filepath.Join(dir, "docker-compose.yml")

	cmdArgs := []string{"compose", "-f", path, "-p", "jetty_" + name}
	cmdArgs = append(cmdArgs, args...)

	cmd := exec.Command("docker", cmdArgs...)
	cmd.Dir = dir

	// Inject decrypted environment variables
	// Start with current environment
	cmd.Env = os.Environ()

	// Add decrypted Jetty env vars (these can be used in docker-compose.yml)
	envData, err := a.getDecryptedEnv()
	if err != nil {
		log.Printf("Warning: failed to decrypt env data: %v", err)
	} else {
		for key, value := range envData {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", key, value))
		}
	}

	out, err := cmd.CombinedOutput()
	return string(out), err
}

// =============================================================================
// Gossip
// =============================================================================

func (a *Agent) gossipLoop() {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

	// Run tombstone GC every 10 minutes (every 60th tick)
	gcCounter := 0

	for {
		select {
		case <-a.stopCh:
			return
		case <-tick.C:
			a.checkPeers()
			a.syncWorkloads()

			// Garbage collect old tombstones periodically
			gcCounter++
			if gcCounter >= 60 { // Every 10 minutes (60 * 10 seconds)
				a.gcTombstones()
				gcCounter = 0
			}
		}
	}
}

// gcTombstones removes tombstones older than 1 hour.
// This prevents unbounded growth of the tombstone map while ensuring
// deletions have enough time to propagate across all nodes.
func (a *Agent) gcTombstones() {
	cutoff := time.Now().Unix() - 3600 // 1 hour ago

	a.stateMu.Lock()
	removed := 0
	for ip, dw := range a.state.DeletedWorkloads {
		if dw.Version < cutoff {
			delete(a.state.DeletedWorkloads, ip)
			removed++
		}
	}
	a.stateMu.Unlock()

	if removed > 0 {
		log.Printf("GC: removed %d old tombstones", removed)
		a.saveState()
	}
}

func (a *Agent) checkPeers() {
	// In tunnel-only mode, send heartbeat through tunnel and check peer staleness
	if a.tunnelDomain != "" {
		a.tunnelModeHealthCheck()
		return
	}

	// Direct mode: check each peer individually via mesh IP
	// Copy peer info outside the lock to avoid holding mutex during network calls
	type peerCheck struct {
		id string
		ip string
	}
	a.stateMu.RLock()
	peers := make([]peerCheck, 0, len(a.state.Peers))
	for _, p := range a.state.Peers {
		peers = append(peers, peerCheck{id: p.ID, ip: p.IP})
	}
	a.stateMu.RUnlock()

	// Check each peer without holding the mutex
	type peerResult struct {
		id      string
		healthy bool
	}
	results := make([]peerResult, 0, len(peers))

	for _, peer := range peers {
		// Use ?node=local to get only the peer's local health, avoiding recursive peer queries
		url := fmt.Sprintf("http://%s:%d/api/health?node=local", peer.ip, a.apiPort)
		resp, err := httpClient.Get(url)

		result := peerResult{id: peer.id, healthy: false}
		if err == nil {
			result.healthy = resp.StatusCode == 200
			resp.Body.Close()
		}
		results = append(results, result)
	}

	// Update peer status under the lock
	a.stateMu.Lock()
	defer a.stateMu.Unlock()

	now := time.Now()
	for _, result := range results {
		if peer, ok := a.state.Peers[result.id]; ok {
			peer.Healthy = result.healthy
			if result.healthy {
				peer.LastSeen = now
			}
		}
	}
}

// tunnelModeHealthCheck handles peer health in tunnel-only mode.
// Since we can't directly reach peers, we:
// 1. Send our heartbeat through the tunnel (any node receiving it updates our LastSeen)
// 2. Check peer staleness based on LastSeen
func (a *Agent) tunnelModeHealthCheck() {
	// Send our heartbeat through the tunnel
	heartbeat := map[string]interface{}{
		"id":     a.hwid,
		"name":   a.hostname,
		"secret": a.clusterSecret,
	}
	data, _ := json.Marshal(heartbeat)

	url := a.getTunnelAPIURL("/api/heartbeat")
	resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
	if err != nil {
		// Only log heartbeat failures once per minute to avoid log spam
		if time.Since(a.lastHeartbeatErrLog) > time.Minute {
			log.Printf("Heartbeat failed: %v (suppressing further errors for 1 min)", err)
			a.lastHeartbeatErrLog = time.Now()
		}
	} else {
		resp.Body.Close()
	}

	// Check peer health based on LastSeen staleness
	a.stateMu.Lock()

	staleThreshold := 45 * time.Second // 3 missed heartbeats (15s interval) + buffer
	now := time.Now()
	healthChanged := false

	for _, peer := range a.state.Peers {
		wasHealthy := peer.Healthy
		if now.Sub(peer.LastSeen) > staleThreshold {
			if peer.Healthy {
				log.Printf("Peer %s marked unhealthy (no heartbeat for %v)", peer.Name, now.Sub(peer.LastSeen))
			}
			peer.Healthy = false
		} else {
			peer.Healthy = true
		}
		if wasHealthy != peer.Healthy {
			healthChanged = true
		}
	}

	// Update routes if any peer health status changed
	// This ensures routes to unhealthy peers are removed immediately
	if healthChanged {
		a.updateWorkloadRoutes()
	}
	a.stateMu.Unlock()
}

// syncStateOnStartup syncs state from known peers before autostarting workloads.
// This handles the case where we restarted and workloads were revived by other nodes.
func (a *Agent) syncStateOnStartup() {
	a.stateMu.RLock()
	peerCount := len(a.state.Peers)
	peers := make([]*Peer, 0, peerCount)
	for _, p := range a.state.Peers {
		peers = append(peers, p)
	}
	a.stateMu.RUnlock()

	if peerCount == 0 {
		log.Printf("No known peers - skipping startup sync")
		return
	}

	log.Printf("Syncing state from %d known peer(s) before autostart...", peerCount)
	synced := false

	// Try tunnel domain first if configured
	if a.tunnelDomain != "" {
		url := fmt.Sprintf("https://%s/api/sync", a.tunnelDomain)
		resp, err := httpClient.Get(url)
		if err == nil {
			var syncResp struct {
				Workloads        []*Workload        `json:"workloads"`
				DeletedWorkloads []*DeletedWorkload `json:"deleted_workloads,omitempty"`
				EnvData          map[string]string  `json:"env_data,omitempty"`
			}
			json.NewDecoder(resp.Body).Decode(&syncResp)
			resp.Body.Close()

			a.stateMu.Lock()
			// Process deleted workloads (tombstones) first
			for _, dw := range syncResp.DeletedWorkloads {
				existingTombstone := a.state.DeletedWorkloads[dw.IP]
				if existingTombstone == nil || dw.Version > existingTombstone.Version {
					a.state.DeletedWorkloads[dw.IP] = dw
				}
				existing := a.state.Workloads[dw.IP]
				if existing != nil && dw.Version > existing.Version {
					log.Printf("Startup sync: workload %s was deleted while we were down", existing.Name)
					delete(a.state.Workloads, dw.IP)
				}
			}
			// Process workloads
			for _, w := range syncResp.Workloads {
				tombstone := a.state.DeletedWorkloads[w.IP]
				if tombstone != nil && tombstone.Version > w.Version {
					continue
				}
				existing := a.state.Workloads[w.IP]
				if existing == nil || w.Version > existing.Version {
					if existing != nil && existing.Owner == a.hwid && w.Owner != a.hwid {
						log.Printf("Workload %s was revived by %s while we were down", w.Name, w.Owner[:12])
					}
					a.state.Workloads[w.IP] = w
					if tombstone != nil {
						delete(a.state.DeletedWorkloads, w.IP)
					}
				}
			}
			// Merge env data from peer
			for k, v := range syncResp.EnvData {
				a.state.EnvData[k] = v
			}
			a.stateMu.Unlock()
			synced = true
		}
	}

	// Try direct peer connections
	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/sync", peer.IP, a.apiPort)
		resp, err := httpClient.Get(url)
		if err != nil {
			continue
		}

		var syncResp struct {
			Workloads        []*Workload        `json:"workloads"`
			DeletedWorkloads []*DeletedWorkload `json:"deleted_workloads,omitempty"`
			EnvData          map[string]string  `json:"env_data,omitempty"`
		}
		json.NewDecoder(resp.Body).Decode(&syncResp)
		resp.Body.Close()

		a.stateMu.Lock()
		// Process deleted workloads (tombstones) first
		for _, dw := range syncResp.DeletedWorkloads {
			existingTombstone := a.state.DeletedWorkloads[dw.IP]
			if existingTombstone == nil || dw.Version > existingTombstone.Version {
				a.state.DeletedWorkloads[dw.IP] = dw
			}
			existing := a.state.Workloads[dw.IP]
			if existing != nil && dw.Version > existing.Version {
				log.Printf("Startup sync: workload %s was deleted while we were down", existing.Name)
				delete(a.state.Workloads, dw.IP)
			}
		}
		// Process workloads
		for _, w := range syncResp.Workloads {
			tombstone := a.state.DeletedWorkloads[w.IP]
			if tombstone != nil && tombstone.Version > w.Version {
				continue
			}
			existing := a.state.Workloads[w.IP]
			if existing == nil || w.Version > existing.Version {
				if existing != nil && existing.Owner == a.hwid && w.Owner != a.hwid {
					log.Printf("Workload %s was revived by %s while we were down", w.Name, w.Owner[:12])
				}
				a.state.Workloads[w.IP] = w
				if tombstone != nil {
					delete(a.state.DeletedWorkloads, w.IP)
				}
			}
		}
		// Merge env data from peer
		for k, v := range syncResp.EnvData {
			a.state.EnvData[k] = v
		}
		a.stateMu.Unlock()
		synced = true
	}

	if synced {
		log.Printf("Startup sync complete")
		a.saveState()
	} else {
		log.Printf("Warning: could not reach any peers for startup sync")
	}
}

func (a *Agent) syncWorkloads() {
	// In tunnel-only mode, sync through tunnel domain
	if a.tunnelDomain != "" {
		a.tunnelModeSyncWorkloads()
		return
	}

	// Direct mode: sync with each peer via mesh IP
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	a.stateMu.RUnlock()

	var lostOwnership []*Workload
	var envUpdated bool

	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/sync", peer.IP, a.apiPort)
		resp, err := httpClient.Get(url)
		if err != nil {
			continue
		}

		var syncResp struct {
			Workloads        []*Workload        `json:"workloads"`
			DeletedWorkloads []*DeletedWorkload `json:"deleted_workloads,omitempty"`
			EnvData          map[string]string  `json:"env_data,omitempty"`
		}
		json.NewDecoder(resp.Body).Decode(&syncResp)
		resp.Body.Close()

		a.stateMu.Lock()
		// Process deleted workloads (tombstones) first
		for _, dw := range syncResp.DeletedWorkloads {
			// Merge tombstone if we don't have it or peer's is newer
			existingTombstone := a.state.DeletedWorkloads[dw.IP]
			if existingTombstone == nil || dw.Version > existingTombstone.Version {
				a.state.DeletedWorkloads[dw.IP] = dw
			}
			// Check if we have a local workload that should be deleted
			existing := a.state.Workloads[dw.IP]
			if existing != nil && dw.Version > existing.Version {
				log.Printf("Sync: removing workload %s (IP %s) - deleted by peer (tombstone version %d > workload version %d)",
					existing.Name, dw.IP, dw.Version, existing.Version)
				if existing.Owner == a.hwid {
					lostOwnership = append(lostOwnership, existing)
				}
				delete(a.state.Workloads, dw.IP)
			}
		}
		// Process workloads - only add/update if not tombstoned with a newer version
		for _, w := range syncResp.Workloads {
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
					log.Printf("Lost ownership of %s (IP %s) to %s - newer version wins", existing.Name, w.IP, w.Owner[:12])
					lostOwnership = append(lostOwnership, existing)
				}
				a.state.Workloads[w.IP] = w
				// Clear any older tombstone since we're accepting a newer workload
				if tombstone != nil {
					delete(a.state.DeletedWorkloads, w.IP)
				}
			}
		}
		// Merge env data from peer (env data is eventually consistent - accept all keys)
		for k, v := range syncResp.EnvData {
			if a.state.EnvData[k] != v {
				a.state.EnvData[k] = v
				envUpdated = true
			}
		}
		a.stateMu.Unlock()
	}

	// Stop workloads we lost ownership of (outside lock)
	for _, wl := range lostOwnership {
		log.Printf("Stopping local workload %s - ownership transferred", wl.Name)
		a.removeWorkload(wl)
	}

	if envUpdated {
		a.saveState()
	}

	a.updateHosts()
}

// tunnelModeSyncWorkloads syncs workloads through the tunnel.
// In tunnel-only mode, we hit the tunnel and get workloads from whichever node responds.
// Since all nodes gossip, they eventually converge on the same state.
func (a *Agent) tunnelModeSyncWorkloads() {
	url := a.getTunnelAPIURL("/api/sync")
	resp, err := httpClient.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var syncResp struct {
		Workloads        []*Workload        `json:"workloads"`
		DeletedWorkloads []*DeletedWorkload `json:"deleted_workloads,omitempty"`
		EnvData          map[string]string  `json:"env_data,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&syncResp); err != nil {
		return
	}

	var lostOwnership []*Workload
	var envUpdated bool

	a.stateMu.Lock()
	// Process deleted workloads (tombstones) first
	for _, dw := range syncResp.DeletedWorkloads {
		// Merge tombstone if we don't have it or peer's is newer
		existingTombstone := a.state.DeletedWorkloads[dw.IP]
		if existingTombstone == nil || dw.Version > existingTombstone.Version {
			a.state.DeletedWorkloads[dw.IP] = dw
		}
		// Check if we have a local workload that should be deleted
		existing := a.state.Workloads[dw.IP]
		if existing != nil && dw.Version > existing.Version {
			log.Printf("Sync: removing workload %s (IP %s) - deleted by peer (tombstone version %d > workload version %d)",
				existing.Name, dw.IP, dw.Version, existing.Version)
			if existing.Owner == a.hwid {
				lostOwnership = append(lostOwnership, existing)
			}
			delete(a.state.Workloads, dw.IP)
		}
	}
	// Process workloads - only add/update if not tombstoned with a newer version
	for _, w := range syncResp.Workloads {
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
				log.Printf("Lost ownership of %s (IP %s) to %s - newer version wins", existing.Name, w.IP, w.Owner[:12])
				lostOwnership = append(lostOwnership, existing)
			}
			a.state.Workloads[w.IP] = w
			// Clear any older tombstone since we're accepting a newer workload
			if tombstone != nil {
				delete(a.state.DeletedWorkloads, w.IP)
			}
		}
	}
	// Merge env data from peer (env data is eventually consistent - accept all keys)
	for k, v := range syncResp.EnvData {
		if a.state.EnvData[k] != v {
			a.state.EnvData[k] = v
			envUpdated = true
		}
	}
	a.stateMu.Unlock()

	// Stop workloads we lost ownership of (outside lock)
	for _, wl := range lostOwnership {
		log.Printf("Stopping local workload %s - ownership transferred", wl.Name)
		a.removeWorkload(wl)
	}

	if envUpdated {
		a.saveState()
	}

	a.updateHosts()
}

func (a *Agent) broadcastState() {
	// In tunnel-only mode, just trigger a sync through the tunnel
	if a.tunnelDomain != "" {
		url := a.getTunnelAPIURL("/api/sync")
		resp, err := httpClient.Get(url)
		if err == nil {
			resp.Body.Close()
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
		httpClient.Get(url) // Trigger sync
	}
}

func (a *Agent) announcePeer(newPeer *Peer) {
	// Include secret in announcement
	announcement := struct {
		Secret string `json:"secret"`
		Peer   *Peer  `json:"peer"`
	}{
		Secret: a.clusterSecret,
		Peer:   newPeer,
	}
	data, _ := json.Marshal(announcement)

	// Track if any direct announcements succeeded
	directSuccess := 0

	// Try direct peer IPs first (if we have peers with IPs)
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		if p.ID != newPeer.ID && p.IP != "" {
			peers = append(peers, p)
		}
	}
	a.stateMu.RUnlock()

	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/peer-announce", peer.IP, a.apiPort)
		resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
		if err != nil {
			log.Printf("Failed to announce peer to %s via direct IP: %v", peer.Name, err)
			continue
		}
		resp.Body.Close()
		directSuccess++
	}

	// Use tunnel as fallback if direct failed or no peers had IPs
	if directSuccess == 0 && a.tunnelDomain != "" {
		url := a.getTunnelAPIURL("/api/peer-announce")
		resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
		if err != nil {
			log.Printf("Failed to announce peer via tunnel: %v", err)
		} else {
			resp.Body.Close()
		}
	}
}

// announceOurIP sends our current IP to all known peers.
// Called after WARP connects so peers can update our address.
func (a *Agent) announceOurIP() {
	if a.ip == "" {
		return
	}

	self := &Peer{
		ID:       a.hwid,
		Name:     a.hostname,
		IP:       a.ip,
		Healthy:  true,
		LastSeen: time.Now(),
	}

	log.Printf("Announcing our IP (%s) to cluster...", a.ip)
	a.announcePeer(self)
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
	a.stateMu.Lock()
	defer a.stateMu.Unlock()

	// Find orphaned workloads
	for _, wl := range a.state.Workloads {
		if !wl.Revive {
			continue
		}

		// Check if owner is dead
		owner := a.state.Peers[wl.Owner]
		if wl.Owner == a.hwid {
			continue // We own it
		}
		if owner != nil && owner.Healthy {
			continue // Owner is alive
		}
		if owner != nil && time.Since(owner.LastSeen) < 45*time.Second {
			continue // Recently seen (3 missed heartbeats + buffer)
		}

		// Owner is dead - should we claim?
		if a.shouldClaim(wl) {
			log.Printf("Claiming orphaned workload: %s", wl.Name)
			oldOwner := wl.Owner
			oldVersion := wl.Version
			wl.Owner = a.hwid
			wl.Version = time.Now().Unix()

			// Deploy it - capture workload state for rollback on failure
			go func(w *Workload, prevOwner string, prevVersion int64) {
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
				a.broadcastState()
			}(wl, oldOwner, oldVersion)
		}
	}
}

// shouldClaim determines if this node should claim an orphaned workload.
// Considers both allowed_nodes and architecture compatibility.
// NOTE: Caller must already hold stateMu lock.
func (a *Agent) shouldClaim(wl *Workload) bool {
	// First check if this node is even allowed to run the workload
	// (this also checks architecture compatibility via runtime.GOARCH)
	if !a.isThisNodeAllowed(wl) {
		return false
	}

	// Deterministic: lowest healthy node ID that is allowed wins
	var candidates []string

	// Add ourselves if allowed (we already checked above)
	candidates = append(candidates, a.hwid)

	// Add healthy peers that are allowed (and have compatible architecture)
	for _, p := range a.state.Peers {
		if p.Healthy && a.isNodeAllowed(wl, p.ID, p.Name, p.Arch) {
			candidates = append(candidates, p.ID)
		}
	}

	sort.Strings(candidates)
	return len(candidates) > 0 && candidates[0] == a.hwid
}

// =============================================================================
// Encryption
// =============================================================================

// deriveKey derives a 32-byte AES key from the cluster secret using SHA-256.
func (a *Agent) deriveKey() []byte {
	hash := sha256.Sum256([]byte(a.clusterSecret))
	return hash[:]
}

// encryptValue encrypts a plaintext value using AES-GCM with the cluster secret.
// Returns base64-encoded ciphertext.
func (a *Agent) encryptValue(plaintext string) (string, error) {
	if a.clusterSecret == "" {
		return "", fmt.Errorf("cluster secret not configured")
	}

	key := a.deriveKey()
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create GCM: %w", err)
	}

	// Generate random nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}

	// Encrypt and prepend nonce to ciphertext
	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decryptValue decrypts a base64-encoded ciphertext using AES-GCM with the cluster secret.
// Returns the plaintext value.
func (a *Agent) decryptValue(encrypted string) (string, error) {
	if a.clusterSecret == "" {
		return "", fmt.Errorf("cluster secret not configured")
	}

	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", fmt.Errorf("decode base64: %w", err)
	}

	key := a.deriveKey()
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create GCM: %w", err)
	}

	if len(ciphertext) < gcm.NonceSize() {
		return "", fmt.Errorf("ciphertext too short")
	}

	// Extract nonce and decrypt
	nonce := ciphertext[:gcm.NonceSize()]
	ciphertext = ciphertext[gcm.NonceSize():]

	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt: %w", err)
	}

	return string(plaintext), nil
}

// getDecryptedEnv returns all environment variables decrypted.
// Returns a map of key -> decrypted value.
func (a *Agent) getDecryptedEnv() (map[string]string, error) {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()

	result := make(map[string]string)
	for key, encrypted := range a.state.EnvData {
		value, err := a.decryptValue(encrypted)
		if err != nil {
			return nil, fmt.Errorf("decrypt %s: %w", key, err)
		}
		result[key] = value
	}
	return result, nil
}

// =============================================================================
// State Persistence
// =============================================================================

func (a *Agent) saveState() {
	a.stateMu.RLock()
	data, _ := json.MarshalIndent(a.state, "", "  ")
	a.stateMu.RUnlock()

	// Atomic write: write to temp file then rename
	statePath := filepath.Join(a.dataDir, "state.json")
	tempPath := statePath + ".tmp"

	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		log.Printf("Failed to write state: %v", err)
		return
	}

	if err := os.Rename(tempPath, statePath); err != nil {
		log.Printf("Failed to rename state file: %v", err)
		os.Remove(tempPath) // Clean up temp file
	}
}

func (a *Agent) loadState() {
	data, err := os.ReadFile(filepath.Join(a.dataDir, "state.json"))
	if err != nil {
		return
	}

	a.stateMu.Lock()
	// Preserve env var CF token if set
	envCFToken := a.state.CFToken

	json.Unmarshal(data, a.state)

	// Env var takes precedence over saved state
	if envCFToken != "" && a.state.CFToken == "" {
		a.state.CFToken = envCFToken
	}

	// Initialize EnvData map if nil (for backwards compatibility)
	if a.state.EnvData == nil {
		a.state.EnvData = make(map[string]string)
	}
	// Initialize DeletedWorkloads map if nil (for backwards compatibility)
	if a.state.DeletedWorkloads == nil {
		a.state.DeletedWorkloads = make(map[string]*DeletedWorkload)
	}
	a.stateMu.Unlock()

	log.Printf("Loaded: %d peers, %d workloads, %d env vars", len(a.state.Peers), len(a.state.Workloads), len(a.state.EnvData))
}

// =============================================================================
// Cloudflare Tunnel
// =============================================================================

// cloudflaredLogFilter filters cloudflared output to only log important messages.
// This prevents verbose debug output from flooding the logs while still capturing
// errors, connection status, and other important information.
type cloudflaredLogFilter struct {
	prefix string
}

func (f *cloudflaredLogFilter) Write(p []byte) (n int, err error) {
	line := strings.TrimSpace(string(p))
	if line == "" {
		return len(p), nil
	}

	// Only log important messages: errors, warnings, connection status
	// Filter out verbose debug output (INFO level routine messages)
	if strings.Contains(line, "ERR") ||
		strings.Contains(line, "WRN") ||
		strings.Contains(line, "error") ||
		strings.Contains(line, "failed") ||
		strings.Contains(line, "Registered") ||
		strings.Contains(line, "Unregistered") ||
		strings.Contains(line, "connected") ||
		strings.Contains(line, "Starting tunnel") {
		log.Printf("[%s] %s", f.prefix, line)
	}

	return len(p), nil
}

// startCloudflared starts the cloudflared tunnel process with the configured token.
// It runs the tunnel pointing to the local API, providing external access to the cluster.
// The process is monitored and automatically restarted on failure.
func (a *Agent) startCloudflared() error {
	a.cfMu.Lock()
	defer a.cfMu.Unlock()

	// Check if already running
	if a.cfCmd != nil && a.cfCmd.Process != nil {
		return nil
	}

	a.stateMu.RLock()
	token := a.state.CFToken
	a.stateMu.RUnlock()

	if token == "" {
		return nil // No token configured
	}

	a.cfStopCh = make(chan struct{})

	// Start cloudflared tunnel with --no-autoupdate to prevent background updates
	// Note: --no-autoupdate is a global flag and must come before 'tunnel'
	// Pass token via --token flag (more reliable than TUNNEL_TOKEN env var)
	a.cfCmd = exec.Command("cloudflared", "--no-autoupdate", "tunnel", "run", "--token", token)

	// Use filtered log writer to capture important messages while suppressing verbose output
	logFilter := &cloudflaredLogFilter{prefix: "cloudflared"}
	a.cfCmd.Stdout = logFilter
	a.cfCmd.Stderr = logFilter

	if err := a.cfCmd.Start(); err != nil {
		return fmt.Errorf("cloudflared start: %w", err)
	}

	log.Printf("Cloudflare tunnel started (pid: %d)", a.cfCmd.Process.Pid)

	// Monitor process and restart on failure
	go a.monitorCloudflared()

	return nil
}

// stopCloudflared gracefully stops the cloudflared tunnel process.
func (a *Agent) stopCloudflared() {
	a.cfMu.Lock()
	defer a.cfMu.Unlock()

	if a.cfStopCh != nil {
		close(a.cfStopCh)
		a.cfStopCh = nil
	}

	if a.cfCmd != nil && a.cfCmd.Process != nil {
		a.cfCmd.Process.Signal(os.Interrupt)
		// Give it 5 seconds to shutdown gracefully
		done := make(chan struct{})
		go func() {
			a.cfCmd.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			a.cfCmd.Process.Kill()
		}
		log.Printf("Cloudflare tunnel stopped")
	}
	a.cfCmd = nil
}

// monitorCloudflared watches the cloudflared process and restarts it if it dies.
// Uses exponential backoff with a maximum of 10 consecutive failures before giving up.
func (a *Agent) monitorCloudflared() {
	const (
		initialBackoff  = 5 * time.Second
		maxBackoff      = 2 * time.Minute
		maxFailures     = 10
		successReset    = 30 * time.Second // Reset failure count if running for this long
	)

	backoff := initialBackoff
	failures := 0

	for {
		a.cfMu.Lock()
		cmd := a.cfCmd
		stopCh := a.cfStopCh
		a.cfMu.Unlock()

		if cmd == nil {
			return
		}

		startTime := time.Now()

		// Wait for process to exit
		err := cmd.Wait()

		// Check if we were asked to stop
		select {
		case <-stopCh:
			return
		default:
		}

		// If process ran for a while, reset the failure counter and backoff
		if time.Since(startTime) >= successReset {
			failures = 0
			backoff = initialBackoff
		} else {
			failures++
		}

		// Check if we've exceeded max failures
		if failures >= maxFailures {
			log.Printf("Cloudflare tunnel failed %d times consecutively, giving up. Check your JETTY_CF_TOKEN.", failures)
			return
		}

		if err != nil {
			log.Printf("Cloudflare tunnel exited: %v (attempt %d/%d), restarting in %v...", err, failures, maxFailures, backoff)
		} else {
			log.Printf("Cloudflare tunnel exited (attempt %d/%d), restarting in %v...", failures, maxFailures, backoff)
		}

		time.Sleep(backoff)

		// Exponential backoff: double the wait time for next failure, up to max
		backoff = backoff * 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}

		// Check again if we should stop
		select {
		case <-stopCh:
			return
		default:
		}

		// Restart
		a.cfMu.Lock()
		a.stateMu.RLock()
		token := a.state.CFToken
		a.stateMu.RUnlock()

		if token == "" {
			a.cfMu.Unlock()
			return
		}

		// Note: --no-autoupdate is a global flag and must come before 'tunnel'
		// Pass token via --token flag (more reliable than TUNNEL_TOKEN env var)
		a.cfCmd = exec.Command("cloudflared", "--no-autoupdate", "tunnel", "run", "--token", token)

		// Use filtered log writer to capture important messages while suppressing verbose output
		logFilter := &cloudflaredLogFilter{prefix: "cloudflared"}
		a.cfCmd.Stdout = logFilter
		a.cfCmd.Stderr = logFilter

		if err := a.cfCmd.Start(); err != nil {
			log.Printf("Cloudflare tunnel restart failed: %v", err)
			a.cfMu.Unlock()
			return
		}
		log.Printf("Cloudflare tunnel restarted (pid: %d)", a.cfCmd.Process.Pid)
		a.cfMu.Unlock()
	}
}

// restartCloudflared stops and starts the tunnel (used when token changes).
func (a *Agent) restartCloudflared() error {
	a.stopCloudflared()
	return a.startCloudflared()
}

// isTunnelRunning returns true if cloudflared is currently running.
func (a *Agent) isTunnelRunning() bool {
	a.cfMu.Lock()
	defer a.cfMu.Unlock()
	return a.cfCmd != nil && a.cfCmd.Process != nil
}

// =============================================================================
// Helpers
// =============================================================================

// getPeerAPIURL returns the URL to reach a peer's API via WARP.
func (a *Agent) getPeerAPIURL(peer *Peer, path string) string {
	// Use WARP IP for direct node-to-node communication
	if peer.IP != "" {
		return fmt.Sprintf("http://%s:%d%s", peer.IP, a.apiPort, path)
	}
	// Fall back to tunnel domain if peer IP unknown (WARP not yet connected)
	if a.tunnelDomain != "" {
		return fmt.Sprintf("https://%s%s", a.tunnelDomain, path)
	}
	return ""
}

// peerRequest creates an HTTP request to a peer with auth headers set.
func (a *Agent) peerRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	if a.clusterSecret != "" {
		req.Header.Set("X-API-Key", a.clusterSecret)
	}
	if method == "POST" || method == "PUT" || method == "PATCH" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

// getTunnelAPIURL returns the Cloudflare tunnel URL for API calls.
// Returns empty string if not in tunnel-only mode.
func (a *Agent) getTunnelAPIURL(path string) string {
	if a.tunnelDomain == "" {
		return ""
	}
	return fmt.Sprintf("https://%s%s", a.tunnelDomain, path)
}

func getEnv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func getEnvInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			log.Printf("Warning: invalid integer value for %s: %q, using default %d", k, v, d)
			return d
		}
		return n
	}
	return d
}

func getHostname() string {
	h, _ := os.Hostname()
	if h == "" {
		return "node"
	}
	return h
}

func getPublicIP() string {
	// Allow override via environment variable (useful in containers)
	if ip := os.Getenv("JETTY_PUBLIC_IP"); ip != "" {
		return ip
	}

	// Try external services to get actual public IP (with short timeout)
	client := &http.Client{Timeout: 3 * time.Second}
	services := []string{
		"https://api.ipify.org",
		"https://ifconfig.me/ip",
		"https://icanhazip.com",
	}

	for _, svc := range services {
		resp, err := client.Get(svc)
		if err != nil {
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}
		ip := strings.TrimSpace(string(body))
		// Validate it looks like an IP
		if parsed := net.ParseIP(ip); parsed != nil {
			return ip
		}
	}

	// Fallback: get outbound IP (local IP used to reach internet)
	dialer := net.Dialer{Timeout: 2 * time.Second}
	c, err := dialer.Dial("udp", "8.8.8.8:80")
	if err == nil {
		defer c.Close()
		if addr, ok := c.LocalAddr().(*net.UDPAddr); ok && !addr.IP.IsLoopback() {
			return addr.IP.String()
		}
	}

	// Last resort: find first non-loopback interface IP
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}

	return "127.0.0.1"
}

func genID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
