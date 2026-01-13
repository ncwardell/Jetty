package agent

import (
	"crypto/rand"
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/ncwardell/jetty/docs"
	httpSwagger "github.com/swaggo/http-swagger"
)

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

// =============================================================================
// Types
// =============================================================================

type Peer struct {
	ID       string    `json:"id"`        // HWID
	Name     string    `json:"name"`      // Hostname
	IP       string    `json:"ip"`        // WARP IP (100.96.x.x) - primary address for node communication
	Healthy  bool      `json:"healthy"`
	LastSeen time.Time `json:"last_seen"`
}

type Workload struct {
	Name         string   `json:"name"`                    // DNS hostname
	IP           string   `json:"ip"`                      // Service IP (routed via WARP)
	Compose      string   `json:"compose"`
	Revive       bool     `json:"revive"`                  // Auto-failover to another node if owner dies
	Autostart    bool     `json:"autostart"`               // Auto-start when Jetty starts up
	AllowedNodes []string `json:"allowed_nodes,omitempty"` // Node whitelist: empty/["*"] = all, otherwise node names/IDs
	Owner        string   `json:"owner"`                   // Node HWID
	Version      int64    `json:"version"`                 // Unix timestamp
}

type State struct {
	Peers     map[string]*Peer     `json:"peers"`     // ID -> Peer
	Workloads map[string]*Workload `json:"workloads"` // IP -> Workload
	CFToken   string               `json:"cf_token,omitempty"`   // Cloudflare tunnel token (shared cluster-wide)
	WarpToken string               `json:"warp_token,omitempty"` // Cloudflare WARP connector token (shared cluster-wide)
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

	stopCh chan struct{}
}

func New() (*Agent, error) {
	dataDir := getEnv("JETTY_DATA_DIR", "/data")
	os.MkdirAll(dataDir, 0755)

	a := &Agent{
		hostname:      getHostname(),
		dataDir:       dataDir,
		apiPort:       getEnvInt("JETTY_API_PORT", 6880),
		serviceCIDR:   getEnv("JETTY_SERVICE_CIDR", "10.100.0.0/16"), // CIDR for workload IPs
		joinURL:       getEnv("JETTY_JOIN", ""),
		clusterSecret: getEnv("JETTY_SECRET", ""),
		tunnelDomain:  getEnv("JETTY_TUNNEL_DOMAIN", ""),            // e.g., "cluster.example.com" - Cloudflare tunnel for API access
		tunnelHost:    getEnv("JETTY_TUNNEL_HOST", ""),              // e.g., "node1.cluster.example.com" - this node's specific subdomain
		cfTunnelID:    getEnv("JETTY_CF_TUNNEL_ID", ""),             // WARP connector tunnel ID for route management
		composeDir:    filepath.Join(dataDir, "compose"),
		hostsFile:     "/etc/hosts",
		state: &State{
			Peers:     make(map[string]*Peer),
			Workloads: make(map[string]*Workload),
			CFToken:   getEnv("JETTY_CF_TOKEN", ""),             // Bootstrap tunnel token
			WarpToken: getEnv("JETTY_WARP_CONNECTOR_TOKEN", ""), // Bootstrap WARP connector token
		},
		stopCh: make(chan struct{}),
	}

	os.MkdirAll(a.composeDir, 0755)

	// Load or generate HWID
	a.hwid = a.loadOrCreateHWID()

	return a, nil
}

func (a *Agent) Start() error {
	// Record start time for uptime tracking
	a.startTime = time.Now()

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

	// Auto-start owned workloads (only those we still own after sync)
	a.autostartWorkloads()

	// Update hosts file
	a.updateHosts()

	// Start Cloudflare tunnel if configured
	if err := a.startCloudflared(); err != nil {
		log.Printf("Warning: failed to start cloudflared: %v", err)
	}

	// Start API
	go a.runAPI()

	// Start gossip
	go a.gossipLoop()

	// Start failover monitor
	go a.failoverLoop()

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

		// Start warp-svc in background
		cmd := exec.Command("warp-svc", "--accept-tos")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
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

// =============================================================================
// WARP Private Network Routes (via cloudflared CLI)
// =============================================================================

// registerWarpRoute adds a private network route for a mesh IP using cloudflared CLI.
// This allows WARP clients to reach the workload through the tunnel.
func (a *Agent) registerWarpRoute(meshIP string) error {
	if a.cfTunnelID == "" {
		return nil // WARP route management not configured
	}

	// Use cloudflared tunnel route command
	cmd := exec.Command("cloudflared", "tunnel", "route", "ip", "add", meshIP+"/32", a.cfTunnelID)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Ignore "already exists" errors
		if strings.Contains(string(output), "already exists") {
			log.Printf("WARP route for %s already exists", meshIP)
			return nil
		}
		return fmt.Errorf("cloudflared route add: %s", output)
	}

	log.Printf("WARP route registered: %s -> tunnel %s", meshIP, a.cfTunnelID[:8])
	return nil
}

// unregisterWarpRoute removes a private network route for a mesh IP.
func (a *Agent) unregisterWarpRoute(meshIP string) error {
	if a.cfTunnelID == "" {
		return nil // WARP route management not configured
	}

	cmd := exec.Command("cloudflared", "tunnel", "route", "ip", "delete", meshIP+"/32")
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Ignore "not found" errors
		if strings.Contains(string(output), "not found") {
			return nil
		}
		return fmt.Errorf("cloudflared route delete: %s", output)
	}

	log.Printf("WARP route unregistered: %s", meshIP)
	return nil
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

	// Clean up iptables rules for all workloads
	a.stateMu.RLock()
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

	// Remove jetty0 interface (this also removes all IPs bound to it)
	if err := exec.Command("ip", "link", "del", "jetty0").Run(); err != nil {
		log.Printf("Warning: could not remove jetty0 interface: %v", err)
	} else {
		log.Printf("Removed jetty0 interface")
	}

	// Unregister WARP device from Cloudflare to prevent orphaned devices
	log.Printf("Unregistering WARP device from Cloudflare...")
	exec.Command("warp-cli", "--accept-tos", "disconnect").Run()
	exec.Command("warp-cli", "--accept-tos", "registration", "delete").Run()

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

	log.Printf("Network ready: %s (WARP), workload interface: jetty0", a.ip)
	return nil
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
// Empty or ["*"] allowed_nodes means all nodes are allowed.
func (a *Agent) isNodeAllowed(wl *Workload, nodeID, nodeName string) bool {
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
	return a.isNodeAllowed(wl, a.hwid, a.hostname)
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
// Returns nil if no suitable node is found.
// Caller must hold stateMu lock (read).
func (a *Agent) findAllowedNode(wl *Workload) *Peer {
	for _, p := range a.state.Peers {
		if p.Healthy && a.isNodeAllowed(wl, p.ID, p.Name) {
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
		"secret": a.clusterSecret, // Cluster secret for authentication
		"id":     a.hwid,
		"name":   a.hostname,
		"ip":     a.ip, // WARP IP (may be empty, set after WARP connect)
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
		Peers        []*Peer     `json:"peers"`
		Workloads    []*Workload `json:"workloads"`
		CFToken      string      `json:"cf_token,omitempty"`
		WarpToken    string      `json:"warp_token,omitempty"`
		ServiceCIDR  string      `json:"service_cidr,omitempty"`
		TunnelDomain string      `json:"tunnel_domain,omitempty"`
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
	r.HandleFunc("/api/health", a.apiHealth).Methods("GET")
	r.HandleFunc("/api/sync", a.apiSync).Methods("GET")
	r.HandleFunc("/api/tunnel", a.apiGetTunnel).Methods("GET")
	r.HandleFunc("/api/tunnel", a.apiSetTunnel).Methods("POST")
	r.HandleFunc("/api/tunnel", a.apiDeleteTunnel).Methods("DELETE")
	r.HandleFunc("/api/tunnel/sync", a.apiTunnelSync).Methods("POST")
	r.HandleFunc("/api/peer-announce", a.apiPeerAnnounce).Methods("POST")
	r.HandleFunc("/api/heartbeat", a.apiHeartbeat).Methods("POST")
	r.PathPrefix("/api/proxy/").HandlerFunc(a.apiWorkloadProxy)

	// Swagger UI
	r.PathPrefix("/swagger/").Handler(httpSwagger.WrapHandler)

	// Wrap router with API key middleware
	handler := a.apiKeyMiddleware(r)

	addr := fmt.Sprintf(":%d", a.apiPort)
	log.Printf("API on %s (auth=%v)", addr, a.clusterSecret != "")
	http.ListenAndServe(addr, handler)
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
	workloads := make([]*Workload, 0, len(a.state.Workloads))
	for _, wl := range a.state.Workloads {
		workloads = append(workloads, wl)
	}
	hasTunnel := a.state.CFToken != ""
	a.stateMu.RUnlock()

	resp := map[string]interface{}{
		"node": map[string]interface{}{
			"id":   a.hwid,
			"name": a.hostname,
			"ip":   a.ip,
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
			IP:       wl.IP,
			Compose:      wl.Compose,
			Revive:       wl.Revive,
			Autostart:    wl.Autostart,
			AllowedNodes: wl.AllowedNodes,
			Owner:        ownerInfo,
			Version:      wl.Version,
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

	if wl.Name == "" || wl.Compose == "" {
		http.Error(w, "name and compose required", 400)
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
		resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
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

	// Register WARP route so workload is reachable via WARP
	if err := a.registerWarpRoute(wl.IP); err != nil {
		log.Printf("Warning: failed to register WARP route for %s: %v", wl.IP, err)
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
		resp, err := httpClient.Get(url)
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
		req, _ := http.NewRequest("PATCH", url, strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
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

	// Handle compose change
	if update.Compose != nil && *update.Compose != found.Compose {
		found.Compose = *update.Compose
		needsRedeploy = true
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

		// Unregister old WARP route
		if foundIP != newMeshIP {
			a.unregisterWarpRoute(foundIP)
		}

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
			req, err := http.NewRequest("DELETE", url, nil)
			if err != nil {
				http.Error(w, fmt.Sprintf("failed to create request: %v", err), 500)
				return
			}
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

	// Local workload or orphaned cleanup - delete from state
	a.stateMu.Lock()
	delete(a.state.Workloads, foundIP)
	a.stateMu.Unlock()

	// Remove if we're running it
	if found.Owner == a.hwid {
		a.removeWorkload(found)

		// Unregister WARP route for this workload
		if err := a.unregisterWarpRoute(found.IP); err != nil {
			log.Printf("Warning: failed to unregister WARP route for %s: %v", found.IP, err)
		}
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

	// Find target
	var target *Peer
	for _, p := range a.state.Peers {
		if p.Name == req.To || p.ID == req.To {
			target = p
			break
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

	// Check if target is allowed to run this workload
	if !a.isNodeAllowed(found, target.ID, target.Name) {
		http.Error(w, "target node is not in allowed_nodes for this workload", 403)
		return
	}

	// Blue-green deployment: deploy on target first (with move=true to allow IP overlap)
	data, _ := json.Marshal(found)
	url := a.getPeerAPIURL(target, "/api/workloads?move=true")
	resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to deploy on target: %v", err), 502)
		return
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		http.Error(w, fmt.Sprintf("target rejected deployment (status %d)", resp.StatusCode), 500)
		return
	}

	// Target is now running - remove from source
	// If we own it, remove locally
	if found.Owner == a.hwid {
		a.removeWorkload(found)

		// Unregister WARP route (target has registered its own)
		if err := a.unregisterWarpRoute(found.IP); err != nil {
			log.Printf("Warning: failed to unregister WARP route for %s: %v", found.IP, err)
		}
	} else if currentOwner != nil && currentOwner.Healthy {
		// Proxy delete to current owner
		deleteURL := a.getPeerAPIURL(currentOwner, "/api/workloads/"+name)
		delReq, _ := http.NewRequest("DELETE", deleteURL, nil)
		delResp, err := httpClient.Do(delReq)
		if err != nil {
			log.Printf("Warning: failed to remove workload from original owner: %v", err)
		} else {
			delResp.Body.Close()
		}
	}

	log.Printf("Moved workload %s to %s (blue-green)", found.Name, target.Name)
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
		resp, err := httpClient.Get(url)
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
		resp, err := httpClient.Post(url, "application/json", nil)
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
		resp, err := httpClient.Post(url, "application/json", nil)
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
		Secret string `json:"secret"` // Cluster secret for authentication
		ID     string `json:"id"`
		Name   string `json:"name"`
		IP     string `json:"ip"`
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
	}

	a.stateMu.Lock()
	a.state.Peers[peer.ID] = peer

	// Build response with all peers (including self)
	allPeers := []*Peer{{
		ID:      a.hwid,
		Name:    a.hostname,
		IP:      a.ip,
		Healthy: true,
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
	a.stateMu.Unlock()

	a.updateHosts()
	a.saveState()

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

// apiHealth godoc
// @Summary Health check
// @Description Returns health status of this node
// @Tags cluster
// @Produce json
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
		a.stateMu.RLock()
		var localWorkloads []string
		for _, wl := range a.state.Workloads {
			if wl.Owner == a.hwid {
				out, _ := exec.Command("docker", "ps", "-q", "-f", "label=com.docker.compose.project=jetty_"+wl.Name).Output()
				status := "stopped"
				if len(strings.TrimSpace(string(out))) > 0 {
					status = "running"
				}
				localWorkloads = append(localWorkloads, fmt.Sprintf("%s:%s:%s", wl.Name, wl.IP, status))
			}
		}
		a.stateMu.RUnlock()

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
			health.Status = fmt.Sprintf("%v", peerHealth["status"])
			if pubIP, ok := peerHealth["public_ip"].(string); ok {
				health.PublicIP = pubIP
			}
			if nodes, ok := peerHealth["nodes"].([]interface{}); ok && len(nodes) > 0 {
				// Peer returned cluster format, extract first node
				if node, ok := nodes[0].(map[string]interface{}); ok {
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

	// Get CPU usage from /proc/stat (calculate over a brief interval)
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
						if i == 4 { // idle is 4th field (index 4)
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
	time.Sleep(100 * time.Millisecond) // Brief sample period
	idle2, total2 := getCPUTimes()

	if total2 > total1 {
		idleDelta := float64(idle2 - idle1)
		totalDelta := float64(total2 - total1)
		cpuPercent := (1.0 - idleDelta/totalDelta) * 100
		stats["cpu_percent"] = fmt.Sprintf("%.1f%%", cpuPercent)
	}

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
	// Return all workloads for sync (not just local ones)
	// This allows other nodes to get a complete view of cluster state
	a.stateMu.RLock()
	workloads := make([]*Workload, 0, len(a.state.Workloads))
	for _, wl := range a.state.Workloads {
		workloads = append(workloads, wl)
	}
	a.stateMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(workloads)
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

	req.Peer.Healthy = true
	req.Peer.LastSeen = time.Now()

	a.stateMu.Lock()
	a.state.Peers[req.Peer.ID] = &req.Peer
	a.stateMu.Unlock()

	a.updateHosts()
	a.saveState()

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
		} else {
			// Register WARP route for successfully started workload
			if err := a.registerWarpRoute(wl.IP); err != nil {
				log.Printf("Warning: failed to register WARP route for %s: %v", wl.IP, err)
			}
		}
	}
}

func (a *Agent) deployWorkload(wl *Workload) error {
	dir := filepath.Join(a.composeDir, wl.Name)
	os.MkdirAll(dir, 0755)

	// Write compose
	path := filepath.Join(dir, "docker-compose.yml")
	if err := os.WriteFile(path, []byte(wl.Compose), 0644); err != nil {
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
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// =============================================================================
// Gossip
// =============================================================================

func (a *Agent) gossipLoop() {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-a.stopCh:
			return
		case <-tick.C:
			a.checkPeers()
			a.syncWorkloads()
		}
	}
}

func (a *Agent) checkPeers() {
	// In tunnel-only mode, send heartbeat through tunnel and check peer staleness
	if a.tunnelDomain != "" {
		a.tunnelModeHealthCheck()
		return
	}

	// Direct mode: check each peer individually via mesh IP
	a.stateMu.Lock()
	defer a.stateMu.Unlock()

	for _, peer := range a.state.Peers {
		url := fmt.Sprintf("http://%s:%d/api/health", peer.IP, a.apiPort)
		resp, err := httpClient.Get(url)

		if err != nil {
			peer.Healthy = false
			continue
		}
		resp.Body.Close()

		peer.Healthy = resp.StatusCode == 200
		if peer.Healthy {
			peer.LastSeen = time.Now()
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
	defer a.stateMu.Unlock()

	staleThreshold := 45 * time.Second // 3 missed heartbeats (15s interval) + buffer
	now := time.Now()

	for _, peer := range a.state.Peers {
		if now.Sub(peer.LastSeen) > staleThreshold {
			if peer.Healthy {
				log.Printf("Peer %s marked unhealthy (no heartbeat for %v)", peer.Name, now.Sub(peer.LastSeen))
			}
			peer.Healthy = false
		} else {
			peer.Healthy = true
		}
	}
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
			var workloads []*Workload
			json.NewDecoder(resp.Body).Decode(&workloads)
			resp.Body.Close()

			a.stateMu.Lock()
			for _, w := range workloads {
				existing := a.state.Workloads[w.IP]
				if existing == nil || w.Version > existing.Version {
					if existing != nil && existing.Owner == a.hwid && w.Owner != a.hwid {
						log.Printf("Workload %s was revived by %s while we were down", w.Name, w.Owner[:12])
					}
					a.state.Workloads[w.IP] = w
				}
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

		var workloads []*Workload
		json.NewDecoder(resp.Body).Decode(&workloads)
		resp.Body.Close()

		a.stateMu.Lock()
		for _, w := range workloads {
			existing := a.state.Workloads[w.IP]
			if existing == nil || w.Version > existing.Version {
				if existing != nil && existing.Owner == a.hwid && w.Owner != a.hwid {
					log.Printf("Workload %s was revived by %s while we were down", w.Name, w.Owner[:12])
				}
				a.state.Workloads[w.IP] = w
			}
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

	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/sync", peer.IP, a.apiPort)
		resp, err := httpClient.Get(url)
		if err != nil {
			continue
		}

		var workloads []*Workload
		json.NewDecoder(resp.Body).Decode(&workloads)
		resp.Body.Close()

		a.stateMu.Lock()
		for _, w := range workloads {
			existing := a.state.Workloads[w.IP]
			if existing == nil || w.Version > existing.Version {
				// Check if we lost ownership (IP collision resolution)
				if existing != nil && existing.Owner == a.hwid && w.Owner != a.hwid {
					log.Printf("Lost ownership of %s (IP %s) to %s - newer version wins", existing.Name, w.IP, w.Owner[:12])
					lostOwnership = append(lostOwnership, existing)
				}
				a.state.Workloads[w.IP] = w
			}
		}
		a.stateMu.Unlock()
	}

	// Stop workloads we lost ownership of (outside lock)
	for _, wl := range lostOwnership {
		log.Printf("Stopping local workload %s - ownership transferred", wl.Name)
		a.removeWorkload(wl)
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

	var workloads []*Workload
	if err := json.NewDecoder(resp.Body).Decode(&workloads); err != nil {
		return
	}

	var lostOwnership []*Workload

	a.stateMu.Lock()
	for _, w := range workloads {
		existing := a.state.Workloads[w.IP]
		if existing == nil || w.Version > existing.Version {
			// Check if we lost ownership (IP collision resolution)
			if existing != nil && existing.Owner == a.hwid && w.Owner != a.hwid {
				log.Printf("Lost ownership of %s (IP %s) to %s - newer version wins", existing.Name, w.IP, w.Owner[:12])
				lostOwnership = append(lostOwnership, existing)
			}
			a.state.Workloads[w.IP] = w
		}
	}
	a.stateMu.Unlock()

	// Stop workloads we lost ownership of (outside lock)
	for _, wl := range lostOwnership {
		log.Printf("Stopping local workload %s - ownership transferred", wl.Name)
		a.removeWorkload(wl)
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

	// In tunnel-only mode, broadcast to tunnel
	if a.tunnelDomain != "" {
		url := a.getTunnelAPIURL("/api/peer-announce")
		resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
		if err != nil {
			log.Printf("Failed to announce peer: %v", err)
		} else {
			resp.Body.Close()
		}
		return
	}

	// Direct mode: send to each peer
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		if p.ID != newPeer.ID {
			peers = append(peers, p)
		}
	}
	a.stateMu.RUnlock()

	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/peer-announce", peer.IP, a.apiPort)
		resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
		if err != nil {
			log.Printf("Failed to announce peer to %s: %v", peer.Name, err)
			continue
		}
		resp.Body.Close()
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
				// Register WARP route so workload is reachable via WARP
				if err := a.registerWarpRoute(w.IP); err != nil {
					log.Printf("Warning: failed to register WARP route for failover workload %s: %v", w.IP, err)
				}
				a.updateHosts()
				a.saveState()
				a.broadcastState()
			}(wl, oldOwner, oldVersion)
		}
	}
}

// shouldClaim determines if this node should claim an orphaned workload.
// NOTE: Caller must already hold stateMu lock.
func (a *Agent) shouldClaim(wl *Workload) bool {
	// First check if this node is even allowed to run the workload
	if !a.isThisNodeAllowed(wl) {
		return false
	}

	// Deterministic: lowest healthy node ID that is allowed wins
	var candidates []string

	// Add ourselves if allowed (we already checked above)
	candidates = append(candidates, a.hwid)

	// Add healthy peers that are allowed
	for _, p := range a.state.Peers {
		if p.Healthy && a.isNodeAllowed(wl, p.ID, p.Name) {
			candidates = append(candidates, p.ID)
		}
	}

	sort.Strings(candidates)
	return len(candidates) > 0 && candidates[0] == a.hwid
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
	a.stateMu.Unlock()

	log.Printf("Loaded: %d peers, %d workloads", len(a.state.Peers), len(a.state.Workloads))
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
		log.Printf("[cloudflared] %s", line)
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

	// Try to determine IP by dialing out (with timeout to prevent hangs)
	dialer := net.Dialer{Timeout: 2 * time.Second}
	c, err := dialer.Dial("udp", "8.8.8.8:80")
	if err == nil {
		defer c.Close()
		if addr, ok := c.LocalAddr().(*net.UDPAddr); ok && !addr.IP.IsLoopback() {
			return addr.IP.String()
		}
	}

	// Fallback: find first non-loopback interface IP
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
