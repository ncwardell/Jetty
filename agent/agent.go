package agent

import (
	"crypto/rand"
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	httpSwagger "github.com/swaggo/http-swagger"
	"golang.org/x/crypto/curve25519"
)

// Shared HTTP client with timeout to prevent blocking on hung peers
var httpClient = &http.Client{
	Timeout: 10 * time.Second,
}

// WebSocket upgrader for WireGuard tunnel
var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Valid workload name pattern (alphanumeric, dash, underscore only)
var validNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// WGPacket represents a WireGuard UDP packet sent through the WebSocket tunnel
type WGPacket struct {
	FromID string `json:"from"` // Sender's HWID
	ToID   string `json:"to"`   // Target peer's HWID
	Data   []byte `json:"data"` // Raw UDP payload (WireGuard encrypted)
}

// =============================================================================
// Types
// =============================================================================

type Peer struct {
	ID         string    `json:"id"`          // HWID
	Name       string    `json:"name"`        // Hostname
	MeshIP     string    `json:"mesh_ip"`
	Endpoint   string    `json:"endpoint"`    // public:wg_port
	PublicKey  string    `json:"public_key"`
	TunnelHost string    `json:"tunnel_host"` // Peer-specific tunnel hostname (e.g., "node1.cluster.example.com")
	WarpIP     string    `json:"warp_ip"`     // Cloudflare WARP IP (CGNAT range, e.g., "100.96.x.x")
	Healthy    bool      `json:"healthy"`
	LastSeen   time.Time `json:"last_seen"`
}

type Workload struct {
	Name      string `json:"name"`      // DNS hostname
	MeshIP    string `json:"mesh_ip"`   // Unique lock
	Compose   string `json:"compose"`
	Revive    bool   `json:"revive"`    // Auto-failover to another node if owner dies
	Autostart bool   `json:"autostart"` // Auto-start when Jetty starts up
	Owner     string `json:"owner"`     // Node HWID
	Version   int64  `json:"version"`   // Unix timestamp
}

type Token struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type State struct {
	Peers     map[string]*Peer     `json:"peers"`     // ID -> Peer
	Workloads map[string]*Workload `json:"workloads"` // MeshIP -> Workload
	Tokens    map[string]*Token    `json:"tokens"`
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
	meshIP   string

	// WireGuard
	wgPrivKey  string
	wgPubKey   string
	wgPort     int
	wgDisabled bool // True if using dummy interface (no WireGuard kernel module)

	// Cloudflare WARP
	warpIP      string // WARP IP address (CGNAT range, e.g., "100.96.x.x")
	warpEnabled bool   // Whether WARP mode is active

	// Cloudflare tunnel (for WARP private network routes)
	cfTunnelID string // WARP connector tunnel ID (for route management)

	// Config
	dataDir       string
	apiPort       int
	meshCIDR      string
	joinURL       string
	joinTok       string
	clusterSecret string // Shared secret all nodes must have to join
	tunnelDomain  string // Cloudflare tunnel domain for tunnel-only mode (no WG port forwarding needed)
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
	startTime time.Time // When Jetty started (for uptime tracking)

	// WebSocket WireGuard tunnel (for tunnel-only mode)
	wgRelays     map[string]*udpRelay // peerID -> local UDP relay
	wgRelaysMu   sync.RWMutex
	wgLocalConn  *net.UDPConn // Local UDP socket to inject packets into WireGuard
	wgRelayBase  int          // Base port for UDP relays (51821, 51822, etc.)

	stopCh chan struct{}
}

// udpRelay captures UDP packets destined for a peer and forwards via WebSocket
type udpRelay struct {
	peerID   string
	conn     *net.UDPConn
	localPort int
	stopCh   chan struct{}
}

func New() (*Agent, error) {
	dataDir := getEnv("JETTY_DATA_DIR", "/data")
	os.MkdirAll(dataDir, 0755)

	a := &Agent{
		hostname:      getHostname(),
		wgPort:        getEnvInt("JETTY_WG_PORT", 51820),
		dataDir:       dataDir,
		apiPort:       getEnvInt("JETTY_API_PORT", 8080),
		meshCIDR:      getEnv("JETTY_MESH_CIDR", "10.100.0.0/16"),
		joinURL:       getEnv("JETTY_JOIN", ""),
		joinTok:       getEnv("JETTY_TOKEN", ""),
		clusterSecret: getEnv("JETTY_SECRET", ""),
		tunnelDomain: getEnv("JETTY_TUNNEL_DOMAIN", ""), // e.g., "cluster.example.com" - enables tunnel-only mode
		tunnelHost:   getEnv("JETTY_TUNNEL_HOST", ""),   // e.g., "node1.cluster.example.com" - this node's specific subdomain
		cfTunnelID:   getEnv("JETTY_CF_TUNNEL_ID", ""),  // WARP connector tunnel ID for route management
		composeDir:    filepath.Join(dataDir, "compose"),
		hostsFile:     "/etc/hosts",
		state: &State{
			Peers:     make(map[string]*Peer),
			Workloads: make(map[string]*Workload),
			Tokens:    make(map[string]*Token),
			CFToken:   getEnv("JETTY_CF_TOKEN", ""),            // Bootstrap tunnel token
			WarpToken: getEnv("JETTY_WARP_CONNECTOR_TOKEN", ""), // Bootstrap WARP connector token
		},
		wgRelays:    make(map[string]*udpRelay),
		wgRelayBase: 51821, // Start relay ports from 51821
		stopCh:      make(chan struct{}),
	}

	os.MkdirAll(a.composeDir, 0755)

	// Load or generate HWID
	a.hwid = a.loadOrCreateHWID()

	return a, nil
}

func (a *Agent) Start() error {
	// Record start time for uptime tracking
	a.startTime = time.Now()

	// Detect WARP IP if WARP is enabled
	a.detectWarpIP()

	// Init WireGuard (for local workload routing)
	// In tunnel-only mode, WG is still used for local mesh IP routing but not for inter-node traffic
	if err := a.initWireGuard(); err != nil {
		return fmt.Errorf("wireguard: %w", err)
	}

	// In tunnel-only mode, start the local UDP socket for packet injection
	if a.tunnelDomain != "" {
		if err := a.initWGTunnel(); err != nil {
			return fmt.Errorf("wg tunnel: %w", err)
		}
	}

	// Load state
	a.loadState()

	// Join or bootstrap
	if a.joinURL != "" {
		if err := a.joinCluster(); err != nil {
			return fmt.Errorf("join: %w", err)
		}
	} else {
		// Not joining - sync state from known peers before autostart
		// This handles the case where we restarted and workloads were revived by other nodes
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

	mode := "wireguard"
	if a.wgDisabled {
		mode = "dummy (local only)"
	}
	if a.tunnelDomain != "" {
		if a.wgDisabled {
			mode = "tunnel-only (" + a.tunnelDomain + ") [dummy + HTTP proxy]"
		} else {
			mode = "tunnel-only (" + a.tunnelDomain + ") [WebSocket UDP tunnel]"
		}
	}
	if a.warpEnabled {
		mode = "warp (" + a.warpIP + ")"
	}
	log.Printf("Jetty started: %s (%s) @ %s [mode: %s]", a.hostname, a.hwid[:12], a.meshIP, mode)
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
		a.warpIP = ip
		a.warpEnabled = true
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
			a.warpIP = ipnet.IP.String()
			a.warpEnabled = true
			log.Printf("WARP IP detected: %s", a.warpIP)
			return
		}
	}
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
	a.stopUDPRelays()
	if a.wgLocalConn != nil {
		a.wgLocalConn.Close()
	}
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
						exec.Command("iptables", "-t", "nat", "-D", "PREROUTING", "-d", wl.MeshIP, "-j", "DNAT", "--to", containerIP).Run()
						exec.Command("iptables", "-t", "nat", "-D", "OUTPUT", "-d", wl.MeshIP, "-j", "DNAT", "--to", containerIP).Run()
						log.Printf("Removed iptables rules for %s (%s -> %s)", wl.Name, wl.MeshIP, containerIP)
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
// Network Interface (WireGuard or Dummy fallback)
// =============================================================================

func (a *Agent) initWireGuard() error {
	keyFile := filepath.Join(a.dataDir, "wg_private_key")

	// Load or generate keys (still useful for peer identity even without WG)
	if data, err := os.ReadFile(keyFile); err == nil {
		a.wgPrivKey = strings.TrimSpace(string(data))
		a.wgPubKey = derivePubKey(a.wgPrivKey)
	} else {
		a.wgPrivKey, a.wgPubKey = genKeyPair()
		os.WriteFile(keyFile, []byte(a.wgPrivKey), 0600)
	}

	// Derive mesh IP
	a.meshIP = a.deriveMeshIP(a.hwid)

	// Clean up any existing interface
	exec.Command("ip", "link", "del", "jetty0").Run()

	// Try WireGuard first
	if err := a.tryWireGuard(keyFile); err != nil {
		log.Printf("WireGuard not available (%v), using dummy interface", err)
		if err := a.initDummyInterface(); err != nil {
			return err
		}
	}

	// Enable forwarding
	os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1"), 0644)

	return nil
}

// tryWireGuard attempts to create a WireGuard interface
func (a *Agent) tryWireGuard(keyFile string) error {
	// Try to create WireGuard interface
	if err := exec.Command("ip", "link", "add", "dev", "jetty0", "type", "wireguard").Run(); err != nil {
		return fmt.Errorf("create interface: %w", err)
	}

	// Configure WireGuard
	if err := exec.Command("wg", "set", "jetty0",
		"listen-port", fmt.Sprintf("%d", a.wgPort),
		"private-key", keyFile).Run(); err != nil {
		// Clean up failed interface
		exec.Command("ip", "link", "del", "jetty0").Run()
		return fmt.Errorf("configure wg: %w", err)
	}

	// Add IP and bring up
	_, network, _ := net.ParseCIDR(a.meshCIDR)
	pfx, _ := network.Mask.Size()
	exec.Command("ip", "addr", "add", fmt.Sprintf("%s/%d", a.meshIP, pfx), "dev", "jetty0").Run()
	exec.Command("ip", "link", "set", "up", "dev", "jetty0").Run()

	log.Printf("WireGuard up: %s", a.meshIP)
	return nil
}

// initDummyInterface creates a dummy interface for local IP binding
// Used when WireGuard kernel module is not available
func (a *Agent) initDummyInterface() error {
	a.wgDisabled = true

	// Create dummy interface
	if err := exec.Command("ip", "link", "add", "dev", "jetty0", "type", "dummy").Run(); err != nil {
		return fmt.Errorf("create dummy interface: %w", err)
	}

	// Add IP and bring up
	_, network, _ := net.ParseCIDR(a.meshCIDR)
	pfx, _ := network.Mask.Size()
	exec.Command("ip", "addr", "add", fmt.Sprintf("%s/%d", a.meshIP, pfx), "dev", "jetty0").Run()
	exec.Command("ip", "link", "set", "up", "dev", "jetty0").Run()

	log.Printf("Dummy interface up: %s (WireGuard disabled, use tunnel/WARP for inter-node)", a.meshIP)
	return nil
}

func (a *Agent) addWGPeer(p *Peer) {
	// Skip WireGuard operations if using dummy interface
	if a.wgDisabled {
		return
	}

	if p.PublicKey == a.wgPubKey {
		return
	}

	// Add peer's mesh IP
	allowedIPs := p.MeshIP + "/32"

	// Also add any workload IPs owned by this peer
	a.stateMu.RLock()
	for _, w := range a.state.Workloads {
		if w.Owner == p.ID && w.MeshIP != "" {
			allowedIPs += "," + w.MeshIP + "/32"
		}
	}
	a.stateMu.RUnlock()

	args := []string{"set", "jetty0", "peer", p.PublicKey,
		"allowed-ips", allowedIPs,
		"persistent-keepalive", "25"}

	// In tunnel-only mode, use local UDP relay instead of real endpoint
	if a.tunnelDomain != "" {
		relayEndpoint, err := a.getRelayEndpoint(p.ID)
		if err != nil {
			log.Printf("Failed to create relay for peer %s: %v", p.Name, err)
		} else {
			args = append(args, "endpoint", relayEndpoint)
		}
	} else if p.Endpoint != "" {
		args = append(args, "endpoint", p.Endpoint)
	}

	exec.Command("wg", args...).Run()
}

func (a *Agent) updateWGPeers() {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()

	for _, p := range a.state.Peers {
		a.addWGPeer(p)
	}
}

func (a *Agent) deriveMeshIP(id string) string {
	_, network, _ := net.ParseCIDR(a.meshCIDR)
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

	// Add mode indicator
	if a.tunnelDomain != "" {
		jettyLines = append(jettyLines, fmt.Sprintf("# Mode: tunnel-only (%s)", a.tunnelDomain))
		jettyLines = append(jettyLines, "# Note: Remote workloads accessible via /api/proxy/{mesh_ip}/")
	} else {
		jettyLines = append(jettyLines, "# Mode: wireguard mesh")
	}

	// Add self
	jettyLines = append(jettyLines, fmt.Sprintf("%s\t%s\t# this node", a.meshIP, a.hostname))

	// Add peers
	for _, p := range a.state.Peers {
		status := "healthy"
		if !p.Healthy {
			status = "unhealthy"
		}
		jettyLines = append(jettyLines, fmt.Sprintf("%s\t%s\t# peer (%s)", p.MeshIP, p.Name, status))
	}

	// Add workloads
	for _, w := range a.state.Workloads {
		if w.MeshIP != "" && w.Name != "" {
			location := "local"
			if w.Owner != a.hwid {
				location = "remote"
			}
			jettyLines = append(jettyLines, fmt.Sprintf("%s\t%s\t# workload (%s)", w.MeshIP, w.Name, location))
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
	log.Printf("Joining cluster via %s", a.joinURL)

	publicIP := getPublicIP()

	req := map[string]string{
		"token":       a.joinTok,
		"secret":      a.clusterSecret,
		"id":          a.hwid,
		"name":        a.hostname,
		"mesh_ip":     a.meshIP,
		"endpoint":    fmt.Sprintf("%s:%d", publicIP, a.wgPort),
		"public_key":  a.wgPubKey,
		"tunnel_host": a.tunnelHost, // Our specific subdomain for direct WG packet routing
		"warp_ip":     a.warpIP,     // Cloudflare WARP IP for L3 connectivity
	}

	data, _ := json.Marshal(req)
	resp, err := httpClient.Post(a.joinURL+"/api/join", "application/json", strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("join failed: %s", body)
	}

	var result struct {
		Peers     []*Peer     `json:"peers"`
		Workloads []*Workload `json:"workloads"`
		CFToken   string      `json:"cf_token,omitempty"`
		WarpToken string      `json:"warp_token,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode join response: %w", err)
	}

	a.stateMu.Lock()
	for _, p := range result.Peers {
		a.state.Peers[p.ID] = p
	}
	for _, w := range result.Workloads {
		a.state.Workloads[w.MeshIP] = w
	}
	// Store tokens received from the cluster
	if result.CFToken != "" {
		a.state.CFToken = result.CFToken
	}
	if result.WarpToken != "" {
		a.state.WarpToken = result.WarpToken
	}
	a.stateMu.Unlock()

	a.updateWGPeers()
	a.saveState()

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
			"/api/health",      // Monitoring
			"/api/join",        // Node joining (uses token + secret in body)
			"/api/sync",        // Internal cluster sync (uses secret in body)
			"/api/peer-announce", // Internal peer announcement
			"/api/heartbeat",   // Internal heartbeat
			"/api/tunnel/sync", // Internal tunnel sync
			"/api/token/sync",  // Internal token sync
			"/api/ws/wg",       // WireGuard WebSocket tunnel
			"/api/wg/packet",   // WireGuard HTTP tunnel
			"/swagger/",        // API documentation
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
	r := mux.NewRouter()

	r.HandleFunc("/api/status", a.apiStatus).Methods("GET")
	r.HandleFunc("/api/workloads", a.apiListWorkloads).Methods("GET")
	r.HandleFunc("/api/workloads", a.apiCreateWorkload).Methods("POST")
	r.HandleFunc("/api/workloads/{name}", a.apiGetWorkload).Methods("GET")
	r.HandleFunc("/api/workloads/{name}", a.apiDeleteWorkload).Methods("DELETE")
	r.HandleFunc("/api/workloads/{name}/move", a.apiMoveWorkload).Methods("POST")
	r.HandleFunc("/api/workloads/{name}/logs", a.apiWorkloadLogs).Methods("GET")
	r.HandleFunc("/api/workloads/{name}/start", a.apiStartWorkload).Methods("POST")
	r.HandleFunc("/api/workloads/{name}/stop", a.apiStopWorkload).Methods("POST")
	r.HandleFunc("/api/token", a.apiCreateToken).Methods("POST", "GET")
	r.HandleFunc("/api/join", a.apiJoin).Methods("POST")
	r.HandleFunc("/api/health", a.apiHealth).Methods("GET")
	r.HandleFunc("/api/sync", a.apiSync).Methods("GET")
	r.HandleFunc("/api/tunnel", a.apiGetTunnel).Methods("GET")
	r.HandleFunc("/api/tunnel", a.apiSetTunnel).Methods("POST")
	r.HandleFunc("/api/tunnel", a.apiDeleteTunnel).Methods("DELETE")
	r.HandleFunc("/api/tunnel/sync", a.apiTunnelSync).Methods("POST")
	r.HandleFunc("/api/token/sync", a.apiTokenSync).Methods("POST")
	r.HandleFunc("/api/peer-announce", a.apiPeerAnnounce).Methods("POST")
	r.HandleFunc("/api/heartbeat", a.apiHeartbeat).Methods("POST")
	r.PathPrefix("/api/proxy/").HandlerFunc(a.apiWorkloadProxy)
	r.HandleFunc("/api/ws/wg", a.wsWireGuard)                     // WebSocket endpoint for WG packets
	r.HandleFunc("/api/wg/packet", a.apiWGPacket).Methods("POST") // HTTP fallback for WG packets

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
			"id":         a.hwid,
			"name":       a.hostname,
			"mesh_ip":    a.meshIP,
			"public_key": a.wgPubKey,
			"warp_ip":    a.warpIP,
		},
		"peers":     peers,
		"workloads": workloads,
		"wireguard": map[string]interface{}{
			"enabled": !a.wgDisabled,
			"mode":    func() string { if a.wgDisabled { return "dummy" } else { return "kernel" } }(),
		},
		"tunnel": map[string]interface{}{
			"configured": hasTunnel,
			"running":    a.isTunnelRunning(),
		},
		"warp": map[string]interface{}{
			"enabled": a.warpEnabled,
			"ip":      a.warpIP,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// apiListWorkloads godoc
// @Summary List all workloads
// @Description Returns all workloads in the cluster
// @Tags workloads
// @Produce json
// @Success 200 {array} Workload
// @Router /workloads [get]
func (a *Agent) apiListWorkloads(w http.ResponseWriter, r *http.Request) {
	a.stateMu.RLock()
	workloads := make([]*Workload, 0, len(a.state.Workloads))
	for _, wl := range a.state.Workloads {
		workloads = append(workloads, wl)
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
	var wl Workload
	if err := json.NewDecoder(r.Body).Decode(&wl); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	if wl.Name == "" || wl.MeshIP == "" || wl.Compose == "" {
		http.Error(w, "name, mesh_ip, compose required", 400)
		return
	}

	// Validate workload name (prevent path traversal attacks)
	if !validNamePattern.MatchString(wl.Name) {
		http.Error(w, "invalid name: must be alphanumeric with dash/underscore only", 400)
		return
	}

	// Validate mesh IP format
	if net.ParseIP(wl.MeshIP) == nil {
		http.Error(w, "invalid mesh_ip: must be valid IP address", 400)
		return
	}

	// Lock for entire check-and-set to prevent race condition
	a.stateMu.Lock()
	existing := a.state.Workloads[wl.MeshIP]
	if existing != nil && existing.Owner != a.hwid {
		a.stateMu.Unlock()
		http.Error(w, "mesh_ip already in use", 409)
		return
	}

	wl.Owner = a.hwid
	wl.Version = time.Now().Unix()
	a.state.Workloads[wl.MeshIP] = &wl
	a.stateMu.Unlock()

	// Deploy (outside lock to avoid blocking other operations)
	if err := a.deployWorkload(&wl); err != nil {
		// Rollback on failure
		a.stateMu.Lock()
		delete(a.state.Workloads, wl.MeshIP)
		a.stateMu.Unlock()
		http.Error(w, err.Error(), 500)
		return
	}

	a.updateHosts()
	a.updateWGPeers()
	a.saveState()
	a.broadcastState()

	// Register WARP private network route for this workload
	if err := a.registerWarpRoute(wl.MeshIP); err != nil {
		log.Printf("Warning: failed to register WARP route for %s: %v", wl.MeshIP, err)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(wl)
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

	// If workload is remote, proxy the request to the owner node
	if found.Owner != a.hwid {
		if ownerPeer == nil || !ownerPeer.Healthy {
			// Owner not reachable, return basic info without container details
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"name":       found.Name,
				"mesh_ip":    found.MeshIP,
				"compose":    found.Compose,
				"revive":     found.Revive,
				"autostart":  found.Autostart,
				"owner":      found.Owner,
				"version":    found.Version,
				"is_local":   false,
				"owner_node": ownerPeer.Name,
				"error":      "owner node unreachable",
			})
			return
		}

		// Proxy request to owner
		url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name)
		resp, err := httpClient.Get(url)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"name":       found.Name,
				"mesh_ip":    found.MeshIP,
				"compose":    found.Compose,
				"revive":     found.Revive,
				"autostart":  found.Autostart,
				"owner":      found.Owner,
				"version":    found.Version,
				"is_local":   false,
				"owner_node": ownerPeer.Name,
				"error":      fmt.Sprintf("failed to reach owner: %v", err),
			})
			return
		}
		defer resp.Body.Close()

		// Forward the response from owner
		w.Header().Set("Content-Type", "application/json")
		body, _ := io.ReadAll(resp.Body)

		// Parse and add owner_node field
		var remoteResp map[string]interface{}
		if err := json.Unmarshal(body, &remoteResp); err == nil {
			remoteResp["owner_node"] = ownerPeer.Name
			remoteResp["is_local"] = false // Override since we proxied
			json.NewEncoder(w).Encode(remoteResp)
		} else {
			w.Write(body)
		}
		return
	}

	// Build enriched response with Docker info for local workload
	response := map[string]interface{}{
		"name":      found.Name,
		"mesh_ip":   found.MeshIP,
		"compose":   found.Compose,
		"revive":    found.Revive,
		"autostart": found.Autostart,
		"owner":     found.Owner,
		"version":   found.Version,
		"is_local":  true,
	}

	containerInfo := a.getContainerInfo(found.Name)
	response["containers"] = containerInfo

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

	a.stateMu.Lock()
	var found *Workload
	var foundIP string
	for ip, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			foundIP = ip
			break
		}
	}

	if found == nil {
		a.stateMu.Unlock()
		http.Error(w, "not found", 404)
		return
	}

	// Only owner can delete (or anyone if owner is dead)
	if found.Owner != a.hwid {
		peer := a.state.Peers[found.Owner]
		if peer != nil && peer.Healthy {
			a.stateMu.Unlock()
			http.Error(w, "workload owned by another node", 403)
			return
		}
	}

	delete(a.state.Workloads, foundIP)
	a.stateMu.Unlock()

	// Remove if we're running it
	if found.Owner == a.hwid {
		a.removeWorkload(found)

		// Unregister WARP route for this workload
		if err := a.unregisterWarpRoute(found.MeshIP); err != nil {
			log.Printf("Warning: failed to unregister WARP route for %s: %v", found.MeshIP, err)
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

	// Find workload
	a.stateMu.RLock()
	var found *Workload
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
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

	// Deploy on target
	data, _ := json.Marshal(found)
	resp, err := httpClient.Post(fmt.Sprintf("http://%s:%d/api/workloads", target.MeshIP, a.apiPort),
		"application/json", strings.NewReader(string(data)))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		http.Error(w, "target rejected", 500)
		return
	}

	// Remove locally if we own it
	if found.Owner == a.hwid {
		a.removeWorkload(found)

		// Unregister WARP route (target will register its own)
		if err := a.unregisterWarpRoute(found.MeshIP); err != nil {
			log.Printf("Warning: failed to unregister WARP route for %s: %v", found.MeshIP, err)
		}
	}

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
	for _, wl := range a.state.Workloads {
		if wl.Name == name && wl.Owner == a.hwid {
			found = wl
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found or not local", 404)
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
	for _, wl := range a.state.Workloads {
		if wl.Name == name && wl.Owner == a.hwid {
			found = wl
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found or not local", 404)
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
	for _, wl := range a.state.Workloads {
		if wl.Name == name && wl.Owner == a.hwid {
			found = wl
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found or not local", 404)
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

// apiCreateToken godoc
// @Summary Create join token
// @Description Generates a new single-use token for joining the cluster (expires in 24h)
// @Tags cluster
// @Produce json
// @Success 200 {object} TokenResponse
// @Router /token [post]
func (a *Agent) apiCreateToken(w http.ResponseWriter, r *http.Request) {
	tok := &Token{
		Token:     genID() + genID(),
		ExpiresAt: time.Now().Add(24 * time.Hour),
	}

	a.stateMu.Lock()
	a.state.Tokens[tok.Token] = tok
	a.stateMu.Unlock()

	a.saveState()

	// Broadcast token to all peers so any node can validate it
	// (important when using Cloudflare tunnel load balancing)
	go a.broadcastToken(tok)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tok)
}

// apiJoin godoc
// @Summary Join cluster
// @Description Allows a new node to join the cluster using a token
// @Tags cluster
// @Accept json
// @Produce json
// @Param request body JoinRequest true "Join request"
// @Success 200 {object} JoinResponse
// @Failure 401 {object} ErrorResponse "Invalid token or secret"
// @Failure 409 {object} ErrorResponse "Mesh IP collision"
// @Router /join [post]
func (a *Agent) apiJoin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token      string `json:"token"`
		Secret     string `json:"secret"`
		ID         string `json:"id"`
		Name       string `json:"name"`
		MeshIP     string `json:"mesh_ip"`
		Endpoint   string `json:"endpoint"`
		PublicKey  string `json:"public_key"`
		TunnelHost string `json:"tunnel_host"`
		WarpIP     string `json:"warp_ip"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	// Validate cluster secret first
	if a.clusterSecret != "" && req.Secret != a.clusterSecret {
		http.Error(w, "invalid cluster secret", 401)
		return
	}

	// Validate and consume token (single-use)
	a.stateMu.Lock()
	tok := a.state.Tokens[req.Token]
	if tok == nil || time.Now().After(tok.ExpiresAt) {
		a.stateMu.Unlock()
		http.Error(w, "invalid token", 401)
		return
	}
	// Delete token after use (single-use tokens)
	delete(a.state.Tokens, req.Token)
	a.stateMu.Unlock()

	// Broadcast token deletion to other nodes
	go a.broadcastTokenDeletion(req.Token)

	// Check for mesh IP collision before creating peer
	a.stateMu.RLock()
	// Check against our own IP
	if req.MeshIP == a.meshIP {
		a.stateMu.RUnlock()
		http.Error(w, "mesh_ip collision with existing node", 409)
		return
	}
	// Check against existing peers
	for _, p := range a.state.Peers {
		if p.MeshIP == req.MeshIP && p.ID != req.ID {
			a.stateMu.RUnlock()
			http.Error(w, "mesh_ip collision with existing node", 409)
			return
		}
	}
	// Check against workloads
	if _, exists := a.state.Workloads[req.MeshIP]; exists {
		a.stateMu.RUnlock()
		http.Error(w, "mesh_ip collision with existing workload", 409)
		return
	}
	a.stateMu.RUnlock()

	// Create peer
	peer := &Peer{
		ID:         req.ID,
		Name:       req.Name,
		MeshIP:     req.MeshIP,
		Endpoint:   req.Endpoint,
		PublicKey:  req.PublicKey,
		TunnelHost: req.TunnelHost,
		WarpIP:     req.WarpIP,
		Healthy:    true,
		LastSeen:   time.Now(),
	}

	a.stateMu.Lock()
	a.state.Peers[peer.ID] = peer

	// Build response with all peers (including self)
	allPeers := []*Peer{{
		ID:         a.hwid,
		Name:       a.hostname,
		MeshIP:     a.meshIP,
		Endpoint:   fmt.Sprintf("%s:%d", getPublicIP(), a.wgPort),
		PublicKey:  a.wgPubKey,
		TunnelHost: a.tunnelHost,
		WarpIP:     a.warpIP,
		Healthy:    true,
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

	a.addWGPeer(peer)
	a.updateHosts()
	a.saveState()

	// Notify other peers
	go a.announcePeer(peer)

	resp := map[string]interface{}{
		"peers":     allPeers,
		"workloads": allWorkloads,
	}

	// Include CF token so new peer can start its tunnel
	if cfToken != "" {
		resp["cf_token"] = cfToken
	}

	// Include WARP connector token so new peer can join WARP network
	if warpToken != "" {
		resp["warp_token"] = warpToken
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)

	log.Printf("Peer joined: %s (%s)", peer.Name, peer.MeshIP)
}

// apiHealth godoc
// @Summary Health check
// @Description Returns health status of this node
// @Tags cluster
// @Produce json
// @Success 200 {object} HealthResponse
// @Router /health [get]
func (a *Agent) apiHealth(w http.ResponseWriter, r *http.Request) {
	// Count workloads
	a.stateMu.RLock()
	var localWorkloads []string
	var totalWorkloads int
	for _, wl := range a.state.Workloads {
		totalWorkloads++
		if wl.Owner == a.hwid {
			// Check if container is running
			out, _ := exec.Command("docker", "ps", "-q", "-f", "label=com.docker.compose.project=jetty_"+wl.Name).Output()
			status := "stopped"
			if len(strings.TrimSpace(string(out))) > 0 {
				status = "running"
			}
			localWorkloads = append(localWorkloads, fmt.Sprintf("%s:%s:%s", wl.Name, wl.MeshIP, status))
		}
	}
	a.stateMu.RUnlock()

	// Get system stats
	systemStats := a.getSystemStats()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":          getHealthStatus(),
		"id":              a.hwid,
		"name":            a.hostname,
		"mesh_ip":         a.meshIP,
		"public_ip":       getPublicIP(),
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
		"workloads_local": localWorkloads,
		"workloads_total": totalWorkloads,
		"wireguard_mode":  func() string { if a.wgDisabled { return "dummy" } else { return "kernel" } }(),
		"warp_ip":         a.warpIP,
		"system":          systemStats,
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
	// Return our workloads for sync
	a.stateMu.RLock()
	local := []*Workload{}
	for _, wl := range a.state.Workloads {
		if wl.Owner == a.hwid {
			local = append(local, wl)
		}
	}
	a.stateMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(local)
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

	a.addWGPeer(&req.Peer)
	a.updateHosts()
	a.saveState()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

	log.Printf("Peer announced: %s (%s)", req.Peer.Name, req.Peer.MeshIP)
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
			targetURL = fmt.Sprintf("http://%s:%d/api/proxy/%s%s", owner.MeshIP, a.apiPort, meshIP, targetPath)
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
		url := fmt.Sprintf("http://%s:%d/api/tunnel/sync", peer.MeshIP, a.apiPort)
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

// apiTokenSync receives a token from another node (for cluster-wide token availability)
// Also handles token deletion when "delete" field is set.
func (a *Agent) apiTokenSync(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
		Delete    string    `json:"delete,omitempty"` // Token to delete (if set)
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	a.stateMu.Lock()
	if req.Delete != "" {
		// Delete a consumed token
		delete(a.state.Tokens, req.Delete)
	} else if req.Token != "" && time.Now().Before(req.ExpiresAt) {
		// Add new token if not expired
		if _, exists := a.state.Tokens[req.Token]; !exists {
			a.state.Tokens[req.Token] = &Token{
				Token:     req.Token,
				ExpiresAt: req.ExpiresAt,
			}
		}
	}
	a.stateMu.Unlock()
	a.saveState()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// broadcastToken sends a new token to all peers so any node can validate join requests.
// This is critical when using Cloudflare tunnel - the join request may hit any node.
func (a *Agent) broadcastToken(tok *Token) {
	data, _ := json.Marshal(tok)

	// In tunnel-only mode, broadcast to tunnel (gossip will propagate)
	if a.tunnelDomain != "" {
		url := a.getTunnelAPIURL("/api/token/sync")
		resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
		if err != nil {
			log.Printf("Failed to broadcast token: %v", err)
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
		url := fmt.Sprintf("http://%s:%d/api/token/sync", peer.MeshIP, a.apiPort)
		resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
		if err != nil {
			log.Printf("Failed to broadcast token to %s: %v", peer.Name, err)
			continue
		}
		resp.Body.Close()
	}
}

// broadcastTokenDeletion tells all peers to delete a consumed token.
func (a *Agent) broadcastTokenDeletion(tokenStr string) {
	data, _ := json.Marshal(map[string]string{"delete": tokenStr})

	// In tunnel-only mode, broadcast to tunnel
	if a.tunnelDomain != "" {
		url := a.getTunnelAPIURL("/api/token/sync")
		resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
		if err == nil {
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
		url := fmt.Sprintf("http://%s:%d/api/token/sync", peer.MeshIP, a.apiPort)
		resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
		if err != nil {
			continue
		}
		resp.Body.Close()
	}
}

// cleanupExpiredTokens removes tokens that have expired from state.
func (a *Agent) cleanupExpiredTokens() {
	a.stateMu.Lock()
	defer a.stateMu.Unlock()

	now := time.Now()
	for k, tok := range a.state.Tokens {
		if now.After(tok.ExpiresAt) {
			delete(a.state.Tokens, k)
		}
	}
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
			if err := a.registerWarpRoute(wl.MeshIP); err != nil {
				log.Printf("Warning: failed to register WARP route for %s: %v", wl.MeshIP, err)
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
	if wl.MeshIP != "" {
		a.setupWorkloadIP(wl)
	}

	log.Printf("Deployed: %s @ %s", wl.Name, wl.MeshIP)
	return nil
}

func (a *Agent) removeWorkload(wl *Workload) {
	// Clean up iptables BEFORE stopping container (need container IP)
	if wl.MeshIP != "" {
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
				exec.Command("iptables", "-t", "nat", "-D", "PREROUTING", "-d", wl.MeshIP, "-j", "DNAT", "--to", containerIP).Run()
				exec.Command("iptables", "-t", "nat", "-D", "OUTPUT", "-d", wl.MeshIP, "-j", "DNAT", "--to", containerIP).Run()
			}
		}
	}

	// Remove mesh IP from interface
	exec.Command("ip", "addr", "del", wl.MeshIP+"/32", "dev", "jetty0").Run()
}

func (a *Agent) setupWorkloadIP(wl *Workload) {
	// Add IP to interface
	exec.Command("ip", "addr", "add", wl.MeshIP+"/32", "dev", "jetty0").Run()

	// Get container IP - use docker ps to find containers in the project
	// This handles any service name in the compose file
	out, err := exec.Command("docker", "ps", "-q", "-f", "label=com.docker.compose.project=jetty_"+wl.Name).Output()
	if err != nil || len(out) == 0 {
		log.Printf("Warning: no containers found for project jetty_%s", wl.Name)
		return
	}

	// Get first container ID
	containerID := strings.Split(strings.TrimSpace(string(out)), "\n")[0]
	if containerID == "" {
		log.Printf("Warning: couldn't get container ID for %s", wl.Name)
		return
	}

	// Get container IP
	out, err = exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerID).Output()
	if err != nil {
		log.Printf("Warning: couldn't inspect container %s: %v", containerID, err)
		return
	}

	containerIP := strings.TrimSpace(string(out))
	if containerIP == "" {
		log.Printf("Warning: couldn't get container IP for %s", wl.Name)
		return
	}

	// DNAT
	exec.Command("iptables", "-t", "nat", "-A", "PREROUTING", "-d", wl.MeshIP, "-j", "DNAT", "--to", containerIP).Run()
	exec.Command("iptables", "-t", "nat", "-A", "OUTPUT", "-d", wl.MeshIP, "-j", "DNAT", "--to", containerIP).Run()

	log.Printf("Routed: %s -> %s", wl.MeshIP, containerIP)
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
			a.cleanupExpiredTokens()
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
		url := fmt.Sprintf("http://%s:%d/api/health", peer.MeshIP, a.apiPort)
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
		log.Printf("Heartbeat failed: %v", err)
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
				existing := a.state.Workloads[w.MeshIP]
				if existing == nil || w.Version > existing.Version {
					if existing != nil && existing.Owner == a.hwid && w.Owner != a.hwid {
						log.Printf("Workload %s was revived by %s while we were down", w.Name, w.Owner[:12])
					}
					a.state.Workloads[w.MeshIP] = w
				}
			}
			a.stateMu.Unlock()
			synced = true
		}
	}

	// Try direct peer connections
	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/sync", peer.MeshIP, a.apiPort)
		resp, err := httpClient.Get(url)
		if err != nil {
			continue
		}

		var workloads []*Workload
		json.NewDecoder(resp.Body).Decode(&workloads)
		resp.Body.Close()

		a.stateMu.Lock()
		for _, w := range workloads {
			existing := a.state.Workloads[w.MeshIP]
			if existing == nil || w.Version > existing.Version {
				if existing != nil && existing.Owner == a.hwid && w.Owner != a.hwid {
					log.Printf("Workload %s was revived by %s while we were down", w.Name, w.Owner[:12])
				}
				a.state.Workloads[w.MeshIP] = w
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
		url := fmt.Sprintf("http://%s:%d/api/sync", peer.MeshIP, a.apiPort)
		resp, err := httpClient.Get(url)
		if err != nil {
			continue
		}

		var workloads []*Workload
		json.NewDecoder(resp.Body).Decode(&workloads)
		resp.Body.Close()

		a.stateMu.Lock()
		for _, w := range workloads {
			existing := a.state.Workloads[w.MeshIP]
			if existing == nil || w.Version > existing.Version {
				// Check if we lost ownership (IP collision resolution)
				if existing != nil && existing.Owner == a.hwid && w.Owner != a.hwid {
					log.Printf("Lost ownership of %s (IP %s) to %s - newer version wins", existing.Name, w.MeshIP, w.Owner[:12])
					lostOwnership = append(lostOwnership, existing)
				}
				a.state.Workloads[w.MeshIP] = w
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
		existing := a.state.Workloads[w.MeshIP]
		if existing == nil || w.Version > existing.Version {
			// Check if we lost ownership (IP collision resolution)
			if existing != nil && existing.Owner == a.hwid && w.Owner != a.hwid {
				log.Printf("Lost ownership of %s (IP %s) to %s - newer version wins", existing.Name, w.MeshIP, w.Owner[:12])
				lostOwnership = append(lostOwnership, existing)
			}
			a.state.Workloads[w.MeshIP] = w
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
		url := fmt.Sprintf("http://%s:%d/api/sync", peer.MeshIP, a.apiPort)
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
		url := fmt.Sprintf("http://%s:%d/api/peer-announce", peer.MeshIP, a.apiPort)
		resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
		if err != nil {
			log.Printf("Failed to announce peer to %s: %v", peer.Name, err)
			continue
		}
		resp.Body.Close()
	}
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
		if owner != nil && time.Since(owner.LastSeen) < 30*time.Second {
			continue // Recently seen
		}

		// Owner is dead - should we claim?
		if a.shouldClaim(wl) {
			log.Printf("Claiming orphaned workload: %s", wl.Name)
			wl.Owner = a.hwid
			wl.Version = time.Now().Unix()

			// Deploy it
			go func(w *Workload) {
				if err := a.deployWorkload(w); err != nil {
					log.Printf("Failover deploy failed: %v", err)
				}
				a.updateHosts()
				a.updateWGPeers()
				a.saveState()
			}(wl)
		}
	}
}

// shouldClaim determines if this node should claim an orphaned workload.
// NOTE: Caller must already hold stateMu lock.
func (a *Agent) shouldClaim(wl *Workload) bool {
	// Deterministic: lowest healthy node ID wins
	candidates := []string{a.hwid}
	for _, p := range a.state.Peers {
		if p.Healthy {
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

	// Reconnect WG peers
	a.updateWGPeers()

	log.Printf("Loaded: %d peers, %d workloads", len(a.state.Peers), len(a.state.Workloads))
}

// =============================================================================
// Cloudflare Tunnel
// =============================================================================

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

	// Start cloudflared tunnel
	a.cfCmd = exec.Command("cloudflared", "tunnel", "run", "--token", token)
	a.cfCmd.Stdout = os.Stdout
	a.cfCmd.Stderr = os.Stderr

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
func (a *Agent) monitorCloudflared() {
	for {
		a.cfMu.Lock()
		cmd := a.cfCmd
		stopCh := a.cfStopCh
		a.cfMu.Unlock()

		if cmd == nil {
			return
		}

		// Wait for process to exit
		err := cmd.Wait()

		// Check if we were asked to stop
		select {
		case <-stopCh:
			return
		default:
		}

		if err != nil {
			log.Printf("Cloudflare tunnel exited: %v, restarting in 5s...", err)
		} else {
			log.Printf("Cloudflare tunnel exited, restarting in 5s...")
		}

		time.Sleep(5 * time.Second)

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

		a.cfCmd = exec.Command("cloudflared", "tunnel", "run", "--token", token)
		a.cfCmd.Stdout = os.Stdout
		a.cfCmd.Stderr = os.Stderr

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
// WebSocket WireGuard Tunnel
// =============================================================================

// initWGTunnel initializes the WebSocket tunnel for WireGuard packets.
// This creates a local UDP socket that can inject packets into the WireGuard interface.
func (a *Agent) initWGTunnel() error {
	// Create local UDP socket for injecting packets into WireGuard
	// We'll send packets TO WireGuard's listen port from various source ports
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	a.wgLocalConn = conn

	log.Printf("WG tunnel initialized (injection socket: %s)", conn.LocalAddr())
	return nil
}

// wsWireGuard handles WebSocket connections for WireGuard packet relay.
// Packets received here are injected into the local WireGuard interface.
func (a *Agent) wsWireGuard(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	log.Printf("WG WebSocket connection from %s", r.RemoteAddr)

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WG WebSocket error: %v", err)
			}
			return
		}

		var pkt WGPacket
		if err := json.Unmarshal(data, &pkt); err != nil {
			continue
		}

		// Check if this packet is for us
		if pkt.ToID == a.hwid {
			a.injectWGPacket(pkt.FromID, pkt.Data)
		}
		// In tunnel mode, we don't forward - let the sender retry through the tunnel
	}
}

// apiWGPacket handles HTTP POST for WireGuard packets (fallback when WebSocket isn't available).
func (a *Agent) apiWGPacket(w http.ResponseWriter, r *http.Request) {
	var pkt WGPacket
	if err := json.NewDecoder(r.Body).Decode(&pkt); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// Check if this packet is for us
	if pkt.ToID == a.hwid {
		a.injectWGPacket(pkt.FromID, pkt.Data)
		w.WriteHeader(http.StatusOK)
		return
	}

	// Not for us - forward through tunnel
	if a.tunnelDomain != "" {
		go a.forwardWGPacket(&pkt)
	}
	w.WriteHeader(http.StatusAccepted)
}

// injectWGPacket injects a received WireGuard packet into the local interface.
// The packet is sent to WireGuard's listen port as if it came from the peer.
func (a *Agent) injectWGPacket(fromID string, data []byte) {
	if a.wgLocalConn == nil {
		return
	}

	// Send to WireGuard's listen port
	wgAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", a.wgPort))
	if err != nil {
		return
	}

	_, err = a.wgLocalConn.WriteToUDP(data, wgAddr)
	if err != nil {
		log.Printf("Failed to inject WG packet from %s: %v", fromID[:12], err)
	}
}

// createUDPRelay creates a UDP relay for a peer in tunnel-only mode.
// WireGuard sends packets to this relay, which forwards them via WebSocket.
func (a *Agent) createUDPRelay(peerID string) (*udpRelay, error) {
	a.wgRelaysMu.Lock()
	defer a.wgRelaysMu.Unlock()

	// Check if relay already exists
	if relay, exists := a.wgRelays[peerID]; exists {
		return relay, nil
	}

	// Find next available port
	port := a.wgRelayBase + len(a.wgRelays)

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	relay := &udpRelay{
		peerID:    peerID,
		conn:      conn,
		localPort: port,
		stopCh:    make(chan struct{}),
	}

	a.wgRelays[peerID] = relay

	// Start relay goroutine
	go a.runUDPRelay(relay)

	log.Printf("Created UDP relay for peer %s on port %d", peerID[:12], port)
	return relay, nil
}

// runUDPRelay reads packets from the local UDP relay and forwards them via the tunnel.
func (a *Agent) runUDPRelay(relay *udpRelay) {
	buf := make([]byte, 65535)

	for {
		select {
		case <-relay.stopCh:
			return
		case <-a.stopCh:
			return
		default:
		}

		relay.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, _, err := relay.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			continue
		}

		// Create packet and send through tunnel
		pkt := &WGPacket{
			FromID: a.hwid,
			ToID:   relay.peerID,
			Data:   make([]byte, n),
		}
		copy(pkt.Data, buf[:n])

		go a.sendWGPacket(pkt)
	}
}

// sendWGPacket sends a WireGuard packet through the Cloudflare tunnel.
// If the target peer has a specific TunnelHost, send directly to that subdomain.
// Otherwise, fall back to the general tunnel domain (random routing).
func (a *Agent) sendWGPacket(pkt *WGPacket) {
	if a.tunnelDomain == "" {
		return
	}

	data, err := json.Marshal(pkt)
	if err != nil {
		return
	}

	// Look up peer's specific tunnel host for direct routing
	var targetHost string
	a.stateMu.RLock()
	if peer, ok := a.state.Peers[pkt.ToID]; ok && peer.TunnelHost != "" {
		targetHost = peer.TunnelHost
	}
	a.stateMu.RUnlock()

	// Fall back to general tunnel domain if peer has no specific host
	if targetHost == "" {
		targetHost = a.tunnelDomain
	}

	// Try HTTP POST (more reliable through CF tunnel than WebSocket)
	url := fmt.Sprintf("https://%s/api/wg/packet", targetHost)
	resp, err := httpClient.Post(url, "application/json", strings.NewReader(string(data)))
	if err != nil {
		// Log occasionally, not every packet
		return
	}
	resp.Body.Close()
}

// forwardWGPacket forwards a packet through the tunnel (when we receive a packet not meant for us).
func (a *Agent) forwardWGPacket(pkt *WGPacket) {
	a.sendWGPacket(pkt)
}

// stopUDPRelays stops all UDP relays.
func (a *Agent) stopUDPRelays() {
	a.wgRelaysMu.Lock()
	defer a.wgRelaysMu.Unlock()

	for _, relay := range a.wgRelays {
		close(relay.stopCh)
		relay.conn.Close()
	}
	a.wgRelays = make(map[string]*udpRelay)
}

// getRelayEndpoint returns the local relay endpoint for a peer in tunnel-only mode.
func (a *Agent) getRelayEndpoint(peerID string) (string, error) {
	relay, err := a.createUDPRelay(peerID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("127.0.0.1:%d", relay.localPort), nil
}

// =============================================================================
// Helpers
// =============================================================================

// getPeerAPIURL returns the URL to reach a peer's API.
// In tunnel-only mode, uses the peer's specific TunnelHost if available,
// otherwise falls back to the general Cloudflare tunnel domain.
// In direct mode, uses the peer's mesh IP.
func (a *Agent) getPeerAPIURL(peer *Peer, path string) string {
	if a.tunnelDomain != "" {
		// Tunnel-only mode: prefer peer's specific subdomain for direct routing
		if peer.TunnelHost != "" {
			return fmt.Sprintf("https://%s%s", peer.TunnelHost, path)
		}
		// Fall back to general tunnel domain (random routing)
		return fmt.Sprintf("https://%s%s", a.tunnelDomain, path)
	}
	// Direct mode: use peer's mesh IP
	return fmt.Sprintf("http://%s:%d%s", peer.MeshIP, a.apiPort, path)
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
		n := 0
		for _, c := range v {
			if c >= '0' && c <= '9' {
				n = n*10 + int(c-'0')
			}
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

	// Try to determine IP by dialing out
	c, err := net.Dial("udp", "8.8.8.8:80")
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

func genKeyPair() (priv, pub string) {
	var privKey [32]byte
	rand.Read(privKey[:])
	privKey[0] &= 248
	privKey[31] &= 127
	privKey[31] |= 64

	var pubKey [32]byte
	curve25519.ScalarBaseMult(&pubKey, &privKey)

	return base64.StdEncoding.EncodeToString(privKey[:]),
		base64.StdEncoding.EncodeToString(pubKey[:])
}

func derivePubKey(priv string) string {
	b, _ := base64.StdEncoding.DecodeString(priv)
	if len(b) != 32 {
		return ""
	}
	var privKey, pubKey [32]byte
	copy(privKey[:], b)
	curve25519.ScalarBaseMult(&pubKey, &privKey)
	return base64.StdEncoding.EncodeToString(pubKey[:])
}
