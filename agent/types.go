package agent

import (
	"net"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/memberlist"
	"github.com/songgao/water"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// Timing constants
	HealthTimeout         = 45 * time.Second // Time before peer is considered unhealthy (3 missed heartbeats + buffer)
	GossipInterval        = 10 * time.Second // How often to check peers and sync state
	FailoverCheckInterval = 15 * time.Second // How often to check for orphaned workloads
	IPMonitorInterval     = 10 * time.Second // How often to check for WARP IP changes
	CPUSampleInterval     = 30 * time.Second // How often to sample CPU usage
	TombstoneMaxAge       = 1 * time.Hour    // How long to keep deletion tombstones

	// Timeouts
	DefaultHTTPTimeout   = 30 * time.Second
	PeerHTTPTimeout      = 5 * time.Second
	UnhealthyPeerTimeout = 1 * time.Second
	QuickHTTPTimeout     = 3 * time.Second

	// Network
	DefaultAPIPort     = 6880
	DefaultServiceCIDR = "10.100.0.0/16"

	// Cloudflared retry settings
	CloudflaredInitialBackoff = 5 * time.Second
	CloudflaredMaxBackoff     = 2 * time.Minute
	CloudflaredMaxFailures    = 10
	CloudflaredSuccessReset   = 30 * time.Second
)

// =============================================================================
// HTTP Clients
// =============================================================================

// Global HTTP clients with different timeouts for different use cases
var (
	httpClient          = &http.Client{Timeout: DefaultHTTPTimeout}   // General requests (30s)
	peerClient          = &http.Client{Timeout: PeerHTTPTimeout}      // Peer communication (5s)
	unhealthyPeerClient = &http.Client{Timeout: UnhealthyPeerTimeout} // Known-unhealthy peers (1s)
)

// =============================================================================
// Core Types
// =============================================================================

// Peer represents a node in the cluster
type Peer struct {
	ID       string    `json:"id"`   // HWID (hardware ID)
	Name     string    `json:"name"` // Hostname
	IP       string    `json:"ip"`   // WARP IP (100.96.x.x) - primary address for node communication
	Healthy  bool      `json:"healthy"`
	LastSeen time.Time `json:"last_seen"`
	Version  string    `json:"version"` // Agent version
	Arch     string    `json:"arch"`    // CPU architecture (amd64, arm64, etc.)
}

// Workload represents a Docker Compose application
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

// DeletedEnvKey represents a tombstone for a deleted environment variable to propagate deletions
type DeletedEnvKey struct {
	Key     string `json:"key"`     // The name of the deleted env key
	Version int64  `json:"version"` // Timestamp when deleted
}

// State represents the cluster-wide state
type State struct {
	Peers            map[string]*Peer            `json:"peers"`                       // ID -> Peer
	Workloads        map[string]*Workload        `json:"workloads"`                   // IP -> Workload
	DeletedWorkloads map[string]*DeletedWorkload `json:"deleted_workloads,omitempty"` // IP -> tombstone (for sync propagation)
	CFToken          string                      `json:"cf_token,omitempty"`          // Cloudflare tunnel token (shared cluster-wide)
	WarpToken        string                      `json:"warp_token,omitempty"`        // Cloudflare WARP connector token (shared cluster-wide)
	EnvData          map[string]string           `json:"env_data,omitempty"`          // Encrypted environment variables (key -> encrypted value)
	DeletedEnvKeys   map[string]*DeletedEnvKey   `json:"deleted_env_keys,omitempty"`  // Key -> tombstone (for sync propagation)
}

// NewState creates an empty state with initialized maps
func NewState() *State {
	return &State{
		Peers:            make(map[string]*Peer),
		Workloads:        make(map[string]*Workload),
		DeletedWorkloads: make(map[string]*DeletedWorkload),
		EnvData:          make(map[string]string),
		DeletedEnvKeys:   make(map[string]*DeletedEnvKey),
	}
}

// =============================================================================
// TCP Proxy Types (for userspace tunneling)
// =============================================================================

// tcpProxyConn represents an active TCP proxy connection through the tunnel.
type tcpProxyConn struct {
	conn      net.Conn        // TCP connection to the workload
	wsConn    *websocket.Conn // WebSocket connection back to the tunnel peer
	srcIP     net.IP          // Original source IP
	srcPort   uint16          // Original source port
	dstIP     net.IP          // Destination IP (workload)
	dstPort   uint16          // Destination port
	localSeq  uint32          // Our sequence number for responses
	remoteSeq uint32          // Remote sequence number (from client)
	mu        sync.Mutex      // Protects sequence numbers
}

// =============================================================================
// Agent Type
// =============================================================================

// Agent is the main Jetty agent that manages the cluster node
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
	startTime           time.Time // When Jetty started (for uptime tracking)
	lastHeartbeatErrLog time.Time // Last time we logged a heartbeat error (to reduce spam)
	publicIP            string    // Cached public IP (set at startup to avoid slow lookups)
	cachedCPUPercent    float64   // Cached CPU usage (updated by background goroutine)
	cachedCPUMu         sync.RWMutex

	// Route management
	workloadRoutes   map[string]string // workload IP -> owner WARP IP (for remote workloads)
	workloadRoutesMu sync.Mutex

	// Tunnel support for cross-node routing
	tunnelMode        string          // "ipip", "gre", or "" (none available)
	ipipWarnedPeers   map[string]bool // Peers we've already warned about tunnel failure
	ipipWarnedPeersMu sync.Mutex

	// Userspace tunnel (fallback when kernel IPIP/GRE not available)
	tunDevice    *water.Interface // TUN device for capturing workload traffic
	tunPeerConns sync.Map         // peerID -> *websocket.Conn (outgoing connections to peers)
	tunPeerIPs   sync.Map         // peerID -> string (peer WARP IP for tunnel)
	tunConnMu    sync.Mutex       // Protects WebSocket connection establishment
	tunTCPConns  sync.Map         // flowKey -> *tcpProxyConn (active TCP proxy connections)

	// Memberlist (cluster membership and failure detection)
	memberlist *memberlist.Memberlist
	mlDelegate *jettyDelegate

	stopCh chan struct{}
}

// =============================================================================
// Sync Response Types
// =============================================================================

// SyncResponse represents the response from a /api/sync call
type SyncResponse struct {
	Workloads        []*Workload        `json:"workloads"`
	DeletedWorkloads []*DeletedWorkload `json:"deleted_workloads,omitempty"`
	EnvData          map[string]string  `json:"env_data,omitempty"`
	DeletedEnvKeys   []*DeletedEnvKey   `json:"deleted_env_keys,omitempty"`
}
