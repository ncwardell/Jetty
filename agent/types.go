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
	// Timing constants. The defaults below are tuned for "seamless-ish"
	// failover: typical detection sub-second via memberlist + 5s polling
	// fallback, plus a 5s failover loop. Total time-to-claim on node death
	// is usually under 10 seconds. The HealthTimeout is the hard backstop
	// when memberlist is unavailable and we're relying on HTTP gossip.
	HealthTimeout         = 15 * time.Second // Hard timeout for HTTP-gossip mode (memberlist usually fires sub-second)
	GossipInterval        = 5 * time.Second  // How often to check peers and sync state in HTTP-gossip fallback mode
	FailoverCheckInterval = 5 * time.Second  // How often to scan for orphaned workloads
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
	EncryptionSalt   []byte                      `json:"encryption_salt,omitempty"`   // Cluster-wide salt for Argon2id key derivation. Generated by bootstrap node, propagated via /api/join.
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

// tunnelConn wraps a WebSocket connection with a write mutex to prevent
// concurrent writes which would cause a panic in gorilla/websocket.
type tunnelConn struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

// WriteMessage sends a message through the WebSocket with proper synchronization.
func (tc *tunnelConn) WriteMessage(messageType int, data []byte) error {
	tc.writeMu.Lock()
	defer tc.writeMu.Unlock()
	return tc.conn.WriteMessage(messageType, data)
}

// tcpProxyConn represents an active TCP proxy connection through the tunnel.
type tcpProxyConn struct {
	conn      net.Conn    // TCP connection to the workload (nil while pending)
	wsConn    *tunnelConn // WebSocket connection back to the tunnel peer (with write mutex)
	srcIP     net.IP          // Original source IP
	srcPort   uint16          // Original source port
	dstIP     net.IP          // Destination IP (workload)
	dstPort   uint16          // Destination port
	localSeq  uint32          // Our sequence number for responses
	remoteSeq uint32          // Remote sequence number (from client)
	mu        sync.Mutex      // Protects sequence numbers and conn
	ready     chan struct{}   // Closed when connection is established (nil = already ready)
	failed    bool            // True if connection establishment failed
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

	// hostShellEnabled gates the WS /api/host/shell endpoint. Off by
	// default; set JETTY_HOST_SHELL=true to opt in. The endpoint hands
	// out an interactive shell as the agent runs (typically root with
	// privileged caps), so enabling it means anyone with JETTY_SECRET
	// has full host access on every node.
	hostShellEnabled bool

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
	warpPreexisting     bool // Was a CloudflareWARP interface already present when we started? If so, don't tear it down on Stop().

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

	// Failover tracking (prevents duplicate claims during deployment)
	failoverInProgress   map[string]time.Time // workload IP -> claim start time
	failoverInProgressMu sync.Mutex

	// Cached AES key from Argon2id(clusterSecret, encryptionSalt). Argon2id is
	// expensive (~100ms-1s per call), so we memoize the result and invalidate
	// only if the salt changes (which only happens on first bootstrap/join).
	derivedKey     []byte
	derivedKeySalt []byte
	derivedKeyMu   sync.Mutex

	// hostsBlockHash is the SHA-256 of the JETTY-managed block we last wrote
	// to /etc/hosts. updateHosts skips the write when the block hasn't changed,
	// which silences the per-gossip-tick churn on /etc/hosts.
	hostsBlockHash [32]byte

	// maroonedLogged tracks when we last warned about a marooned workload
	// (no compatible node available - e.g. arm64-only workload but only
	// amd64 nodes left). Rate-limited to avoid log spam.
	maroonedLogged   map[string]time.Time
	maroonedLoggedMu sync.Mutex

	// hostsOverrideHash is a hash of the workload-name -> mesh-IP map
	// last reflected in our owned workloads' compose override files. When
	// the map changes we rewrite the override files so future workload
	// restarts pick up new entries.
	hostsOverrideHash [32]byte

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
