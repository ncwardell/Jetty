package agent

// Swagger response types for API documentation

// StatusResponse represents the cluster status
type StatusResponse struct {
	Node        NodeInfo     `json:"node"`
	Peers       []*Peer      `json:"peers"`
	Workloads   []*Workload  `json:"workloads"`
	ServiceCIDR string       `json:"service_cidr" example:"10.100.0.0/16"`
	Tunnel      TunnelStatus `json:"tunnel"`
	Warp        WarpStatus   `json:"warp"`
}

// WarpStatus describes whether this node has a live WARP IP. The
// dashboard's "WARP Mesh" indicator reads `enabled`; the IP is shown
// alongside when set.
type WarpStatus struct {
	Enabled bool   `json:"enabled" example:"true"`
	IP      string `json:"ip" example:"100.96.0.20"`
}

// NodeInfo represents this node's information
type NodeInfo struct {
	ID   string `json:"id" example:"abc123def456"`
	Name string `json:"name" example:"node1"`
	IP   string `json:"ip" example:"100.96.0.1"`
	Arch string `json:"arch" example:"amd64"`
}

// TunnelStatus represents Cloudflare tunnel status
type TunnelStatus struct {
	Configured bool `json:"configured" example:"true"`
	Running    bool `json:"running" example:"true"`
}

// OwnerInfo represents workload owner details
type OwnerInfo struct {
	ID   string `json:"id" example:"abc123def456"`
	Name string `json:"name" example:"node1"`
	IP   string `json:"ip" example:"100.96.0.1"`
}

// WorkloadRequest represents a request to create a workload
type WorkloadRequest struct {
	Name         string   `json:"name" example:"nginx"`
	IP           string   `json:"ip,omitempty" example:"10.100.0.50"`
	Compose      string   `json:"compose" example:"services:\n  web:\n    image: nginx"`
	ComposeAmd64 string   `json:"compose_amd64,omitempty" example:"services:\n  web:\n    image: nginx:amd64"`
	ComposeArm64 string   `json:"compose_arm64,omitempty" example:"services:\n  web:\n    image: nginx:arm64"`
	Revive       bool     `json:"revive" example:"true"`
	Autostart    bool     `json:"autostart" example:"true"`
	AllowedNodes []string `json:"allowed_nodes,omitempty" example:"node1,node2"`
}

// WorkloadUpdateRequest represents a request to update a workload
type WorkloadUpdateRequest struct {
	Compose      *string   `json:"compose,omitempty"`
	ComposeAmd64 *string   `json:"compose_amd64,omitempty"`
	ComposeArm64 *string   `json:"compose_arm64,omitempty"`
	IP           *string   `json:"ip,omitempty"`
	Revive       *bool     `json:"revive,omitempty"`
	Autostart    *bool     `json:"autostart,omitempty"`
	AllowedNodes *[]string `json:"allowed_nodes,omitempty"`
}

// WorkloadResponse represents a workload with enriched owner info
type WorkloadResponse struct {
	Name         string    `json:"name" example:"nginx"`
	IP           string    `json:"ip" example:"10.100.0.50"`
	Compose      string    `json:"compose"`
	Revive       bool      `json:"revive" example:"true"`
	Autostart    bool      `json:"autostart" example:"true"`
	AllowedNodes []string  `json:"allowed_nodes,omitempty"`
	Owner        OwnerInfo `json:"owner"`
	Version      int64     `json:"version" example:"1704067200"`
	Status       string    `json:"status" example:"running"`
}

// WorkloadDetailResponse represents enriched workload details with container info
type WorkloadDetailResponse struct {
	Name         string          `json:"name" example:"nginx"`
	IP           string          `json:"ip" example:"10.100.0.50"`
	Compose      string          `json:"compose"`
	Revive       bool            `json:"revive" example:"true"`
	Autostart    bool            `json:"autostart" example:"true"`
	AllowedNodes []string        `json:"allowed_nodes,omitempty"`
	Owner        OwnerInfo       `json:"owner"`
	Version      int64           `json:"version" example:"1704067200"`
	IsLocal      bool            `json:"is_local" example:"true"`
	Containers   []ContainerInfo `json:"containers,omitempty"`
}

// ContainerInfo represents Docker container runtime information
type ContainerInfo struct {
	ID            string   `json:"id" example:"abc123def456"`
	Name          string   `json:"name" example:"jetty_nginx-web-1"`
	Image         string   `json:"image" example:"nginx:alpine"`
	Status        string   `json:"status" example:"running"`
	Running       bool     `json:"running" example:"true"`
	StartedAt     string   `json:"started_at,omitempty" example:"2024-01-02T15:04:05Z"`
	Uptime        string   `json:"uptime,omitempty" example:"2h30m15s"`
	FinishedAt    string   `json:"finished_at,omitempty" example:"2024-01-02T15:04:05Z"`
	ExitCode      int      `json:"exit_code,omitempty" example:"0"`
	Health        string   `json:"health,omitempty" example:"healthy"`
	Networks      []string `json:"networks,omitempty" example:"bridge:172.17.0.2"`
	Ports         []string `json:"ports,omitempty" example:"80/tcp"`
	CPUPercent    string   `json:"cpu_percent,omitempty" example:"0.50%"`
	MemoryUsage   string   `json:"memory_usage,omitempty" example:"15MiB / 1.94GiB"`
	MemoryPercent string   `json:"memory_percent,omitempty" example:"0.75%"`
}

// JoinRequest represents a cluster join request
type JoinRequest struct {
	Secret  string `json:"secret"`
	ID      string `json:"id"`
	Name    string `json:"name"`
	IP      string `json:"ip" example:"100.96.0.5"`
	Version string `json:"version" example:"2.0.0"`
	Arch    string `json:"arch" example:"amd64"`
}

// JoinResponse represents a successful join response
type JoinResponse struct {
	Peers        []*Peer     `json:"peers"`
	Workloads    []*Workload `json:"workloads"`
	CFToken      string      `json:"cf_token,omitempty"`
	WarpToken    string      `json:"warp_token,omitempty"`
	ServiceCIDR  string      `json:"service_cidr" example:"10.100.0.0/16"`
	TunnelDomain string      `json:"tunnel_domain,omitempty"`
}

// TunnelRequest represents a tunnel configuration request
type TunnelRequest struct {
	Token string `json:"token" example:"eyJhIjoiNjA2..."`
}

// MoveRequest represents a workload move request
type MoveRequest struct {
	To string `json:"to" example:"node2"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error" example:"workload not found"`
}

// HealthResponse represents unified health check response (local or cluster-wide)
type HealthResponse struct {
	// Local health fields (when ?node=local or single node response)
	Status         string                 `json:"status,omitempty" example:"healthy"`
	ID             string                 `json:"id,omitempty" example:"abc123def456"`
	Name           string                 `json:"name,omitempty" example:"node1"`
	IP             string                 `json:"ip,omitempty" example:"100.96.0.1"`
	PublicIP       string                 `json:"public_ip,omitempty" example:"203.0.113.1"`
	Timestamp      string                 `json:"timestamp" example:"2024-01-02T15:04:05Z"`
	WorkloadsLocal []string               `json:"workloads_local,omitempty" example:"nginx:10.100.0.50:running"`
	WorkloadsTotal int                    `json:"workloads_total,omitempty" example:"5"`
	System         map[string]interface{} `json:"system,omitempty"`

	// Cluster health fields (when no filter or multiple nodes)
	ClusterStatus  string             `json:"cluster_status,omitempty" example:"healthy"`
	TotalNodes     int                `json:"total_nodes,omitempty" example:"3"`
	HealthyNodes   int                `json:"healthy_nodes,omitempty" example:"3"`
	TotalWorkloads int                `json:"total_workloads,omitempty" example:"10"`
	Nodes          []NodeHealthStatus `json:"nodes,omitempty"`
}

// NodeHealthStatus represents health status for a single node in cluster view
type NodeHealthStatus struct {
	ID        string                 `json:"id" example:"abc123def456"`
	Name      string                 `json:"name" example:"node1"`
	IP        string                 `json:"ip" example:"100.96.0.1"`
	PublicIP  string                 `json:"public_ip,omitempty" example:"203.0.113.1"`
	Healthy   bool                   `json:"healthy" example:"true"`
	Status    string                 `json:"status" example:"healthy"`
	Workloads []string               `json:"workloads,omitempty"`
	System    map[string]interface{} `json:"system,omitempty"`
	Error     string                 `json:"error,omitempty"`
}

// NodeResponse represents a node in the cluster
type NodeResponse struct {
	ID       string `json:"id" example:"abc123def456"`
	Name     string `json:"name" example:"node1"`
	IP       string `json:"ip" example:"100.96.0.1"`
	Healthy  bool   `json:"healthy" example:"true"`
	LastSeen string `json:"last_seen" example:"2024-01-02T15:04:05Z"`
	IsSelf   bool   `json:"is_self" example:"false"`
	Version  string `json:"version" example:"2.0.0"`
	Arch     string `json:"arch" example:"amd64"`
}

// NodeUpdateRequest represents a request to update a node
type NodeUpdateRequest struct {
	Image string `json:"image" example:"ghcr.io/ncwardell/jetty:2.1.0"`
}

// NodeUpdateResponse represents the response from updating a node
type NodeUpdateResponse struct {
	Status  string `json:"status" example:"updating"`
	Message string `json:"message" example:"pulling image and restarting"`
	Image   string `json:"image" example:"ghcr.io/ncwardell/jetty:2.1.0"`
}

// RemoveNodeResponse represents the response when removing a node
type RemoveNodeResponse struct {
	Removed           string   `json:"removed" example:"node2"`
	ID                string   `json:"id" example:"abc123def456"`
	OrphanedWorkloads []string `json:"orphaned_workloads" example:"nginx,redis"`
	Message           string   `json:"message" example:"node removed; orphaned workloads will failover if revive is enabled"`
}

// WorkloadActionResponse represents start/stop response
type WorkloadActionResponse struct {
	Status string `json:"status" example:"started"`
	Name   string `json:"name" example:"nginx"`
}

// WorkloadMoveResponse represents move response
type WorkloadMoveResponse struct {
	Moved string `json:"moved" example:"ok"`
	To    string `json:"to" example:"node2"`
}

// WorkloadUpdateResponse represents update response
type WorkloadUpdateResponse struct {
	Name         string    `json:"name" example:"nginx"`
	IP           string    `json:"ip" example:"10.100.0.50"`
	Compose      string    `json:"compose"`
	Revive       bool      `json:"revive" example:"true"`
	Autostart    bool      `json:"autostart" example:"true"`
	AllowedNodes []string  `json:"allowed_nodes,omitempty"`
	Owner        OwnerInfo `json:"owner"`
	Version      int64     `json:"version" example:"1704067200"`
	Redeployed   bool      `json:"redeployed" example:"true"`
}

// EnvListResponse represents the list of environment variable keys
type EnvListResponse struct {
	Keys  []string `json:"keys" example:"DB_HOST,DB_PASSWORD,API_KEY"`
	Count int      `json:"count" example:"3"`
}

// EnvSetRequest represents a request to set environment variables
type EnvSetRequest struct {
	Env map[string]string `json:"env" example:"DB_HOST:localhost,DB_PASSWORD:secret123"`
}

// EnvSetResponse represents the response after setting environment variables
type EnvSetResponse struct {
	Added   []string `json:"added" example:"DB_HOST,DB_PASSWORD"`
	Updated []string `json:"updated" example:"API_KEY"`
}

// EnvGetResponse represents a single environment variable
type EnvGetResponse struct {
	Key   string `json:"key" example:"DB_PASSWORD"`
	Value string `json:"value" example:"secret123"`
}

// CreateTokenRequest is the body of POST /api/tokens.
type CreateTokenRequest struct {
	TTLSeconds int    `json:"ttl_seconds,omitempty" example:"3600"`
	Note       string `json:"note,omitempty" example:"for arnold's laptop"`
}

// CreateTokenResponse is what POST /api/tokens returns. The token
// value is shown exactly once - operator must save it from the
// response or mint a new one.
type CreateTokenResponse struct {
	Token     string `json:"token" example:"H8mC...43-char-base64..."`
	ExpiresAt string `json:"expires_at" example:"2026-04-30T03:00:00Z"`
	Note      string `json:"note,omitempty" example:"for arnold's laptop"`
}

// ListTokensResponse wraps the list returned by GET /api/tokens.
// Pending token IDs are redacted to an 8-char prefix.
type ListTokensResponse struct {
	Tokens []JoinToken `json:"tokens"`
}

// RevokeTokenResponse is the body of DELETE /api/tokens/{id}.
type RevokeTokenResponse struct {
	Revoked bool `json:"revoked"`
}

// BulkRequest is the body of POST /api/workloads/bulk.
type BulkRequest struct {
	Tag    string   `json:"tag,omitempty" example:"prod"`
	Names  []string `json:"names,omitempty" example:"nginx,redis"`
	All    bool     `json:"all,omitempty"`
	Action string   `json:"action" example:"stop"`
}

// BulkResponse is what POST /api/workloads/bulk returns.
type BulkResponse struct {
	Selected []string                     `json:"selected"`
	Results  map[string]BulkResponseEntry `json:"results"`
}

// BulkResponseEntry is the per-workload result inside BulkResponse.
type BulkResponseEntry struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// PortableExport is what GET /api/workloads/export returns.
type PortableExport struct {
	Version           string             `json:"version" example:"1"`
	ExportedAt        string             `json:"exported_at" example:"2026-04-30T03:00:00Z"`
	Workloads         []PortableWorkload `json:"workloads"`
	ReferencedEnvKeys []string           `json:"referenced_env_keys,omitempty" example:"DATABASE_URL,API_KEY"`
}

// PortableWorkload mirrors the export entry. Owner+Version are
// dropped because they're runtime/cluster-specific.
type PortableWorkload struct {
	Name         string   `json:"name" example:"nginx"`
	IP           string   `json:"ip,omitempty" example:"10.100.0.10"`
	Compose      string   `json:"compose,omitempty"`
	ComposeAmd64 string   `json:"compose_amd64,omitempty"`
	ComposeArm64 string   `json:"compose_arm64,omitempty"`
	Revive       bool     `json:"revive"`
	Autostart    bool     `json:"autostart"`
	AllowedNodes []string `json:"allowed_nodes,omitempty"`
	Tags         []string `json:"tags,omitempty" example:"prod,web"`
}

// ImportRequest is the body of POST /api/workloads/import.
type ImportRequest struct {
	Mode        string         `json:"mode,omitempty" example:"skip"`
	ReassignIPs *bool          `json:"reassign_ips,omitempty"`
	Payload     PortableExport `json:"payload"`
}

// ImportReport is what POST /api/workloads/import returns.
type ImportReport struct {
	Mode    string              `json:"mode"`
	Total   int                 `json:"total"`
	Created int                 `json:"created"`
	Skipped int                 `json:"skipped"`
	Errors  int                 `json:"errors"`
	Entries []ImportReportEntry `json:"entries"`
}

// ImportReportEntry describes one workload's outcome.
type ImportReportEntry struct {
	Name       string `json:"name"`
	Status     string `json:"status" example:"imported"`
	Detail     string `json:"detail,omitempty"`
	OriginalIP string `json:"original_ip,omitempty"`
	AssignedIP string `json:"assigned_ip,omitempty"`
}

// RotateAdminKeyRequest is the body of POST /api/admin-key/rotate.
// Both fields are optional - empty body means "server picks a fresh
// 256-bit random key".
type RotateAdminKeyRequest struct {
	NewKey string `json:"new_key,omitempty"`
}

// RotateAdminKeyResponse is what POST /api/admin-key/rotate returns.
type RotateAdminKeyResponse struct {
	Status string `json:"status" example:"rotated"`
	NewKey string `json:"new_key"`
	Hint   string `json:"hint,omitempty"`
}

// RotatePeerKeyResponse is what POST /api/peers/{id}/rotate-key returns.
type RotatePeerKeyResponse struct {
	Status string `json:"status" example:"rotated"`
	PeerID string `json:"peer_id"`
	Hint   string `json:"hint,omitempty"`
}
