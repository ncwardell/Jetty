package agent

// Swagger response types for API documentation

// StatusResponse represents the cluster status
type StatusResponse struct {
	Node      NodeInfo               `json:"node"`
	Peers     []*Peer                `json:"peers"`
	Workloads []*Workload            `json:"workloads"`
	WireGuard WireGuardStatus        `json:"wireguard"`
	Tunnel    TunnelStatus           `json:"tunnel"`
	Warp      WarpStatus             `json:"warp"`
}

// NodeInfo represents this node's information
type NodeInfo struct {
	ID        string `json:"id" example:"abc123def456"`
	Name      string `json:"name" example:"node1"`
	MeshIP    string `json:"mesh_ip" example:"10.100.42.1"`
	PublicKey string `json:"public_key" example:"..."`
	WarpIP    string `json:"warp_ip,omitempty" example:"100.96.0.5"`
}

// WireGuardStatus represents WireGuard interface status
type WireGuardStatus struct {
	Enabled bool   `json:"enabled" example:"true"`
	Mode    string `json:"mode" example:"kernel"` // "kernel" or "dummy"
}

// TunnelStatus represents Cloudflare tunnel status
type TunnelStatus struct {
	Configured bool `json:"configured" example:"true"`
	Running    bool `json:"running" example:"true"`
}

// WarpStatus represents WARP status
type WarpStatus struct {
	Enabled bool   `json:"enabled" example:"false"`
	IP      string `json:"ip,omitempty" example:"100.96.0.5"`
}

// WorkloadRequest represents a request to create a workload
type WorkloadRequest struct {
	Name      string `json:"name" example:"nginx"`
	MeshIP    string `json:"mesh_ip" example:"10.100.50.1"`
	Compose   string `json:"compose" example:"version: '3'\nservices:\n  web:\n    image: nginx"`
	Revive    bool   `json:"revive" example:"true"`
	Autostart bool   `json:"autostart" example:"true"`
}

// TokenResponse represents a join token
type TokenResponse struct {
	Token     string `json:"token" example:"abc123..."`
	ExpiresAt string `json:"expires_at" example:"2024-01-02T15:04:05Z"`
}

// JoinRequest represents a cluster join request
type JoinRequest struct {
	Token     string `json:"token"`
	Secret    string `json:"secret"`
	ID        string `json:"id"`
	Name      string `json:"name"`
	MeshIP    string `json:"mesh_ip"`
	Endpoint  string `json:"endpoint"`
	PublicKey string `json:"public_key"`
}

// JoinResponse represents a successful join response
type JoinResponse struct {
	Peers     []*Peer     `json:"peers"`
	Workloads []*Workload `json:"workloads"`
	CFToken   string      `json:"cf_token,omitempty"`
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

// HealthResponse represents a health check response
type HealthResponse struct {
	Healthy        bool     `json:"healthy" example:"true"`
	ID             string   `json:"id" example:"abc123def456"`
	Name           string   `json:"name" example:"node1"`
	MeshIP         string   `json:"mesh_ip" example:"10.100.42.1"`
	PublicIP       string   `json:"public_ip" example:"203.0.113.1"`
	Timestamp      string   `json:"timestamp" example:"2024-01-02T15:04:05Z"`
	WorkloadsLocal []string `json:"workloads_local" example:"nginx:10.100.50.1:running,redis:10.100.50.2:stopped"`
	WorkloadsTotal int      `json:"workloads_total" example:"5"`
	WireguardMode  string   `json:"wireguard_mode" example:"kernel"`
	WarpIP         string   `json:"warp_ip,omitempty" example:"100.96.0.5"`
}

// WorkloadActionResponse represents start/stop response
type WorkloadActionResponse struct {
	Status string `json:"status" example:"started"`
	Name   string `json:"name" example:"nginx"`
}

// WorkloadDetailResponse represents enriched workload details with container info
type WorkloadDetailResponse struct {
	Name       string          `json:"name" example:"nginx"`
	MeshIP     string          `json:"mesh_ip" example:"10.100.50.1"`
	Compose    string          `json:"compose"`
	Revive     bool            `json:"revive" example:"true"`
	Autostart  bool            `json:"autostart" example:"true"`
	Owner      string          `json:"owner" example:"abc123def456"`
	Version    int64           `json:"version" example:"1704067200"`
	IsLocal    bool            `json:"is_local" example:"true"`
	Containers []ContainerInfo `json:"containers,omitempty"`
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
