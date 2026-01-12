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
	Status string `json:"status" example:"ok"`
	ID     string `json:"id" example:"abc123def456"`
}
