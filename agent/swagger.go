package agent

// Swagger response types for API documentation

// StatusResponse represents the cluster status
type StatusResponse struct {
	Node        NodeInfo     `json:"node"`
	Peers       []*Peer      `json:"peers"`
	Workloads   []*Workload  `json:"workloads"`
	ServiceCIDR string       `json:"service_cidr" example:"10.100.0.0/16"`
	Tunnel      TunnelStatus `json:"tunnel"`
}

// NodeInfo represents this node's information
type NodeInfo struct {
	ID   string `json:"id" example:"abc123def456"`
	Name string `json:"name" example:"node1"`
	IP   string `json:"ip" example:"100.96.0.1"`
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
	Revive       bool     `json:"revive" example:"true"`
	Autostart    bool     `json:"autostart" example:"true"`
	AllowedNodes []string `json:"allowed_nodes,omitempty" example:"node1,node2"`
}

// WorkloadUpdateRequest represents a request to update a workload
type WorkloadUpdateRequest struct {
	Compose      *string   `json:"compose,omitempty"`
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
