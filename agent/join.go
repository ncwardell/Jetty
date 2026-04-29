package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"strings"
	"time"
)

// =============================================================================
// Cluster Join (client side and server side)
// =============================================================================
//
// A new node joins the cluster by POSTing JETTY_SECRET + identity to an
// existing node's /api/join endpoint. The existing node validates the
// secret, allocates an entry in its peer table, and returns:
//
//   - The full peer list (so the joiner discovers everyone else).
//   - The full workload list (so the joiner can install routes).
//   - The cluster service CIDR (so the joiner agrees on the mesh range).
//   - The CF Tunnel and WARP connector tokens (so the joiner can bring up
//     the same Cloudflare assets).
//   - The cluster encryption salt (so the joiner can derive the same AES
//     key under Argon2id and decrypt env_data).
//   - The encrypted env_data map.
//
// The joiner then either uses an already-connected WARP (bootstrap-style)
// or runs configureWarpRuntime with the received connector token.
//
// Security note: the cluster secret travels in the request body. We refuse
// http:// joins to non-loopback hosts to keep the secret off plaintext
// links. Production clusters should join through a Cloudflare tunnel
// domain (https://) which terminates TLS at Cloudflare's edge.

// joinCluster is the client-side join called from Start() when JETTY_JOIN
// is set and we have no existing cluster state. Returns nil on success;
// non-nil errors abort startup.
func (a *Agent) joinCluster() error {
	// Normalize join URL - allow both base URL and full /api/join URL
	joinEndpoint := a.joinURL
	if !strings.HasSuffix(joinEndpoint, "/api/join") {
		joinEndpoint = strings.TrimSuffix(joinEndpoint, "/") + "/api/join"
	}
	// The cluster secret is sent in the request body. Refuse plaintext joins
	// unless the destination is loopback - shipping the secret over an
	// untrusted http:// link would let any on-path observer read it.
	if strings.HasPrefix(joinEndpoint, "http://") {
		host := joinEndpoint[len("http://"):]
		if i := strings.IndexAny(host, "/:"); i >= 0 {
			host = host[:i]
		}
		if host != "localhost" && host != "127.0.0.1" && host != "::1" {
			return fmt.Errorf("refusing to join over plaintext http://: cluster secret would be sent in cleartext to %s. Use https:// (e.g. via your Cloudflare tunnel domain).", host)
		}
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
		Peers          []*Peer           `json:"peers"`
		Workloads      []*Workload       `json:"workloads"`
		CFToken        string            `json:"cf_token,omitempty"`
		WarpToken      string            `json:"warp_token,omitempty"`
		ServiceCIDR    string            `json:"service_cidr,omitempty"`
		TunnelDomain   string            `json:"tunnel_domain,omitempty"`
		EnvData        map[string]string `json:"env_data,omitempty"`        // Encrypted env vars
		EncryptionSalt []byte            `json:"encryption_salt,omitempty"` // Cluster KDF salt
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
	// Adopt the cluster's encryption salt before importing env data, since
	// the env data was encrypted under the cluster's key.
	if len(result.EncryptionSalt) > 0 {
		a.state.EncryptionSalt = result.EncryptionSalt
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

// apiJoin is the server-side handler that an existing node exposes for
// new joiners. See joinCluster for the response contract.
//
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), 400)
		return
	}

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
	// Copy the cluster encryption salt so the joining node can decrypt envData.
	encryptionSalt := append([]byte(nil), a.state.EncryptionSalt...)
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
		"peers":        allPeers,
		"workloads":    allWorkloads,
		"service_cidr": a.serviceCIDR, // So joining node uses same CIDR
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

	// Include the cluster encryption salt so the joining node derives the
	// same AES key. Without this, env_data is undecryptable on the joiner.
	if len(encryptionSalt) > 0 {
		resp["encryption_salt"] = encryptionSalt
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)

	log.Printf("Peer joined: %s (%s)", peer.Name, peer.IP)
}
