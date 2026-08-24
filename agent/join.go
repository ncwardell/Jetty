package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"
)

// =============================================================================
// Cluster Join (client side and server side)
// =============================================================================
//
// A new node joins the cluster by POSTing a one-time JoinToken (minted
// by an admin via POST /api/tokens) plus its own identity + a freshly
// generated SelfAPIKey to an existing node's /api/join endpoint. The
// existing node validates and burns the token, registers the joining
// peer's APIKey in its peer table, and returns:
//
//   - The full peer list with each peer's APIKey (so the joiner can
//     authenticate inbound requests from any peer).
//   - The full workload list (so the joiner can install routes).
//   - The cluster service CIDR (so the joiner agrees on the mesh range).
//   - The CF Tunnel and WARP connector tokens (so the joiner can bring up
//     the same Cloudflare assets).
//   - The cluster AdminKey (so the dashboard works on the joining node
//     without needing JETTY_SECRET in the joining node's env).
//   - The cluster EncryptionKey (so the joiner can decrypt env_data).
//   - The encrypted env_data map.
//
// The joiner then either uses an already-connected WARP (bootstrap-style)
// or runs configureWarpRuntime with the received connector token.
//
// Security note: the JoinToken travels in the request body. Production
// clusters should join through a Cloudflare tunnel domain (https://)
// which terminates TLS at Cloudflare's edge. We refuse plaintext
// http:// joins to non-loopback hosts to keep the token off untrusted
// links.

// joinCluster is the client-side join called from Start() when JETTY_JOIN
// is set and we have no existing cluster state. Requires JETTY_JOIN_TOKEN.
// Returns nil on success; non-nil errors abort startup.
func (a *Agent) joinCluster() error {
	if a.joinToken == "" {
		return fmt.Errorf("JETTY_JOIN_TOKEN is required to join (mint one with POST /api/tokens on an existing cluster node)")
	}

	// Normalize join URL - allow both base URL and full /api/join URL
	joinEndpoint := a.joinURL
	if !strings.HasSuffix(joinEndpoint, "/api/join") {
		joinEndpoint = strings.TrimSuffix(joinEndpoint, "/") + "/api/join"
	}
	if strings.HasPrefix(joinEndpoint, "http://") {
		host := joinEndpoint[len("http://"):]
		if i := strings.IndexAny(host, "/:"); i >= 0 {
			host = host[:i]
		}
		if host != "localhost" && host != "127.0.0.1" && host != "::1" {
			return fmt.Errorf("refusing to join over plaintext http://: the join token would be sent in cleartext to %s. Use https:// (e.g. via your Cloudflare tunnel domain).", host)
		}
	}
	logInfof("Joining cluster via %s", joinEndpoint)

	// Generate our own SelfAPIKey before contacting the cluster. The
	// joiner picks this; the cluster registers it as our Peer.APIKey.
	a.stateMu.Lock()
	if a.state.SelfAPIKey == "" {
		key, err := generateAPIKey()
		if err != nil {
			a.stateMu.Unlock()
			return fmt.Errorf("generate self api key: %w", err)
		}
		a.state.SelfAPIKey = key
	}
	selfKey := a.state.SelfAPIKey
	a.stateMu.Unlock()

	req := map[string]string{
		"join_token": a.joinToken,
		"id":         a.hwid,
		"name":       a.hostname,
		"ip":         a.ip,
		"version":    Version,
		"arch":       runtime.GOARCH,
		"api_key":    selfKey,
	}

	data, _ := json.Marshal(req)
	resp, err := httpClient.Post(joinEndpoint, "application/json", strings.NewReader(string(data)))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return fmt.Errorf("join failed (%d): %s", resp.StatusCode, body)
	}

	var result struct {
		Peers         []peerWire        `json:"peers"`
		Workloads     []*Workload       `json:"workloads"`
		CFToken       string            `json:"cf_token,omitempty"`
		WarpToken     string            `json:"warp_token,omitempty"`
		ServiceCIDR   string            `json:"service_cidr,omitempty"`
		TunnelDomain  string            `json:"tunnel_domain,omitempty"`
		EnvData       map[string]string `json:"env_data,omitempty"`
		AdminKey      string            `json:"admin_key,omitempty"`
		EncryptionKey []byte            `json:"encryption_key,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		resp.Body.Close()
		return fmt.Errorf("decode join response: %w", err)
	}
	resp.Body.Close()

	if result.ServiceCIDR != "" && result.ServiceCIDR != a.serviceCIDR {
		logInfof("Adopting cluster service CIDR: %s", result.ServiceCIDR)
		a.serviceCIDR = result.ServiceCIDR
	}
	if result.TunnelDomain != "" && a.tunnelDomain == "" {
		a.tunnelDomain = result.TunnelDomain
		logInfof("Adopting cluster tunnel domain: %s", a.tunnelDomain)
	}

	a.stateMu.Lock()
	for _, w := range result.Peers {
		p := w.toPeer()
		if !validIngestedPeer(p) {
			continue
		}
		a.state.Peers[p.ID] = p
	}
	for _, w := range result.Workloads {
		if !validIngestedWorkload(w) {
			continue
		}
		a.state.Workloads[w.IP] = w
	}
	if result.CFToken != "" {
		a.state.CFToken = result.CFToken
	}
	// Adopt the cluster's WARP token only if this node doesn't already
	// have its own. Since Cloudflare's 2026-04 Mesh migration, a token
	// shared across machines registers them as active-passive replicas
	// of ONE mesh node (passive replicas drop all traffic) - so each
	// node should register with its own per-node token
	// (JETTY_WARP_CONNECTOR_TOKEN). The shared token remains a fallback
	// for clusters that predate per-node provisioning.
	if result.WarpToken != "" && a.state.WarpToken == "" {
		// Warn, not info. This node will join, gossip, run workloads and
		// report healthy while Cloudflare Mesh treats it as a passive
		// replica of an identity another machine already owns - and passive
		// replicas drop all traffic. A silent degradation that presents as
		// success is exactly what a warning level is for.
		logWarnf("WARP token: no per-node token set, adopting the cluster-shared token. " +
			"Cloudflare Mesh registers shared-token nodes as active-passive replicas of ONE " +
			"identity and passive replicas DROP ALL TRAFFIC. Set JETTY_WARP_CONNECTOR_TOKEN " +
			"to a per-node token (Zero Trust > Networking > Mesh > Add a node) and restart.")
		a.state.WarpToken = result.WarpToken
	}
	ownWarpToken := a.state.WarpToken
	if result.AdminKey != "" {
		a.state.AdminKey = result.AdminKey
	}
	if len(result.EncryptionKey) == encryptionKeySize {
		a.state.EncryptionKey = result.EncryptionKey
	}
	if len(result.EnvData) > 0 {
		for k, v := range result.EnvData {
			a.state.EnvData[k] = v
		}
	}
	a.stateMu.Unlock()

	a.saveState()

	// Configure WARP at runtime if we have a token and WARP isn't
	// connected yet. ownWarpToken prefers this node's per-node token
	// over the cluster-shared one from the join response.
	if ownWarpToken != "" && a.ip == "" {
		if err := a.configureWarpRuntime(ownWarpToken); err != nil {
			logWarnf("failed to configure WARP at runtime: %v", err)
		}
	}

	// Start cloudflared if we received a token
	if result.CFToken != "" {
		if err := a.startCloudflared(); err != nil {
			logWarnf("failed to start cloudflared: %v", err)
		}
	}

	logInfof("Joined: %d peers, %d workloads, tunnel=%v, warp=%v",
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
		JoinToken string `json:"join_token"`
		ID        string `json:"id"`
		Name      string `json:"name"`
		IP        string `json:"ip"`
		Version   string `json:"version"`
		Arch      string `json:"arch"`
		APIKey    string `json:"api_key"` // Joiner-generated; stored as Peer.APIKey
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	// Validate joiner identity before consuming the token. Cheap and
	// avoids burning a token on a malformed request.
	if !validNamePattern.MatchString(req.ID) {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	if req.Name == "" || !validPeerNamePattern.MatchString(req.Name) {
		writeError(w, http.StatusBadRequest, "invalid name")
		return
	}
	if req.APIKey == "" || len(req.APIKey) < 16 {
		writeError(w, http.StatusBadRequest, "invalid api_key (must be present and >=16 chars)")
		return
	}

	// Consume the one-time token. consumeJoinToken takes stateMu so we
	// can't hold an RLock around it.
	tok, err := a.consumeJoinToken(req.JoinToken, req.ID)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	logInfof("token: consumed %s for joiner %s (note=%q)", redactTokenID(tok.ID), shortID(req.ID, 12), tok.Note)

	// Check for mesh IP collision before creating peer
	a.stateMu.RLock()
	// Check against our own IP
	if req.IP == a.ip {
		a.stateMu.RUnlock()
		writeError(w, http.StatusConflict, "mesh_ip collision with existing node")
		return
	}
	// Check against existing peers
	for _, p := range a.state.Peers {
		if p.IP == req.IP && p.ID != req.ID {
			a.stateMu.RUnlock()
			writeError(w, http.StatusConflict, "mesh_ip collision with existing node")
			return
		}
	}
	// Check against workloads
	if _, exists := a.state.Workloads[req.IP]; exists {
		a.stateMu.RUnlock()
		writeError(w, http.StatusConflict, "mesh_ip collision with existing workload")
		return
	}
	a.stateMu.RUnlock()

	// Create peer. Store the joiner-supplied APIKey so future inbound
	// requests from this peer authenticate via apiKeyMiddleware.
	// Version/arch are joiner-supplied display metadata - blank unsafe
	// values (the dashboard interpolates them into HTML).
	sanitizePeerMeta(req.ID, &req.Version, &req.Arch)
	peer := &Peer{
		ID:       req.ID,
		Name:     req.Name,
		IP:       req.IP,
		Healthy:  true,
		LastSeen: time.Now(),
		Version:  req.Version,
		Arch:     req.Arch,
		APIKey:   req.APIKey,
	}

	a.stateMu.Lock()
	a.state.Peers[peer.ID] = peer

	// Build response with all peers (including self). Self carries
	// SelfAPIKey so the joiner can call us back with auth that matches
	// in our middleware (it'll match against state.SelfAPIKey).
	// We use peerWire (not Peer) so APIKey is explicitly serialized -
	// Peer's json:"-" tag suppresses APIKey on all the other handlers
	// that return Peer objects.
	allPeers := []peerWire{{
		ID:      a.hwid,
		Name:    a.hostname,
		IP:      a.ip,
		Healthy: true,
		Version: Version,
		Arch:    runtime.GOARCH,
		APIKey:  a.state.SelfAPIKey,
	}}
	for _, p := range a.state.Peers {
		if p.ID != req.ID {
			allPeers = append(allPeers, peerToWire(p))
		}
	}

	allWorkloads := make([]*Workload, 0, len(a.state.Workloads))
	for _, w := range a.state.Workloads {
		allWorkloads = append(allWorkloads, w)
	}
	cfToken := a.state.CFToken
	warpToken := a.state.WarpToken
	// Copy env data (already encrypted, safe to share with the joiner -
	// they receive the matching EncryptionKey below).
	envData := make(map[string]string)
	for k, v := range a.state.EnvData {
		envData[k] = v
	}
	adminKey := a.state.AdminKey
	encryptionKey := append([]byte(nil), a.state.EncryptionKey...)
	a.stateMu.Unlock()

	a.updateHosts()
	a.saveState()

	// Create IPIP tunnel to this peer (for receiving their traffic)
	if peer.IP != "" {
		if err := a.ensurePeerTunnel(peer.ID, peer.IP); err != nil {
			logWarnf("failed to create tunnel to %s: %v", peer.Name, err)
		}
	}

	// Notify other peers
	goSafe("announcePeer", func() { a.announcePeer(peer) })

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

	// Hand the joiner the cluster's AdminKey + EncryptionKey so it can
	// (a) accept dashboard auth without needing JETTY_SECRET set in its
	// env and (b) decrypt env_data. Both travel over TLS via the
	// Cloudflare tunnel; they never appear on a plaintext wire.
	if adminKey != "" {
		resp["admin_key"] = adminKey
	}
	if len(encryptionKey) == encryptionKeySize {
		resp["encryption_key"] = encryptionKey
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)

	logInfof("Peer joined: %s (%s)", peer.Name, peer.IP)
}
