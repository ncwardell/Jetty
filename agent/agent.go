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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"golang.org/x/crypto/curve25519"
)

// =============================================================================
// Types
// =============================================================================

type Peer struct {
	ID        string    `json:"id"`         // HWID
	Name      string    `json:"name"`       // Hostname
	MeshIP    string    `json:"mesh_ip"`
	Endpoint  string    `json:"endpoint"`   // public:wg_port
	PublicKey string    `json:"public_key"`
	Healthy   bool      `json:"healthy"`
	LastSeen  time.Time `json:"last_seen"`
}

type Workload struct {
	Name    string `json:"name"`     // DNS hostname
	MeshIP  string `json:"mesh_ip"`  // Unique lock
	Compose string `json:"compose"`
	Revive  bool   `json:"revive"`
	Owner   string `json:"owner"`    // Node HWID
	Version int64  `json:"version"`  // Unix timestamp
}

type Token struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type State struct {
	Peers     map[string]*Peer     `json:"peers"`     // ID -> Peer
	Workloads map[string]*Workload `json:"workloads"` // MeshIP -> Workload
	Tokens    map[string]*Token    `json:"tokens"`
	CFToken   string               `json:"cf_token,omitempty"` // Cloudflare tunnel token (shared cluster-wide)
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
	wgPrivKey string
	wgPubKey  string
	wgPort    int

	// Config
	dataDir       string
	apiPort       int
	meshCIDR      string
	joinURL       string
	joinTok       string
	clusterSecret string // Shared secret all nodes must have to join

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

	stopCh chan struct{}
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
		composeDir:    filepath.Join(dataDir, "compose"),
		hostsFile:     "/etc/hosts",
		state: &State{
			Peers:     make(map[string]*Peer),
			Workloads: make(map[string]*Workload),
			Tokens:    make(map[string]*Token),
		},
		stopCh: make(chan struct{}),
	}

	os.MkdirAll(a.composeDir, 0755)

	// Load or generate HWID
	a.hwid = a.loadOrCreateHWID()

	return a, nil
}

func (a *Agent) Start() error {
	// Init WireGuard
	if err := a.initWireGuard(); err != nil {
		return fmt.Errorf("wireguard: %w", err)
	}

	// Load state
	a.loadState()

	// Join or bootstrap
	if a.joinURL != "" {
		if err := a.joinCluster(); err != nil {
			return fmt.Errorf("join: %w", err)
		}
	}

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

	log.Printf("Jetty started: %s (%s) @ %s", a.hostname, a.hwid[:12], a.meshIP)
	return nil
}

func (a *Agent) Stop() {
	close(a.stopCh)
	a.stopCloudflared()
	a.saveState()
	exec.Command("ip", "link", "del", "jetty0").Run()
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
// WireGuard
// =============================================================================

func (a *Agent) initWireGuard() error {
	keyFile := filepath.Join(a.dataDir, "wg_private_key")

	// Load or generate keys
	if data, err := os.ReadFile(keyFile); err == nil {
		a.wgPrivKey = strings.TrimSpace(string(data))
		a.wgPubKey = derivePubKey(a.wgPrivKey)
	} else {
		a.wgPrivKey, a.wgPubKey = genKeyPair()
		os.WriteFile(keyFile, []byte(a.wgPrivKey), 0600)
	}

	// Derive mesh IP
	a.meshIP = a.deriveMeshIP(a.hwid)

	// Setup interface
	exec.Command("ip", "link", "del", "jetty0").Run()
	if err := exec.Command("ip", "link", "add", "dev", "jetty0", "type", "wireguard").Run(); err != nil {
		return err
	}

	if err := exec.Command("wg", "set", "jetty0",
		"listen-port", fmt.Sprintf("%d", a.wgPort),
		"private-key", keyFile).Run(); err != nil {
		return err
	}

	_, network, _ := net.ParseCIDR(a.meshCIDR)
	pfx, _ := network.Mask.Size()
	exec.Command("ip", "addr", "add", fmt.Sprintf("%s/%d", a.meshIP, pfx), "dev", "jetty0").Run()
	exec.Command("ip", "link", "set", "up", "dev", "jetty0").Run()

	// Enable forwarding
	os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1"), 0644)

	log.Printf("WireGuard up: %s", a.meshIP)
	return nil
}

func (a *Agent) addWGPeer(p *Peer) {
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

	if p.Endpoint != "" {
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

	// Add self
	jettyLines = append(jettyLines, fmt.Sprintf("%s\t%s", a.meshIP, a.hostname))

	// Add peers
	for _, p := range a.state.Peers {
		jettyLines = append(jettyLines, fmt.Sprintf("%s\t%s", p.MeshIP, p.Name))
	}

	// Add workloads
	for _, w := range a.state.Workloads {
		if w.MeshIP != "" && w.Name != "" {
			jettyLines = append(jettyLines, fmt.Sprintf("%s\t%s", w.MeshIP, w.Name))
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
		"token":      a.joinTok,
		"secret":     a.clusterSecret,
		"id":         a.hwid,
		"name":       a.hostname,
		"mesh_ip":    a.meshIP,
		"endpoint":   fmt.Sprintf("%s:%d", publicIP, a.wgPort),
		"public_key": a.wgPubKey,
	}

	data, _ := json.Marshal(req)
	resp, err := http.Post(a.joinURL+"/api/join", "application/json", strings.NewReader(string(data)))
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
	// Store the CF token received from the cluster
	if result.CFToken != "" {
		a.state.CFToken = result.CFToken
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

	log.Printf("Joined: %d peers, %d workloads, tunnel=%v", len(result.Peers), len(result.Workloads), result.CFToken != "")
	return nil
}

// =============================================================================
// API
// =============================================================================

func (a *Agent) runAPI() {
	r := mux.NewRouter()

	r.HandleFunc("/api/status", a.apiStatus).Methods("GET")
	r.HandleFunc("/api/workloads", a.apiListWorkloads).Methods("GET")
	r.HandleFunc("/api/workloads", a.apiCreateWorkload).Methods("POST")
	r.HandleFunc("/api/workloads/{name}", a.apiGetWorkload).Methods("GET")
	r.HandleFunc("/api/workloads/{name}", a.apiDeleteWorkload).Methods("DELETE")
	r.HandleFunc("/api/workloads/{name}/move", a.apiMoveWorkload).Methods("POST")
	r.HandleFunc("/api/workloads/{name}/logs", a.apiWorkloadLogs).Methods("GET")
	r.HandleFunc("/api/token", a.apiCreateToken).Methods("POST", "GET")
	r.HandleFunc("/api/join", a.apiJoin).Methods("POST")
	r.HandleFunc("/api/health", a.apiHealth).Methods("GET")
	r.HandleFunc("/api/sync", a.apiSync).Methods("GET")
	r.HandleFunc("/api/tunnel", a.apiGetTunnel).Methods("GET")
	r.HandleFunc("/api/tunnel", a.apiSetTunnel).Methods("POST")
	r.HandleFunc("/api/tunnel", a.apiDeleteTunnel).Methods("DELETE")
	r.HandleFunc("/api/tunnel/sync", a.apiTunnelSync).Methods("POST")
	r.HandleFunc("/api/peer-announce", a.apiPeerAnnounce).Methods("POST")

	addr := fmt.Sprintf(":%d", a.apiPort)
	log.Printf("API on %s", addr)
	http.ListenAndServe(addr, r)
}

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
		},
		"peers":     peers,
		"workloads": workloads,
		"tunnel": map[string]interface{}{
			"configured": hasTunnel,
			"running":    a.isTunnelRunning(),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

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

	// Check if mesh_ip already taken
	a.stateMu.RLock()
	existing := a.state.Workloads[wl.MeshIP]
	a.stateMu.RUnlock()

	if existing != nil && existing.Owner != a.hwid {
		http.Error(w, "mesh_ip already in use", 409)
		return
	}

	wl.Owner = a.hwid
	wl.Version = time.Now().Unix()

	// Deploy
	if err := a.deployWorkload(&wl); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Save
	a.stateMu.Lock()
	a.state.Workloads[wl.MeshIP] = &wl
	a.stateMu.Unlock()

	a.updateHosts()
	a.updateWGPeers()
	a.saveState()
	a.broadcastState()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(wl)
}

func (a *Agent) apiGetWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	a.stateMu.RLock()
	var found *Workload
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(found)
}

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
	}

	a.updateHosts()
	a.saveState()
	a.broadcastState()

	w.WriteHeader(204)
}

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
	resp, err := http.Post(fmt.Sprintf("http://%s:%d/api/workloads", target.MeshIP, a.apiPort),
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
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"moved": "ok", "to": target.Name})
}

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

func (a *Agent) apiCreateToken(w http.ResponseWriter, r *http.Request) {
	tok := &Token{
		Token:     genID() + genID(),
		ExpiresAt: time.Now().Add(24 * time.Hour),
	}

	a.stateMu.Lock()
	a.state.Tokens[tok.Token] = tok
	a.stateMu.Unlock()

	a.saveState()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tok)
}

func (a *Agent) apiJoin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token     string `json:"token"`
		Secret    string `json:"secret"`
		ID        string `json:"id"`
		Name      string `json:"name"`
		MeshIP    string `json:"mesh_ip"`
		Endpoint  string `json:"endpoint"`
		PublicKey string `json:"public_key"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	// Validate cluster secret first
	if a.clusterSecret != "" && req.Secret != a.clusterSecret {
		http.Error(w, "invalid cluster secret", 401)
		return
	}

	// Validate token
	a.stateMu.RLock()
	tok := a.state.Tokens[req.Token]
	a.stateMu.RUnlock()

	if tok == nil || time.Now().After(tok.ExpiresAt) {
		http.Error(w, "invalid token", 401)
		return
	}

	// Create peer
	peer := &Peer{
		ID:        req.ID,
		Name:      req.Name,
		MeshIP:    req.MeshIP,
		Endpoint:  req.Endpoint,
		PublicKey: req.PublicKey,
		Healthy:   true,
		LastSeen:  time.Now(),
	}

	a.stateMu.Lock()
	a.state.Peers[peer.ID] = peer

	// Build response with all peers (including self)
	allPeers := []*Peer{{
		ID:        a.hwid,
		Name:      a.hostname,
		MeshIP:    a.meshIP,
		Endpoint:  fmt.Sprintf("%s:%d", getPublicIP(), a.wgPort),
		PublicKey: a.wgPubKey,
		Healthy:   true,
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)

	log.Printf("Peer joined: %s (%s)", peer.Name, peer.MeshIP)
}

func (a *Agent) apiHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"healthy": true,
		"id":      a.hwid,
		"name":    a.hostname,
	})
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

// broadcastTunnelToken sends the CF token to all peers so they can start their tunnels.
func (a *Agent) broadcastTunnelToken(token string) {
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	a.stateMu.RUnlock()

	data, _ := json.Marshal(map[string]string{"token": token})

	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/tunnel/sync", peer.MeshIP, a.apiPort)
		resp, err := http.Post(url, "application/json", strings.NewReader(string(data)))
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
	a.composeCmd(wl.Name, "down", "-v", "--remove-orphans")

	if wl.MeshIP != "" {
		exec.Command("ip", "addr", "del", wl.MeshIP+"/32", "dev", "jetty0").Run()
	}

	dir := filepath.Join(a.composeDir, wl.Name)
	os.RemoveAll(dir)

	log.Printf("Removed: %s", wl.Name)
}

func (a *Agent) setupWorkloadIP(wl *Workload) {
	// Add IP to interface
	exec.Command("ip", "addr", "add", wl.MeshIP+"/32", "dev", "jetty0").Run()

	// Get container IP
	containerName := fmt.Sprintf("jetty_%s-%s-1", wl.Name, wl.Name)
	out, err := exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerName).Output()
	if err != nil {
		// Try alternate naming
		containerName = fmt.Sprintf("jetty_%s-1", wl.Name)
		out, _ = exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerName).Output()
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
		}
	}
}

func (a *Agent) checkPeers() {
	a.stateMu.Lock()
	defer a.stateMu.Unlock()

	for _, peer := range a.state.Peers {
		url := fmt.Sprintf("http://%s:%d/api/health", peer.MeshIP, a.apiPort)
		resp, err := http.Get(url)

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

func (a *Agent) syncWorkloads() {
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	a.stateMu.RUnlock()

	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/sync", peer.MeshIP, a.apiPort)
		resp, err := http.Get(url)
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
				a.state.Workloads[w.MeshIP] = w
			}
		}
		a.stateMu.Unlock()
	}

	a.updateHosts()
}

func (a *Agent) broadcastState() {
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		peers = append(peers, p)
	}
	a.stateMu.RUnlock()

	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/sync", peer.MeshIP, a.apiPort)
		http.Get(url) // Trigger sync
	}
}

func (a *Agent) announcePeer(newPeer *Peer) {
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		if p.ID != newPeer.ID {
			peers = append(peers, p)
		}
	}
	a.stateMu.RUnlock()

	// Include secret in announcement
	announcement := struct {
		Secret string `json:"secret"`
		Peer   *Peer  `json:"peer"`
	}{
		Secret: a.clusterSecret,
		Peer:   newPeer,
	}
	data, _ := json.Marshal(announcement)

	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/api/peer-announce", peer.MeshIP, a.apiPort)
		resp, err := http.Post(url, "application/json", strings.NewReader(string(data)))
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

func (a *Agent) shouldClaim(wl *Workload) bool {
	// Deterministic: lowest healthy node ID wins
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()

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

	os.WriteFile(filepath.Join(a.dataDir, "state.json"), data, 0644)
}

func (a *Agent) loadState() {
	data, err := os.ReadFile(filepath.Join(a.dataDir, "state.json"))
	if err != nil {
		return
	}

	a.stateMu.Lock()
	json.Unmarshal(data, a.state)
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
// Helpers
// =============================================================================

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
	c, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).IP.String()
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
