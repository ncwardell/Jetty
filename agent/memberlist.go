package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// =============================================================================
// Memberlist Integration
// =============================================================================

// MemberlistPort is the port used for memberlist gossip (UDP and TCP)
const MemberlistPort = 6881

// NodeMeta contains metadata about a node that is broadcast via memberlist
type NodeMeta struct {
	ID      string `json:"id"`      // HWID
	Name    string `json:"name"`    // Hostname
	IP      string `json:"ip"`      // WARP IP
	Version string `json:"version"` // Agent version
	Arch    string `json:"arch"`    // CPU architecture
	APIPort int    `json:"api_port"`
}

// BroadcastMessage represents a message broadcast to all nodes
type BroadcastMessage struct {
	Type    string          `json:"type"`    // "workload_update", "workload_delete", "env_update", "env_delete"
	Payload json.RawMessage `json:"payload"` // Type-specific payload
}

// jettyDelegate implements memberlist.Delegate for Jetty
type jettyDelegate struct {
	agent      *Agent
	broadcasts *memberlist.TransmitLimitedQueue
	meta       []byte
	metaMu     sync.RWMutex
}

// jettyEventDelegate implements memberlist.EventDelegate for node join/leave events
type jettyEventDelegate struct {
	agent *Agent
}

// =============================================================================
// Delegate Implementation
// =============================================================================

// NodeMeta returns metadata about this node (called by memberlist)
func (d *jettyDelegate) NodeMeta(limit int) []byte {
	d.metaMu.RLock()
	defer d.metaMu.RUnlock()
	if len(d.meta) > limit {
		log.Printf("Warning: node meta exceeds limit (%d > %d)", len(d.meta), limit)
		return d.meta[:limit]
	}
	return d.meta
}

// NotifyMsg is called when a broadcast message is received
func (d *jettyDelegate) NotifyMsg(msg []byte) {
	if len(msg) == 0 {
		return
	}

	var broadcast BroadcastMessage
	if err := json.Unmarshal(msg, &broadcast); err != nil {
		log.Printf("Memberlist: failed to unmarshal broadcast: %v", err)
		return
	}

	d.agent.handleBroadcast(&broadcast)
}

// GetBroadcasts returns any queued broadcasts (called by memberlist during protocol)
func (d *jettyDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.broadcasts.GetBroadcasts(overhead, limit)
}

// LocalState is called for full state sync when a new node joins
// We return serialized state that MergeRemoteState will process
func (d *jettyDelegate) LocalState(join bool) []byte {
	d.agent.stateMu.RLock()
	defer d.agent.stateMu.RUnlock()

	// Build sync response with current state
	syncResp := &SyncResponse{
		Workloads:        make([]*Workload, 0, len(d.agent.state.Workloads)),
		DeletedWorkloads: make([]*DeletedWorkload, 0, len(d.agent.state.DeletedWorkloads)),
		EnvData:          d.agent.state.EnvData,
		DeletedEnvKeys:   make([]*DeletedEnvKey, 0, len(d.agent.state.DeletedEnvKeys)),
	}

	for _, w := range d.agent.state.Workloads {
		syncResp.Workloads = append(syncResp.Workloads, w)
	}
	for _, dw := range d.agent.state.DeletedWorkloads {
		syncResp.DeletedWorkloads = append(syncResp.DeletedWorkloads, dw)
	}
	for _, dek := range d.agent.state.DeletedEnvKeys {
		syncResp.DeletedEnvKeys = append(syncResp.DeletedEnvKeys, dek)
	}

	data, err := json.Marshal(syncResp)
	if err != nil {
		log.Printf("Memberlist: failed to marshal local state: %v", err)
		return nil
	}
	return data
}

// MergeRemoteState is called to merge state from another node
func (d *jettyDelegate) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}

	var syncResp SyncResponse
	if err := json.Unmarshal(buf, &syncResp); err != nil {
		log.Printf("Memberlist: failed to unmarshal remote state: %v", err)
		return
	}

	d.agent.stateMu.Lock()
	result := d.agent.mergeWorkloadState(&syncResp)
	d.agent.stateMu.Unlock()

	// Stop workloads we lost ownership of
	for _, wl := range result.LostOwnership {
		log.Printf("Memberlist sync: stopping local workload %s - ownership transferred", wl.Name)
		d.agent.removeWorkload(wl)
	}

	if result.EnvUpdated || len(result.LostOwnership) > 0 {
		d.agent.saveState()
	}

	d.agent.updateHosts()
}

// =============================================================================
// Event Delegate Implementation (Node Join/Leave)
// =============================================================================

// NotifyJoin is called when a node joins the cluster
func (e *jettyEventDelegate) NotifyJoin(node *memberlist.Node) {
	if node.Name == e.agent.hwid {
		return // Ignore self
	}

	meta := parseNodeMeta(node.Meta)
	if meta == nil {
		log.Printf("Memberlist: node %s joined but has invalid metadata", node.Name)
		return
	}

	log.Printf("Memberlist: node %s (%s) joined at %s", meta.Name, shortID(meta.ID, 12), meta.IP)

	e.agent.stateMu.Lock()
	peer := e.agent.state.Peers[meta.ID]
	if peer == nil {
		peer = &Peer{
			ID:   meta.ID,
			Name: meta.Name,
		}
		e.agent.state.Peers[meta.ID] = peer
	}
	peer.IP = meta.IP
	peer.Version = meta.Version
	peer.Arch = meta.Arch
	peer.Healthy = true
	peer.LastSeen = time.Now()
	e.agent.stateMu.Unlock()

	// Update userspace tunnel peer address
	e.agent.updateTunPeerAddr(meta.ID, meta.IP)

	// Ensure tunnel route to peer if needed
	e.agent.ensurePeerTunnel(meta.ID, meta.IP)

	e.agent.saveState()
}

// NotifyLeave is called when a node leaves the cluster. We don't wait
// for the periodic failover scan - kick checkFailover immediately so
// orphaned workloads start moving sub-second after node loss.
func (e *jettyEventDelegate) NotifyLeave(node *memberlist.Node) {
	if node.Name == e.agent.hwid {
		return // Ignore self
	}

	meta := parseNodeMeta(node.Meta)
	nodeID := node.Name
	nodeName := node.Name
	if meta != nil {
		nodeID = meta.ID
		nodeName = meta.Name
	}

	log.Printf("Memberlist: node %s (%s) left", nodeName, shortID(nodeID, 12))

	e.agent.stateMu.Lock()
	if peer := e.agent.state.Peers[nodeID]; peer != nil {
		peer.Healthy = false
		peer.LastSeen = time.Now()
	}
	e.agent.stateMu.Unlock()

	// React immediately rather than waiting for the next failover tick.
	// Cheap: checkFailover is a no-op if no orphans need claiming, and
	// shouldClaim's deterministic election prevents thundering-herd
	// across the surviving nodes.
	go e.agent.checkFailover()
}

// NotifyUpdate is called when a node's metadata is updated
func (e *jettyEventDelegate) NotifyUpdate(node *memberlist.Node) {
	if node.Name == e.agent.hwid {
		return // Ignore self
	}

	meta := parseNodeMeta(node.Meta)
	if meta == nil {
		return
	}

	e.agent.stateMu.Lock()
	if peer := e.agent.state.Peers[meta.ID]; peer != nil {
		oldIP := peer.IP
		peer.IP = meta.IP
		peer.Version = meta.Version
		peer.Arch = meta.Arch
		peer.Healthy = true
		peer.LastSeen = time.Now()

		// If IP changed, update tunnel
		if oldIP != meta.IP {
			log.Printf("Memberlist: node %s IP changed from %s to %s", meta.Name, oldIP, meta.IP)
			e.agent.updateTunPeerAddr(meta.ID, meta.IP)
			e.agent.ensurePeerTunnel(meta.ID, meta.IP)
		}
	}
	e.agent.stateMu.Unlock()
}

// =============================================================================
// Helper Functions
// =============================================================================

// parseNodeMeta parses the node metadata from memberlist
func parseNodeMeta(data []byte) *NodeMeta {
	if len(data) == 0 {
		return nil
	}
	var meta NodeMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil
	}
	return &meta
}

// updateNodeMeta updates this node's metadata (called when IP changes)
func (d *jettyDelegate) updateNodeMeta(ip string) {
	meta := NodeMeta{
		ID:      d.agent.hwid,
		Name:    d.agent.hostname,
		IP:      ip,
		Version: Version,
		Arch:    getArch(),
		APIPort: d.agent.apiPort,
	}

	data, err := json.Marshal(meta)
	if err != nil {
		log.Printf("Memberlist: failed to marshal node meta: %v", err)
		return
	}

	d.metaMu.Lock()
	d.meta = data
	d.metaMu.Unlock()
}

// =============================================================================
// Broadcast Handling
// =============================================================================

// handleBroadcast processes an incoming broadcast message
func (a *Agent) handleBroadcast(msg *BroadcastMessage) {
	switch msg.Type {
	case "workload_update":
		var wl Workload
		if err := json.Unmarshal(msg.Payload, &wl); err != nil {
			log.Printf("Memberlist: failed to unmarshal workload update: %v", err)
			return
		}
		a.handleWorkloadUpdate(&wl)

	case "workload_delete":
		var dw DeletedWorkload
		if err := json.Unmarshal(msg.Payload, &dw); err != nil {
			log.Printf("Memberlist: failed to unmarshal workload delete: %v", err)
			return
		}
		a.handleWorkloadDelete(&dw)

	case "env_update":
		var update struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.Unmarshal(msg.Payload, &update); err != nil {
			log.Printf("Memberlist: failed to unmarshal env update: %v", err)
			return
		}
		a.handleEnvUpdate(update.Key, update.Value)

	case "env_delete":
		var dek DeletedEnvKey
		if err := json.Unmarshal(msg.Payload, &dek); err != nil {
			log.Printf("Memberlist: failed to unmarshal env delete: %v", err)
			return
		}
		a.handleEnvDelete(&dek)
	}
}

// handleWorkloadUpdate processes a workload update broadcast
func (a *Agent) handleWorkloadUpdate(wl *Workload) {
	a.stateMu.Lock()
	defer a.stateMu.Unlock()

	// Check tombstone
	if tombstone := a.state.DeletedWorkloads[wl.IP]; tombstone != nil && tombstone.Version > wl.Version {
		return // Tombstone supersedes
	}

	existing := a.state.Workloads[wl.IP]
	if existing == nil || wl.Version > existing.Version {
		// Check if we lost ownership
		if existing != nil && existing.Owner == a.hwid && wl.Owner != a.hwid {
			log.Printf("Broadcast: lost ownership of %s to %s", existing.Name, shortID(wl.Owner, 12))
			go a.removeWorkload(existing)
		}
		a.state.Workloads[wl.IP] = wl
		a.saveState()
		a.updateHosts()
	}
}

// handleWorkloadDelete processes a workload delete broadcast
func (a *Agent) handleWorkloadDelete(dw *DeletedWorkload) {
	a.stateMu.Lock()
	defer a.stateMu.Unlock()

	// Update tombstone
	existingTombstone := a.state.DeletedWorkloads[dw.IP]
	if existingTombstone == nil || dw.Version > existingTombstone.Version {
		a.state.DeletedWorkloads[dw.IP] = dw
	}

	// Remove workload if tombstone is newer
	if existing := a.state.Workloads[dw.IP]; existing != nil && dw.Version > existing.Version {
		log.Printf("Broadcast: removing workload %s (deleted)", existing.Name)
		if existing.Owner == a.hwid {
			go a.removeWorkload(existing)
		}
		delete(a.state.Workloads, dw.IP)
		a.saveState()
		a.updateHosts()
	}
}

// handleEnvUpdate processes an env update broadcast
func (a *Agent) handleEnvUpdate(key, value string) {
	a.stateMu.Lock()
	defer a.stateMu.Unlock()

	// Check tombstone
	if tombstone := a.state.DeletedEnvKeys[key]; tombstone != nil {
		return // Key was deleted
	}

	if a.state.EnvData[key] != value {
		a.state.EnvData[key] = value
		a.saveState()
	}
}

// handleEnvDelete processes an env delete broadcast
func (a *Agent) handleEnvDelete(dek *DeletedEnvKey) {
	a.stateMu.Lock()
	defer a.stateMu.Unlock()

	existingTombstone := a.state.DeletedEnvKeys[dek.Key]
	if existingTombstone == nil || dek.Version > existingTombstone.Version {
		a.state.DeletedEnvKeys[dek.Key] = dek
	}

	if _, exists := a.state.EnvData[dek.Key]; exists {
		delete(a.state.EnvData, dek.Key)
		a.saveState()
	}
}

// =============================================================================
// Memberlist Setup
// =============================================================================

// initMemberlist initializes the memberlist cluster membership
func (a *Agent) initMemberlist() (*memberlist.Memberlist, error) {
	// Create memberlist config
	config := memberlist.DefaultLANConfig()
	config.Name = a.hwid
	config.BindPort = MemberlistPort
	config.AdvertisePort = MemberlistPort

	// Bind to WARP IP if available, otherwise all interfaces
	if a.ip != "" {
		config.BindAddr = a.ip
		config.AdvertiseAddr = a.ip
	}

	// Create delegate
	delegate := &jettyDelegate{
		agent: a,
	}
	delegate.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			a.stateMu.RLock()
			defer a.stateMu.RUnlock()
			return len(a.state.Peers) + 1
		},
		RetransmitMult: 3,
	}

	// Set initial node metadata
	delegate.updateNodeMeta(a.ip)

	config.Delegate = delegate
	config.Events = &jettyEventDelegate{agent: a}

	// Reduce logging noise
	config.LogOutput = &memberlistLogFilter{}

	// Create memberlist
	list, err := memberlist.Create(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}

	// Store delegate reference for later use
	a.mlDelegate = delegate

	return list, nil
}

// joinMemberlistPeers attempts to join known peers
func (a *Agent) joinMemberlistPeers() {
	a.stateMu.RLock()
	var addrs []string
	for _, peer := range a.state.Peers {
		if peer.IP != "" {
			addrs = append(addrs, fmt.Sprintf("%s:%d", peer.IP, MemberlistPort))
		}
	}
	a.stateMu.RUnlock()

	if len(addrs) == 0 {
		return
	}

	n, err := a.memberlist.Join(addrs)
	if err != nil {
		log.Printf("Memberlist: join failed: %v", err)
	} else {
		log.Printf("Memberlist: joined %d nodes", n)
	}
}

// broadcastWorkloadUpdate broadcasts a workload change to all nodes
func (a *Agent) broadcastWorkloadUpdate(wl *Workload) {
	if a.mlDelegate == nil {
		return
	}

	payload, err := json.Marshal(wl)
	if err != nil {
		log.Printf("Warning: failed to marshal workload for broadcast: %v", err)
		return
	}
	msg := BroadcastMessage{
		Type:    "workload_update",
		Payload: payload,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Warning: failed to marshal broadcast message: %v", err)
		return
	}

	a.mlDelegate.broadcasts.QueueBroadcast(&simpleBroadcast{msg: data})
}

// broadcastWorkloadDelete broadcasts a workload deletion to all nodes
func (a *Agent) broadcastWorkloadDelete(dw *DeletedWorkload) {
	if a.mlDelegate == nil {
		return
	}

	payload, err := json.Marshal(dw)
	if err != nil {
		log.Printf("Warning: failed to marshal deleted workload for broadcast: %v", err)
		return
	}
	msg := BroadcastMessage{
		Type:    "workload_delete",
		Payload: payload,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Warning: failed to marshal broadcast message: %v", err)
		return
	}

	a.mlDelegate.broadcasts.QueueBroadcast(&simpleBroadcast{msg: data})
}

// broadcastEnvUpdate broadcasts an env var change to all nodes
func (a *Agent) broadcastEnvUpdate(key, value string) {
	if a.mlDelegate == nil {
		return
	}

	payload, err := json.Marshal(map[string]string{"key": key, "value": value})
	if err != nil {
		log.Printf("Warning: failed to marshal env update for broadcast: %v", err)
		return
	}
	msg := BroadcastMessage{
		Type:    "env_update",
		Payload: payload,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Warning: failed to marshal broadcast message: %v", err)
		return
	}

	a.mlDelegate.broadcasts.QueueBroadcast(&simpleBroadcast{msg: data})
}

// broadcastEnvDelete broadcasts an env var deletion to all nodes
func (a *Agent) broadcastEnvDelete(dek *DeletedEnvKey) {
	if a.mlDelegate == nil {
		return
	}

	payload, err := json.Marshal(dek)
	if err != nil {
		log.Printf("Warning: failed to marshal deleted env key for broadcast: %v", err)
		return
	}
	msg := BroadcastMessage{
		Type:    "env_delete",
		Payload: payload,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Warning: failed to marshal broadcast message: %v", err)
		return
	}

	a.mlDelegate.broadcasts.QueueBroadcast(&simpleBroadcast{msg: data})
}

// =============================================================================
// Broadcast Implementation
// =============================================================================

// simpleBroadcast implements memberlist.Broadcast
type simpleBroadcast struct {
	msg []byte
}

func (b *simpleBroadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *simpleBroadcast) Message() []byte {
	return b.msg
}

func (b *simpleBroadcast) Finished() {}

func (b *simpleBroadcast) Name() string {
	return ""
}

// =============================================================================
// Log Filter
// =============================================================================

// memberlistLogFilter filters memberlist debug logs
type memberlistLogFilter struct{}

func (f *memberlistLogFilter) Write(p []byte) (n int, err error) {
	// Only log errors and warnings, suppress debug output
	// Memberlist is quite verbose by default
	return len(p), nil
}

// =============================================================================
// Sync Loop (background tasks that memberlist doesn't handle)
// =============================================================================

// memberlistSyncLoop runs periodic tasks alongside memberlist:
// - Tombstone garbage collection
// - Periodic state sync (catches missed broadcasts)
// - Route updates for remote workloads
func (a *Agent) memberlistSyncLoop() {
	syncTicker := time.NewTicker(30 * time.Second) // Full sync every 30s
	gcTicker := time.NewTicker(10 * time.Minute)   // GC every 10 minutes
	defer syncTicker.Stop()
	defer gcTicker.Stop()

	for {
		select {
		case <-a.stopCh:
			return

		case <-syncTicker.C:
			// Periodic full state sync to catch any missed broadcasts
			a.memberlistPeriodicSync()

		case <-gcTicker.C:
			// Garbage collect old tombstones
			a.gcTombstones()
		}
	}
}

// memberlistPeriodicSync does a full state sync with a random peer
// This catches any broadcasts that might have been missed
func (a *Agent) memberlistPeriodicSync() {
	if a.memberlist == nil {
		return
	}

	// Get a random healthy peer to sync with
	members := a.memberlist.Members()
	var peers []*memberlist.Node
	for _, m := range members {
		if m.Name != a.hwid {
			peers = append(peers, m)
		}
	}

	if len(peers) == 0 {
		return
	}

	// Pick a random peer
	peer := peers[time.Now().UnixNano()%int64(len(peers))]
	meta := parseNodeMeta(peer.Meta)
	if meta == nil || meta.IP == "" {
		return
	}

	// Request sync via API (memberlist LocalState/MergeRemoteState handles full sync on join,
	// but this catches incremental updates that might have been missed)
	url := fmt.Sprintf("http://%s:%d/api/sync", meta.IP, meta.APIPort)
	resp, err := peerClient.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var syncResp SyncResponse
	if err := json.NewDecoder(resp.Body).Decode(&syncResp); err != nil {
		return
	}

	a.stateMu.Lock()
	result := a.mergeWorkloadState(&syncResp)
	a.stateMu.Unlock()

	// Stop workloads we lost ownership of
	for _, wl := range result.LostOwnership {
		log.Printf("Periodic sync: stopping local workload %s - ownership transferred", wl.Name)
		a.removeWorkload(wl)
	}

	if result.EnvUpdated {
		a.saveState()
	}

	// Update routes for remote workloads (uses existing function from agent.go)
	a.stateMu.Lock()
	a.updateWorkloadRoutes()
	a.stateMu.Unlock()
	a.updateHosts()
}

// =============================================================================
// Utility
// =============================================================================

// getArch returns the current CPU architecture
func getArch() string {
	return runtime.GOARCH
}

// getMemberlistMembers returns the current memberlist members
func (a *Agent) getMemberlistMembers() []*memberlist.Node {
	if a.memberlist == nil {
		return nil
	}
	return a.memberlist.Members()
}

// getLocalNode returns this node's memberlist node
func (a *Agent) getLocalNode() *memberlist.Node {
	if a.memberlist == nil {
		return nil
	}
	return a.memberlist.LocalNode()
}

// updateMemberlistIP updates the advertised IP when WARP IP changes
func (a *Agent) updateMemberlistIP(newIP string) {
	if a.mlDelegate == nil {
		return
	}

	a.mlDelegate.updateNodeMeta(newIP)

	// Trigger metadata update to peers
	if a.memberlist != nil {
		a.memberlist.UpdateNode(0)
	}
}

// getHealthyPeersFromMemberlist returns healthy peers based on memberlist state
func (a *Agent) getHealthyPeersFromMemberlist() []string {
	if a.memberlist == nil {
		return nil
	}

	var healthy []string
	for _, node := range a.memberlist.Members() {
		if node.Name != a.hwid {
			healthy = append(healthy, node.Name)
		}
	}
	return healthy
}

// isPeerHealthyInMemberlist checks if a peer is healthy according to memberlist
func (a *Agent) isPeerHealthyInMemberlist(peerID string) bool {
	if a.memberlist == nil {
		return false
	}

	for _, node := range a.memberlist.Members() {
		if node.Name == peerID {
			return true
		}
	}
	return false
}
