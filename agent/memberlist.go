package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
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

// NodeMeta contains metadata about a node that is broadcast via memberlist.
// Carried over the gossip channel, which binds to WARP IP and is therefore
// only readable by other authenticated WARP peers - the APIKey field below
// inherits that trust boundary.
type NodeMeta struct {
	ID      string `json:"id"`      // HWID
	Name    string `json:"name"`    // Hostname
	IP      string `json:"ip"`      // WARP IP
	Version string `json:"version"` // Agent version
	Arch    string `json:"arch"`    // CPU architecture
	APIPort int    `json:"api_port"`
	// APIKey is this node's SelfAPIKey, propagated so other peers can
	// authenticate inbound requests from us via apiKeyMiddleware.
	// Without this, only the apiJoin-receiving node knows a new peer's
	// APIKey and the rest of the cluster rejects calls from it.
	APIKey string `json:"api_key,omitempty"`
}

// BroadcastMessage represents a message broadcast to all nodes
type BroadcastMessage struct {
	Type    string          `json:"type"`    // "workload_update", "workload_delete", "env_update", "env_delete", "env_undelete", "admin_key_rotate"
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

	if result.Changed {
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
	if !validIngestedNodeMeta(meta) {
		return
	}
	// Enforce that the peer's claimed meta.ID matches the trusted
	// node.Name (set by the peer's own memberlist config). Without
	// this, peer X could broadcast NodeMeta with ID="peer-Y" and we'd
	// merge their data onto peer Y's record.
	if meta.ID != node.Name {
		log.Printf("Memberlist: peer %s broadcast meta.ID=%s; refusing to merge",
			node.Name, meta.ID)
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
	// Adopt APIKey from gossip whenever it differs - the broadcasting
	// peer is the canonical owner of its own SelfAPIKey, so we follow
	// their lead. The node.Name == meta.ID enforcement above is what
	// makes this safe (peer X can't claim to be peer Y on the wire).
	if meta.APIKey != "" && peer.APIKey != meta.APIKey {
		peer.APIKey = meta.APIKey
	}
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
	if !validIngestedNodeMeta(meta) {
		return
	}
	if meta.ID != node.Name {
		log.Printf("Memberlist update: peer %s broadcast meta.ID=%s; refusing",
			node.Name, meta.ID)
		return
	}

	e.agent.stateMu.Lock()
	if peer := e.agent.state.Peers[meta.ID]; peer != nil {
		oldIP := peer.IP
		oldKey := peer.APIKey
		peer.IP = meta.IP
		peer.Version = meta.Version
		peer.Arch = meta.Arch
		peer.Healthy = true
		peer.LastSeen = time.Now()
		// Adopt APIKey from the broadcasting peer's NodeMeta. With
		// the meta.ID == node.Name check above, we know this is the
		// peer rotating its own key, not impersonation.
		if meta.APIKey != "" && peer.APIKey != meta.APIKey {
			peer.APIKey = meta.APIKey
			if oldKey != "" {
				log.Printf("Memberlist: adopted rotated APIKey for peer %s", shortID(meta.ID, 12))
			}
		}

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

// validIngestedNodeMeta is the memberlist-side analog of
// validIngestedPeer. NodeMeta.ID and NodeMeta.Name flow into /etc/hosts
// via routes.go::updateHosts; embedded newlines/tabs would let any
// gossip-channel participant inject arbitrary host->IP mappings on
// every node (and therefore every workload, since they share host
// /etc/hosts via --net host).
func validIngestedNodeMeta(meta *NodeMeta) bool {
	if meta == nil {
		return false
	}
	if !validNamePattern.MatchString(meta.ID) {
		log.Printf("Memberlist: rejecting node with invalid ID %q", meta.ID)
		return false
	}
	if !validPeerNamePattern.MatchString(meta.Name) {
		log.Printf("Memberlist: rejecting node %q with invalid name %q", meta.ID, meta.Name)
		return false
	}
	if meta.IP != "" && net.ParseIP(meta.IP) == nil {
		log.Printf("Memberlist: rejecting node %q with invalid IP %q", meta.ID, meta.IP)
		return false
	}
	return true
}

// updateNodeMeta updates this node's metadata (called when IP changes
// and when SelfAPIKey is first set during bootstrap).
func (d *jettyDelegate) updateNodeMeta(ip string) {
	d.agent.stateMu.RLock()
	apiKey := d.agent.state.SelfAPIKey
	d.agent.stateMu.RUnlock()

	meta := NodeMeta{
		ID:      d.agent.hwid,
		Name:    d.agent.hostname,
		IP:      ip,
		Version: Version,
		Arch:    getArch(),
		APIPort: d.agent.apiPort,
		APIKey:  apiKey,
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
			Key     string `json:"key"`
			Value   string `json:"value"`
			Version int64  `json:"version,omitempty"`
		}
		if err := json.Unmarshal(msg.Payload, &update); err != nil {
			log.Printf("Memberlist: failed to unmarshal env update: %v", err)
			return
		}
		a.handleEnvUpdate(update.Key, update.Value, update.Version)

	case "env_delete":
		var dek DeletedEnvKey
		if err := json.Unmarshal(msg.Payload, &dek); err != nil {
			log.Printf("Memberlist: failed to unmarshal env delete: %v", err)
			return
		}
		a.handleEnvDelete(&dek)

	case "env_undelete":
		var u struct {
			Key string `json:"key"`
		}
		if err := json.Unmarshal(msg.Payload, &u); err != nil {
			log.Printf("Memberlist: failed to unmarshal env undelete: %v", err)
			return
		}
		a.handleEnvUndelete(u.Key)

	case "admin_key_rotate":
		var ak struct {
			Key string `json:"key"`
		}
		if err := json.Unmarshal(msg.Payload, &ak); err != nil {
			log.Printf("Memberlist: failed to unmarshal admin-key rotation: %v", err)
			return
		}
		a.handleAdminKeyRotate(ak.Key)
	}
}

// handleAdminKeyRotate adopts a new AdminKey gossiped from another
// node. The rotation is initiated by an admin-authenticated POST to
// /api/admin-key/rotate, which already verified the caller. By the
// time this gossip lands, the originating node has accepted the new
// key, so we trust the broadcast.
func (a *Agent) handleAdminKeyRotate(newKey string) {
	if newKey == "" {
		return
	}
	a.stateMu.Lock()
	if a.state.AdminKey == newKey {
		a.stateMu.Unlock()
		return
	}
	a.state.AdminKey = newKey
	a.stateMu.Unlock()
	a.saveState()
	log.Printf("AdminKey rotated via gossip")
}

// broadcastAdminKey gossips a new AdminKey to all peers. Called from
// apiRotateAdminKey after the originating node has already saved the
// new key locally.
func (a *Agent) broadcastAdminKey(newKey string) {
	if a.mlDelegate == nil {
		return
	}
	payload, err := json.Marshal(map[string]string{"key": newKey})
	if err != nil {
		log.Printf("Warning: failed to marshal admin-key broadcast: %v", err)
		return
	}
	msg := BroadcastMessage{
		Type:    "admin_key_rotate",
		Payload: payload,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Warning: failed to marshal admin-key broadcast: %v", err)
		return
	}
	a.mlDelegate.broadcasts.QueueBroadcast(&simpleBroadcast{msg: data})
}

// handleWorkloadUpdate processes a workload update broadcast.
//
// We never call saveState/updateHosts under stateMu - both take a
// read lock internally and would deadlock against the write lock we
// hold here (Go's RWMutex is not reentrant).
func (a *Agent) handleWorkloadUpdate(wl *Workload) {
	if !validIngestedWorkload(wl) {
		return
	}

	a.stateMu.Lock()
	changed := false
	tombstone := a.state.DeletedWorkloads[wl.IP]
	if tombstone != nil && tombstone.Version > wl.Version {
		a.stateMu.Unlock()
		return
	}
	existing := a.state.Workloads[wl.IP]
	if existing == nil || wl.Version > existing.Version {
		if existing != nil && existing.Owner == a.hwid && wl.Owner != a.hwid {
			log.Printf("Broadcast: lost ownership of %s to %s", existing.Name, shortID(wl.Owner, 12))
			go a.removeWorkload(existing)
		}
		a.state.Workloads[wl.IP] = wl
		// Same stale-tombstone retirement as mergeWorkloadState: if
		// we're accepting a newer workload at this IP, any older
		// tombstone is stale and must be dropped. Otherwise our local
		// state.json would carry both entries until GC, /api/sync
		// responses to peers would include both, and a peer that
		// hasn't yet received this update could see the tombstone and
		// stop the workload it just accepted.
		if tombstone != nil {
			delete(a.state.DeletedWorkloads, wl.IP)
		}
		changed = true
	}
	a.stateMu.Unlock()

	if changed {
		a.saveState()
		a.updateHosts()
	}
}

// handleWorkloadDelete processes a workload delete broadcast
func (a *Agent) handleWorkloadDelete(dw *DeletedWorkload) {
	if !validIngestedTombstone(dw) {
		return
	}

	a.stateMu.Lock()
	changed := false
	existingTombstone := a.state.DeletedWorkloads[dw.IP]
	if existingTombstone == nil || dw.Version > existingTombstone.Version {
		a.state.DeletedWorkloads[dw.IP] = dw
		changed = true
	}
	if existing := a.state.Workloads[dw.IP]; existing != nil && dw.Version > existing.Version {
		log.Printf("Broadcast: removing workload %s (deleted)", existing.Name)
		if existing.Owner == a.hwid {
			go a.removeWorkload(existing)
		}
		delete(a.state.Workloads, dw.IP)
		changed = true
	}
	a.stateMu.Unlock()

	if changed {
		a.saveState()
		a.updateHosts()
	}
}

// handleEnvUpdate processes an env update broadcast.
//
// version is the originator's wall-clock at apiSetEnv time. We use it
// to decide whether our local tombstone (if any) is fresher than this
// update. Peers running pre-versioned builds send version=0; for those
// we keep the old "drop if tombstone present" behavior so a stale
// re-broadcast can't resurrect a value that another node legitimately
// just deleted.
func (a *Agent) handleEnvUpdate(key, value string, version int64) {
	if !ValidateEnvKey(key) {
		return
	}

	a.stateMu.Lock()
	changed := false
	if tombstone := a.state.DeletedEnvKeys[key]; tombstone != nil {
		// Old peers don't send a version - fall back to the conservative
		// path: tombstone wins, drop the update. /api/sync's merge logic
		// will heal within 30s using value-with-no-tombstone signaling.
		if version == 0 || version <= tombstone.Version {
			a.stateMu.Unlock()
			return
		}
		// Our tombstone is older than the originator's set - retire it.
		// This is the broadcast-side analog of the merge fix in
		// mergeWorkloadState; it lets the common delete-then-re-set
		// pattern converge without waiting for the next sync round.
		delete(a.state.DeletedEnvKeys, key)
		changed = true
	}
	if a.state.EnvData[key] != value {
		a.state.EnvData[key] = value
		changed = true
	}
	a.stateMu.Unlock()

	if changed {
		a.saveState()
	}
}

// handleEnvUndelete clears the tombstone for a key that was just
// re-set on the originating node. Without this, our local tombstone
// would still be present, the next sync round would re-broadcast it
// as a deletion, and the value would get wiped cluster-wide. The
// originating node already cleared its own tombstone before
// broadcasting; we mirror that.
func (a *Agent) handleEnvUndelete(key string) {
	if !ValidateEnvKey(key) {
		return
	}
	a.stateMu.Lock()
	_, hadTombstone := a.state.DeletedEnvKeys[key]
	if hadTombstone {
		delete(a.state.DeletedEnvKeys, key)
	}
	a.stateMu.Unlock()
	if hadTombstone {
		a.saveState()
	}
}

// handleEnvDelete processes an env delete broadcast
func (a *Agent) handleEnvDelete(dek *DeletedEnvKey) {
	if dek == nil || !ValidateEnvKey(dek.Key) {
		return
	}

	a.stateMu.Lock()
	changed := false
	existingTombstone := a.state.DeletedEnvKeys[dek.Key]
	if existingTombstone == nil || dek.Version > existingTombstone.Version {
		a.state.DeletedEnvKeys[dek.Key] = dek
		changed = true
	}
	if _, exists := a.state.EnvData[dek.Key]; exists {
		delete(a.state.EnvData, dek.Key)
		changed = true
	}
	a.stateMu.Unlock()

	if changed {
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

// broadcastEnvUpdate broadcasts an env var change to all nodes.
// version should be the originator's wall-clock (UnixNano) at the
// moment apiSetEnv accepted the change - receivers compare it against
// their local tombstone to decide whether to retire it.
func (a *Agent) broadcastEnvUpdate(key, value string, version int64) {
	if a.mlDelegate == nil {
		return
	}

	payload, err := json.Marshal(struct {
		Key     string `json:"key"`
		Value   string `json:"value"`
		Version int64  `json:"version,omitempty"`
	}{Key: key, Value: value, Version: version})
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

// broadcastEnvUndelete tells peers to clear their tombstone for a
// given env key (typically because the operator just re-set it).
// Sent BEFORE the matching env_update so peers' tombstone-aware
// handlers don't bounce the update.
func (a *Agent) broadcastEnvUndelete(key string) {
	if a.mlDelegate == nil {
		return
	}
	payload, err := json.Marshal(map[string]string{"key": key})
	if err != nil {
		log.Printf("Warning: failed to marshal env undelete: %v", err)
		return
	}
	msg := BroadcastMessage{
		Type:    "env_undelete",
		Payload: payload,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Warning: failed to marshal env undelete envelope: %v", err)
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
			// Garbage collect old tombstones and expired/burned join tokens.
			a.gcTombstones()
			a.gcExpiredTokens()
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
	req, err := a.peerRequest("GET", url, nil)
	if err != nil {
		return
	}
	resp, err := peerClient.Do(req)
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
