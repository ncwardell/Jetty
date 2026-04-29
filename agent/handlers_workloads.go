package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

// =============================================================================
// HTTP Handlers - workload CRUD and proxy
// =============================================================================
//
//   GET    /api/workloads               list, with ?node= and arch filters
//   POST   /api/workloads               create + deploy
//   GET    /api/workloads/{name}        details + container info
//   PATCH  /api/workloads/{name}        update; some fields force redeploy
//   DELETE /api/workloads/{name}        stop + remove
//   POST   /api/workloads/{name}/move   migrate to another node (blue-green)
//   GET    /api/workloads/{name}/logs   docker compose logs
//   POST   /api/workloads/{name}/start  start stopped containers
//   POST   /api/workloads/{name}/stop   stop without removing
//
// /api/proxy/{ip}/{path...} is a generic forwarder that lets the dashboard
// talk to any workload by mesh IP without the browser needing direct mesh
// reachability.

// apiListWorkloads godoc
// @Summary List all workloads
// @Description Returns all workloads in the cluster, optionally filtered by node
// @Tags workloads
// @Produce json
// @Param node query string false "Filter by node name or ID (use 'local' for this node only)"
// @Success 200 {array} Workload
// @Router /workloads [get]
func (a *Agent) apiListWorkloads(w http.ResponseWriter, r *http.Request) {
	nodeFilter := r.URL.Query().Get("node")

	a.stateMu.RLock()

	// Build peer info maps for enrichment and filtering
	peerNameToID := make(map[string]string)
	peerIDToInfo := make(map[string]map[string]string)
	peersByID := make(map[string]*Peer)
	for _, p := range a.state.Peers {
		peerNameToID[p.Name] = p.ID
		peerIDToInfo[p.ID] = map[string]string{
			"id":   p.ID,
			"name": p.Name,
			"ip":   p.IP,
		}
		peersByID[p.ID] = p
	}

	// Add local node to owner info map
	peerIDToInfo[a.hwid] = map[string]string{
		"id":   a.hwid,
		"name": a.hostname,
		"ip":   a.ip,
	}

	type WorkloadResponse struct {
		Name         string            `json:"name"`
		IP           string            `json:"ip"`
		Compose      string            `json:"compose"`
		Revive       bool              `json:"revive"`
		Autostart    bool              `json:"autostart"`
		AllowedNodes []string          `json:"allowed_nodes,omitempty"`
		Owner        map[string]string `json:"owner"`
		Version      int64             `json:"version"`
		Status       string            `json:"status"`
	}

	// Collect workload data - we'll fetch remote statuses after releasing the lock
	type workloadData struct {
		wl        *Workload
		ownerInfo map[string]string
		ownerPeer *Peer
		isLocal   bool
	}
	var workloadInfos []workloadData

	for _, wl := range a.state.Workloads {
		// Apply node filter if specified
		if nodeFilter != "" {
			// Handle special "local" filter
			if nodeFilter == "local" {
				if wl.Owner != a.hwid {
					continue
				}
			} else if nodeFilter == a.hwid || nodeFilter == a.hostname {
				// Filter for this node by ID or name
				if wl.Owner != a.hwid {
					continue
				}
			} else {
				// Filter for a specific peer by ID or name
				matchesFilter := wl.Owner == nodeFilter
				if !matchesFilter {
					// Check if filter matches a peer name
					if peerID, ok := peerNameToID[nodeFilter]; ok {
						matchesFilter = wl.Owner == peerID
					}
				}
				if !matchesFilter {
					continue
				}
			}
		}

		// Build enriched owner info
		ownerInfo := peerIDToInfo[wl.Owner]
		if ownerInfo == nil {
			ownerInfo = map[string]string{"id": wl.Owner, "name": "unknown", "ip": "unknown"}
		}

		isLocal := wl.Owner == a.hwid
		var ownerPeer *Peer
		if !isLocal {
			ownerPeer = peersByID[wl.Owner]
		}
		workloadInfos = append(workloadInfos, workloadData{
			wl:        wl,
			ownerInfo: ownerInfo,
			ownerPeer: ownerPeer,
			isLocal:   isLocal,
		})
	}
	a.stateMu.RUnlock()

	// Fetch remote workload statuses in parallel with 2 second timeout
	statusClient := &http.Client{Timeout: 2 * time.Second}
	statuses := make(map[string]string)
	var statusMu sync.Mutex
	var wg sync.WaitGroup

	for _, info := range workloadInfos {
		if info.isLocal {
			// Rich status: running/unhealthy/starting/restarting/stopped/unknown.
			statuses[info.wl.Name] = a.computeWorkloadStatus(info.wl.Name)
		} else if info.ownerPeer != nil && info.ownerPeer.Healthy {
			wg.Add(1)
			go func(wl *Workload, peer *Peer) {
				defer wg.Done()
				status := "remote" // Default fallback

				url := a.getPeerAPIURL(peer, "/api/workloads/"+wl.Name)
				if url != "" {
					req, err := a.peerRequest("GET", url, nil)
					if err == nil {
						resp, err := statusClient.Do(req)
						if err == nil {
							defer resp.Body.Close()
							if resp.StatusCode == 200 {
								var data map[string]interface{}
								if json.NewDecoder(resp.Body).Decode(&data) == nil {
									// Check containers array for running status
									if containers, ok := data["containers"].([]interface{}); ok && len(containers) > 0 {
										hasRunning := false
										for _, c := range containers {
											if cm, ok := c.(map[string]interface{}); ok {
												if running, ok := cm["running"].(bool); ok && running {
													hasRunning = true
													break
												}
											}
										}
										if hasRunning {
											status = "running"
										} else {
											status = "stopped"
										}
									} else {
										status = "stopped"
									}
								}
							}
						}
					}
				}

				statusMu.Lock()
				statuses[wl.Name] = status
				statusMu.Unlock()
			}(info.wl, info.ownerPeer)
		} else {
			statuses[info.wl.Name] = "remote"
		}
	}
	wg.Wait()

	// Build final workloads list
	workloads := make([]WorkloadResponse, 0, len(workloadInfos))
	for _, info := range workloadInfos {
		workloads = append(workloads, WorkloadResponse{
			Name:         info.wl.Name,
			IP:           info.wl.IP,
			Compose:      info.wl.Compose,
			Revive:       info.wl.Revive,
			Autostart:    info.wl.Autostart,
			AllowedNodes: info.wl.AllowedNodes,
			Owner:        info.ownerInfo,
			Version:      info.wl.Version,
			Status:       statuses[info.wl.Name],
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(workloads)
}
// apiCreateWorkload godoc
// @Summary Deploy a new workload
// @Description Creates and deploys a new Docker Compose workload with a mesh IP
// @Tags workloads
// @Accept json
// @Produce json
// @Param workload body WorkloadRequest true "Workload configuration"
// @Success 201 {object} Workload
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 409 {object} ErrorResponse "Mesh IP already in use"
// @Failure 500 {object} ErrorResponse "Deployment failed"
// @Router /workloads [post]
func (a *Agent) apiCreateWorkload(w http.ResponseWriter, r *http.Request) {
	// Check if this is a move operation (allows IP overlap during migration)
	isMove := r.URL.Query().Get("move") == "true"

	var wl Workload
	if err := json.NewDecoder(r.Body).Decode(&wl); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// Require at least one compose file (default or architecture-specific)
	if wl.Name == "" {
		http.Error(w, "name required", 400)
		return
	}
	if wl.Compose == "" && wl.ComposeAmd64 == "" && wl.ComposeArm64 == "" {
		http.Error(w, "at least one compose file required (compose, compose_amd64, or compose_arm64)", 400)
		return
	}

	// Validate workload name (prevent path traversal attacks)
	if !validNamePattern.MatchString(wl.Name) {
		http.Error(w, "invalid name: must be alphanumeric with dash/underscore only", 400)
		return
	}

	// Check if this node is allowed to run this workload
	if !a.isThisNodeAllowed(&wl) {
		// Find an allowed node and proxy the deployment
		a.stateMu.RLock()
		targetPeer := a.findAllowedNode(&wl)
		a.stateMu.RUnlock()

		if targetPeer == nil {
			http.Error(w, "no allowed nodes available for this workload", 503)
			return
		}

		// Proxy deployment to allowed node
		data, _ := json.Marshal(wl)
		url := a.getPeerAPIURL(targetPeer, "/api/workloads")
		if isMove {
			url += "?move=true"
		}
		req, _ := a.peerRequest("POST", url, strings.NewReader(string(data)))
		resp, err := httpClient.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to proxy to allowed node: %v", err), 502)
			return
		}
		defer resp.Body.Close()

		// Forward the response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Lock for entire check-and-set to prevent race condition
	a.stateMu.Lock()

	// Check for duplicate workload name (skip during move operation)
	if !isMove && a.isWorkloadNameTaken(wl.Name) {
		a.stateMu.Unlock()
		http.Error(w, "workload name already in use", 409)
		return
	}

	// Auto-allocate mesh IP if not provided
	if wl.IP == "" {
		wl.IP = a.allocateServiceIP()
		if wl.IP == "" {
			a.stateMu.Unlock()
			http.Error(w, "no available IPs in mesh CIDR", 507)
			return
		}
	} else {
		// Validate provided mesh IP format
		if net.ParseIP(wl.IP) == nil {
			a.stateMu.Unlock()
			http.Error(w, "invalid mesh_ip: must be valid IP address", 400)
			return
		}

		// Validate IP is within mesh CIDR
		if !a.isIPInCIDR(wl.IP) {
			a.stateMu.Unlock()
			http.Error(w, fmt.Sprintf("mesh_ip must be within %s", a.serviceCIDR), 400)
			return
		}

		// Check if IP is already taken (skip during move for blue-green deployment)
		if !isMove && a.isIPTaken(wl.IP) {
			a.stateMu.Unlock()
			http.Error(w, "mesh_ip already in use", 409)
			return
		}
	}

	wl.Owner = a.hwid
	wl.Version = time.Now().UnixNano()
	a.state.Workloads[wl.IP] = &wl
	a.stateMu.Unlock()

	// Deploy (outside lock to avoid blocking other operations)
	if err := a.deployWorkload(&wl); err != nil {
		// Rollback on failure
		a.stateMu.Lock()
		delete(a.state.Workloads, wl.IP)
		a.stateMu.Unlock()
		http.Error(w, err.Error(), 500)
		return
	}

	a.updateHosts()
	a.saveState()
	a.broadcastState()

	// Ask allowed peers to pre-pull this workload's image so future
	// failover doesn't have to download during the outage.
	go a.triggerPeerPrePull(&wl)

	// Build response with enriched owner info
	response := map[string]interface{}{
		"name":          wl.Name,
		"ip":            wl.IP,
		"compose":       wl.Compose,
		"revive":        wl.Revive,
		"autostart":     wl.Autostart,
		"allowed_nodes": wl.AllowedNodes,
		"owner": map[string]string{
			"id":   a.hwid,
			"name": a.hostname,
			"ip":   a.ip,
		},
		"version": wl.Version,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
// apiGetWorkload godoc
// @Summary Get workload details
// @Description Returns details for a specific workload by name
// @Tags workloads
// @Produce json
// @Param name path string true "Workload name"
// @Success 200 {object} Workload
// @Failure 404 {object} ErrorResponse "Workload not found"
// @Router /workloads/{name} [get]
func (a *Agent) apiGetWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	a.stateMu.RLock()
	var found *Workload
	var ownerPeer *Peer
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			// Find the owner peer if not local
			if wl.Owner != a.hwid {
				ownerPeer = a.state.Peers[wl.Owner]
			}
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	// Helper to build owner info object
	buildOwnerInfo := func(id, peerName, meshIP string) map[string]string {
		return map[string]string{
			"id":   id,
			"name": peerName,
			"ip":   meshIP,
		}
	}

	// If workload is remote, proxy the request to the owner node
	if found.Owner != a.hwid {
		if ownerPeer == nil || !ownerPeer.Healthy {
			// Owner not reachable, return basic info without container details
			ownerInfo := buildOwnerInfo(found.Owner, "unknown", "unknown")
			if ownerPeer != nil {
				ownerInfo = buildOwnerInfo(ownerPeer.ID, ownerPeer.Name, ownerPeer.IP)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"name":          found.Name,
				"ip":            found.IP,
				"compose":       found.Compose,
				"revive":        found.Revive,
				"autostart":     found.Autostart,
				"allowed_nodes": found.AllowedNodes,
				"owner":         ownerInfo,
				"version":       found.Version,
				"is_local":      false,
				"error":         "owner node unreachable",
			})
			return
		}

		// Proxy request to owner
		url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name)
		req, _ := a.peerRequest("GET", url, nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			ownerInfo := buildOwnerInfo(ownerPeer.ID, ownerPeer.Name, ownerPeer.IP)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"name":          found.Name,
				"ip":            found.IP,
				"compose":       found.Compose,
				"revive":        found.Revive,
				"autostart":     found.Autostart,
				"allowed_nodes": found.AllowedNodes,
				"owner":         ownerInfo,
				"version":       found.Version,
				"is_local":      false,
				"error":         fmt.Sprintf("failed to reach owner: %v", err),
			})
			return
		}
		defer resp.Body.Close()

		// Forward the response from owner
		w.Header().Set("Content-Type", "application/json")
		body, _ := io.ReadAll(resp.Body)

		// Parse and update is_local field
		var remoteResp map[string]interface{}
		if err := json.Unmarshal(body, &remoteResp); err == nil {
			remoteResp["is_local"] = false // Override since we proxied
			json.NewEncoder(w).Encode(remoteResp)
		} else {
			w.Write(body)
		}
		return
	}

	// Build enriched response with Docker info for local workload
	ownerInfo := buildOwnerInfo(a.hwid, a.hostname, a.ip)
	response := map[string]interface{}{
		"name":          found.Name,
		"ip":            found.IP,
		"compose":       found.Compose,
		"revive":        found.Revive,
		"autostart":     found.Autostart,
		"allowed_nodes": found.AllowedNodes,
		"owner":         ownerInfo,
		"version":       found.Version,
		"is_local":      true,
	}

	containerInfo := a.getContainerInfo(found.Name)
	response["containers"] = containerInfo

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
// apiUpdateWorkload godoc
// @Summary Update a workload
// @Description Updates workload configuration. Some fields require redeploy.
// @Tags workloads
// @Accept json
// @Produce json
// @Param name path string true "Workload name"
// @Param update body object true "Fields to update"
// @Success 200 {object} Workload
// @Failure 404 {object} ErrorResponse "Workload not found"
// @Failure 409 {object} ErrorResponse "Mesh IP conflict"
// @Failure 500 {object} ErrorResponse "Update failed"
// @Router /workloads/{name} [patch]
func (a *Agent) apiUpdateWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	// Parse update request
	var update struct {
		Compose      *string   `json:"compose,omitempty"`
		ComposeAmd64 *string   `json:"compose_amd64,omitempty"`
		ComposeArm64 *string   `json:"compose_arm64,omitempty"`
		IP           *string   `json:"ip,omitempty"`
		Revive       *bool     `json:"revive,omitempty"`
		Autostart    *bool     `json:"autostart,omitempty"`
		AllowedNodes *[]string `json:"allowed_nodes,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// Find workload
	a.stateMu.RLock()
	var found *Workload
	var foundIP string
	var ownerPeer *Peer
	for ip, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			foundIP = ip
			if wl.Owner != a.hwid {
				ownerPeer = a.state.Peers[wl.Owner]
			}
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	// If workload is remote, proxy to owner
	if found.Owner != a.hwid {
		if ownerPeer == nil || !ownerPeer.Healthy {
			http.Error(w, "owner node unreachable", 502)
			return
		}

		// Proxy PATCH to owner
		body, _ := json.Marshal(update)
		url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name)
		req, _ := a.peerRequest("PATCH", url, strings.NewReader(string(body)))
		resp, err := httpClient.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to reach owner: %v", err), 502)
			return
		}
		defer resp.Body.Close()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Local workload - apply updates
	needsRedeploy := false
	newMeshIP := foundIP

	a.stateMu.Lock()

	// Handle mesh IP change
	if update.IP != nil && *update.IP != found.IP {
		newIP := *update.IP

		// Validate new IP
		if net.ParseIP(newIP) == nil {
			a.stateMu.Unlock()
			http.Error(w, "invalid mesh_ip: must be valid IP address", 400)
			return
		}
		if !a.isIPInCIDR(newIP) {
			a.stateMu.Unlock()
			http.Error(w, fmt.Sprintf("mesh_ip must be within %s", a.serviceCIDR), 400)
			return
		}
		if a.isIPTaken(newIP) {
			a.stateMu.Unlock()
			http.Error(w, "mesh_ip already in use", 409)
			return
		}

		// Remove old entry, will add new one
		delete(a.state.Workloads, foundIP)
		newMeshIP = newIP
		found.IP = newIP
		needsRedeploy = true
	}

	// Handle compose changes
	if update.Compose != nil && *update.Compose != found.Compose {
		found.Compose = *update.Compose
		needsRedeploy = true
	}
	if update.ComposeAmd64 != nil && *update.ComposeAmd64 != found.ComposeAmd64 {
		found.ComposeAmd64 = *update.ComposeAmd64
		// Only redeploy if we're on amd64 and this is the active compose
		if runtime.GOARCH == "amd64" {
			needsRedeploy = true
		}
	}
	if update.ComposeArm64 != nil && *update.ComposeArm64 != found.ComposeArm64 {
		found.ComposeArm64 = *update.ComposeArm64
		// Only redeploy if we're on arm64 and this is the active compose
		if runtime.GOARCH == "arm64" {
			needsRedeploy = true
		}
	}

	// Handle metadata updates (no redeploy needed)
	if update.Revive != nil {
		found.Revive = *update.Revive
	}
	if update.Autostart != nil {
		found.Autostart = *update.Autostart
	}
	if update.AllowedNodes != nil {
		found.AllowedNodes = *update.AllowedNodes
	}

	// Update version
	found.Version = time.Now().UnixNano()

	// Store with (potentially new) mesh IP
	a.state.Workloads[newMeshIP] = found
	a.stateMu.Unlock()

	// Redeploy if needed
	if needsRedeploy {
		// Remove old deployment
		a.removeWorkload(found)

		// Deploy with new config
		if err := a.deployWorkload(found); err != nil {
			// Rollback on failure
			a.stateMu.Lock()
			delete(a.state.Workloads, newMeshIP)
			if newMeshIP != foundIP {
				found.IP = foundIP
				a.state.Workloads[foundIP] = found
			}
			a.stateMu.Unlock()
			http.Error(w, fmt.Sprintf("redeploy failed: %v", err), 500)
			return
		}
	}

	a.updateHosts()
	a.saveState()
	a.broadcastState()

	// If the compose changed, refresh peer pre-pulls so they cache the
	// new image before any failover.
	if needsRedeploy {
		go a.triggerPeerPrePull(found)
	}

	// Build response
	response := map[string]interface{}{
		"name":          found.Name,
		"ip":            found.IP,
		"compose":       found.Compose,
		"revive":        found.Revive,
		"autostart":     found.Autostart,
		"allowed_nodes": found.AllowedNodes,
		"owner": map[string]string{
			"id":   a.hwid,
			"name": a.hostname,
			"ip":   a.ip,
		},
		"version":    found.Version,
		"redeployed": needsRedeploy,
	}

	log.Printf("Updated workload %s (redeploy=%v)", found.Name, needsRedeploy)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
// getContainerInfo retrieves Docker container details for a workload
// computeWorkloadStatus returns a richer per-workload status string than
// the legacy "running" / "stopped" check, by reading every container's
// state and healthcheck status via docker inspect.
//
// Possible return values:
//
//	"running"     - all containers up, healthchecks passing or absent
//	"unhealthy"   - all containers up, but at least one healthcheck failing
//	"starting"    - up, healthcheck still in its startup grace period
//	"restarting"  - at least one container is in restart-loop state
//	"stopped"     - no containers running
//	"unknown"     - couldn't determine (compose project missing, docker error)
//
// Only inspects the workload's own compose project (label=jetty_<name>).
// Cheap-ish: one `docker ps` plus one `docker inspect` per container.
func (a *Agent) computeWorkloadStatus(workloadName string) string {
	out, err := exec.Command("docker", "ps", "-a", "-q", "-f", "label=com.docker.compose.project=jetty_"+workloadName).Output()
	if err != nil {
		return "unknown"
	}
	ids := strings.Fields(strings.TrimSpace(string(out)))
	if len(ids) == 0 {
		return "stopped"
	}

	var (
		anyRunning    bool
		anyUnhealthy  bool
		anyStarting   bool
		anyRestarting bool
		anyStopped    bool
	)

	for _, id := range ids {
		inspectOut, err := exec.Command("docker", "inspect", "--format", `{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}`, id).Output()
		if err != nil {
			anyStopped = true
			continue
		}
		fields := strings.SplitN(strings.TrimSpace(string(inspectOut)), "|", 2)
		state := fields[0]
		health := ""
		if len(fields) == 2 {
			health = fields[1]
		}

		switch state {
		case "running":
			anyRunning = true
			switch health {
			case "unhealthy":
				anyUnhealthy = true
			case "starting":
				anyStarting = true
			}
		case "restarting":
			anyRestarting = true
		default: // exited, dead, paused, created
			anyStopped = true
		}
	}

	switch {
	case anyRestarting:
		return "restarting"
	case anyUnhealthy:
		return "unhealthy"
	case anyStarting:
		return "starting"
	case anyRunning && !anyStopped:
		return "running"
	case anyRunning:
		// At least one running but at least one stopped - partial.
		// Surface as "unhealthy" so the user notices.
		return "unhealthy"
	default:
		return "stopped"
	}
}

func (a *Agent) getContainerInfo(workloadName string) []map[string]interface{} {
	var containers []map[string]interface{}

	// Get container IDs for this project
	out, err := exec.Command("docker", "ps", "-a", "-q", "-f", "label=com.docker.compose.project=jetty_"+workloadName).Output()
	if err != nil || len(out) == 0 {
		return containers
	}

	containerIDs := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, containerID := range containerIDs {
		if containerID == "" {
			continue
		}

		// Get detailed container info using docker inspect
		inspectOut, err := exec.Command("docker", "inspect", "--format", `{{json .}}`, containerID).Output()
		if err != nil {
			continue
		}

		var inspectData map[string]interface{}
		if err := json.Unmarshal(inspectOut, &inspectData); err != nil {
			continue
		}

		info := make(map[string]interface{})
		info["id"] = containerID[:12] // Short ID

		// Extract container name
		if name, ok := inspectData["Name"].(string); ok {
			info["name"] = strings.TrimPrefix(name, "/")
		}

		// Extract state info
		if state, ok := inspectData["State"].(map[string]interface{}); ok {
			info["status"] = state["Status"]
			info["running"] = state["Running"]
			if startedAt, ok := state["StartedAt"].(string); ok && startedAt != "0001-01-01T00:00:00Z" {
				if t, err := time.Parse(time.RFC3339Nano, startedAt); err == nil {
					info["started_at"] = t.Format(time.RFC3339)
					info["uptime"] = time.Since(t).Round(time.Second).String()
				}
			}
			if finishedAt, ok := state["FinishedAt"].(string); ok && finishedAt != "0001-01-01T00:00:00Z" {
				if t, err := time.Parse(time.RFC3339Nano, finishedAt); err == nil && !t.IsZero() {
					info["finished_at"] = t.Format(time.RFC3339)
				}
			}
			if exitCode, ok := state["ExitCode"].(float64); ok {
				info["exit_code"] = int(exitCode)
			}
			if health, ok := state["Health"].(map[string]interface{}); ok {
				info["health"] = health["Status"]
			}
		}

		// Extract image info
		if config, ok := inspectData["Config"].(map[string]interface{}); ok {
			info["image"] = config["Image"]
		}

		// Extract network info
		if netSettings, ok := inspectData["NetworkSettings"].(map[string]interface{}); ok {
			if networks, ok := netSettings["Networks"].(map[string]interface{}); ok {
				var ips []string
				for netName, netData := range networks {
					if netInfo, ok := netData.(map[string]interface{}); ok {
						if ip, ok := netInfo["IPAddress"].(string); ok && ip != "" {
							ips = append(ips, fmt.Sprintf("%s:%s", netName, ip))
						}
					}
				}
				info["networks"] = ips
			}
			if ports, ok := netSettings["Ports"].(map[string]interface{}); ok {
				var portList []string
				for port := range ports {
					portList = append(portList, port)
				}
				info["ports"] = portList
			}
		}

		// Get resource usage using docker stats (quick snapshot)
		statsOut, _ := exec.Command("docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}", containerID).Output()
		if len(statsOut) > 0 {
			parts := strings.Split(strings.TrimSpace(string(statsOut)), "|")
			if len(parts) == 3 {
				info["cpu_percent"] = strings.TrimSpace(parts[0])
				info["memory_usage"] = strings.TrimSpace(parts[1])
				info["memory_percent"] = strings.TrimSpace(parts[2])
			}
		}

		containers = append(containers, info)
	}

	return containers
}
// apiDeleteWorkload godoc
// @Summary Delete a workload
// @Description Stops and removes a workload from the cluster
// @Tags workloads
// @Param name path string true "Workload name"
// @Success 204 "Workload deleted"
// @Failure 404 {object} ErrorResponse "Workload not found"
// @Router /workloads/{name} [delete]
func (a *Agent) apiDeleteWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	a.stateMu.RLock()
	var found *Workload
	var foundIP string
	var ownerPeer *Peer
	for ip, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			foundIP = ip
			if wl.Owner != a.hwid {
				ownerPeer = a.state.Peers[wl.Owner]
			}
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	// If workload is remote and owner is healthy, proxy the delete request
	if found.Owner != a.hwid {
		if ownerPeer != nil && ownerPeer.Healthy {
			// Proxy delete to owner node
			url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name)
			req, _ := a.peerRequest("DELETE", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				http.Error(w, fmt.Sprintf("failed to reach owner: %v", err), 502)
				return
			}
			defer resp.Body.Close()

			// Forward response from owner
			w.WriteHeader(resp.StatusCode)
			if resp.StatusCode != 204 {
				body, _ := io.ReadAll(resp.Body)
				w.Write(body)
			}
			return
		}
		// Owner is dead - allow cleanup of orphaned workload from our state
		log.Printf("Cleaning up orphaned workload %s (owner %s unreachable)", name, found.Owner)
	}

	// Local workload or orphaned cleanup - delete from state and add tombstone
	a.stateMu.Lock()
	delete(a.state.Workloads, foundIP)
	// Add tombstone with version greater than the deleted workload's version
	// This ensures the deletion propagates to peers even if they have older copies
	a.state.DeletedWorkloads[foundIP] = &DeletedWorkload{
		IP:      foundIP,
		Version: time.Now().UnixNano(),
	}
	a.stateMu.Unlock()

	log.Printf("Deleted workload %s (IP %s), created tombstone for sync propagation", name, foundIP)

	// Remove if we're running it
	if found.Owner == a.hwid {
		a.removeWorkload(found)
	}

	a.updateHosts()
	a.saveState()
	a.broadcastState()

	w.WriteHeader(204)
}
// apiMoveWorkload godoc
// @Summary Move workload to another node
// @Description Migrates a workload to a different node in the cluster
// @Tags workloads
// @Accept json
// @Param name path string true "Workload name"
// @Param target body MoveRequest true "Target node"
// @Success 200 "Workload moved"
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 404 {object} ErrorResponse "Workload or target not found"
// @Router /workloads/{name}/move [post]
func (a *Agent) apiMoveWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	var req struct {
		To string `json:"to"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), 400)
		return
	}

	// Find workload and current owner
	a.stateMu.RLock()
	var found *Workload
	var currentOwner *Peer
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			if wl.Owner != a.hwid {
				currentOwner = a.state.Peers[wl.Owner]
			}
			break
		}
	}

	// Find target (check if moving to self first, then search peers)
	var target *Peer
	var targetIsSelf bool
	if req.To == a.hwid || req.To == a.hostname {
		target = &Peer{
			ID:      a.hwid,
			Name:    a.hostname,
			IP:      a.ip,
			Healthy: true,
			Arch:    runtime.GOARCH,
		}
		targetIsSelf = true
	} else {
		for _, p := range a.state.Peers {
			if p.Name == req.To || p.ID == req.To {
				target = p
				break
			}
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
	if !target.Healthy {
		http.Error(w, "target node is not healthy", 503)
		return
	}

	// Check if we already own it (no-op)
	if found.Owner == a.hwid && targetIsSelf {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"moved": "no-op", "reason": "already on target node"})
		return
	}

	// Check if target is allowed to run this workload (includes arch compatibility)
	if !a.isNodeAllowed(found, target.ID, target.Name, target.Arch) {
		// Provide a more specific error message
		if !found.canRunOnArch(target.Arch) {
			http.Error(w, fmt.Sprintf("workload has no compose file for target architecture (%s)", target.Arch), 400)
		} else {
			http.Error(w, "target node is not in allowed_nodes for this workload", 403)
		}
		return
	}

	// Blue-green deployment: deploy on target first, THEN remove from source.
	// Doing it in this order means the workload is briefly running on both
	// nodes - acceptable, since other nodes' /32 routes still point at the
	// source until our broadcast catches up. The reverse order (delete first,
	// then deploy) creates a real downtime window equal to the deploy time.
	var newVersion int64
	if targetIsSelf {
		// Deploy locally FIRST while the source is still serving.
		newVersion = time.Now().UnixNano()
		localWl := *found // Copy workload
		localWl.Owner = a.hwid
		localWl.Version = newVersion

		if err := a.deployWorkload(&localWl); err != nil {
			http.Error(w, fmt.Sprintf("local deploy failed: %v", err), 500)
			return
		}

		// Update state immediately so our routes/hosts reflect the new owner.
		a.stateMu.Lock()
		a.state.Workloads[found.IP] = &localWl
		a.stateMu.Unlock()

		// Now that we're serving, ask the source to drop its copy.
		// If this fails, both nodes briefly run the workload; sync's
		// "highest version wins" cleans up at the next gossip tick.
		if currentOwner != nil && currentOwner.Healthy {
			deleteURL := a.getPeerAPIURL(currentOwner, "/api/workloads/"+name)
			delReq, _ := a.peerRequest("DELETE", deleteURL, nil)
			delResp, err := httpClient.Do(delReq)
			if err != nil {
				log.Printf("Warning: failed to remove workload from original owner (will reconcile via gossip): %v", err)
			} else {
				delResp.Body.Close()
			}
		}
	} else {
		// Moving to remote peer - deploy via API
		data, _ := json.Marshal(found)
		url := a.getPeerAPIURL(target, "/api/workloads?move=true")
		deployReq, _ := a.peerRequest("POST", url, strings.NewReader(string(data)))
		resp, err := httpClient.Do(deployReq)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to deploy on target: %v", err), 502)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			http.Error(w, fmt.Sprintf("target rejected deployment (status %d): %s", resp.StatusCode, body), 500)
			return
		}

		// Parse response to get updated workload info (new owner, version)
		var deployResult struct {
			Name    string `json:"name"`
			IP      string `json:"ip"`
			Version int64  `json:"version"`
			Owner   struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"owner"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&deployResult); err != nil {
			log.Printf("Warning: could not parse deploy response: %v", err)
		}
		newVersion = deployResult.Version

		// Target is now running - remove from source
		// If we own it, remove locally (but don't delete from state yet - we'll update it)
		if found.Owner == a.hwid {
			a.removeWorkload(found)
		} else if currentOwner != nil && currentOwner.Healthy {
			// Proxy delete to current owner
			deleteURL := a.getPeerAPIURL(currentOwner, "/api/workloads/"+name)
			delReq, _ := a.peerRequest("DELETE", deleteURL, nil)
			delResp, err := httpClient.Do(delReq)
			if err != nil {
				log.Printf("Warning: failed to remove workload from original owner: %v", err)
			} else {
				delResp.Body.Close()
			}
		}

		// Update local state immediately to reflect new owner
		// This prevents stale state issues during rapid moves
		a.stateMu.Lock()
		if wl, exists := a.state.Workloads[found.IP]; exists {
			wl.Owner = target.ID
			if newVersion > 0 {
				wl.Version = newVersion
			} else {
				wl.Version = time.Now().UnixNano()
			}
		}
		a.stateMu.Unlock()
	}

	a.updateHosts()
	a.saveState()
	a.broadcastState()

	log.Printf("Moved workload %s to %s", found.Name, target.Name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"moved": "ok", "to": target.Name})
}
// apiWorkloadLogs godoc
// @Summary Get workload logs
// @Description Returns container logs for a workload
// @Tags workloads
// @Produce plain
// @Param name path string true "Workload name"
// @Success 200 {string} string "Container logs"
// @Failure 404 {object} ErrorResponse "Workload not found"
// @Router /workloads/{name}/logs [get]
func (a *Agent) apiWorkloadLogs(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	a.stateMu.RLock()
	var found *Workload
	var ownerPeer *Peer
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			if wl.Owner != a.hwid {
				ownerPeer = a.state.Peers[wl.Owner]
			}
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	// If workload is remote, proxy to owner
	if found.Owner != a.hwid {
		if ownerPeer == nil || !ownerPeer.Healthy {
			http.Error(w, "owner node unreachable", 502)
			return
		}
		// Proxy logs request to owner
		url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name+"/logs")
		req, _ := a.peerRequest("GET", url, nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to reach owner: %v", err), 502)
			return
		}
		defer resp.Body.Close()

		w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	out, _ := a.composeCmd(found.Name, "logs", "--tail", "200")
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(out))
}
// apiStartWorkload godoc
// @Summary Start a stopped workload
// @Description Starts the containers for a workload that was previously stopped
// @Tags workloads
// @Param name path string true "Workload name"
// @Success 200 {object} map[string]string
// @Failure 404 {object} ErrorResponse "Workload not found"
// @Failure 500 {object} ErrorResponse "Start failed"
// @Router /workloads/{name}/start [post]
func (a *Agent) apiStartWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	a.stateMu.RLock()
	var found *Workload
	var ownerPeer *Peer
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			if wl.Owner != a.hwid {
				ownerPeer = a.state.Peers[wl.Owner]
			}
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	// If workload is remote, proxy to owner
	if found.Owner != a.hwid {
		if ownerPeer == nil || !ownerPeer.Healthy {
			http.Error(w, "owner node unreachable", 502)
			return
		}
		// Proxy start request to owner
		url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name+"/start")
		req, _ := a.peerRequest("POST", url, nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to reach owner: %v", err), 502)
			return
		}
		defer resp.Body.Close()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	if out, err := a.composeCmd(found.Name, "start"); err != nil {
		http.Error(w, fmt.Sprintf("start failed: %s", out), 500)
		return
	}

	// Re-setup mesh IP routing (container IP may have changed)
	a.setupWorkloadIP(found)

	log.Printf("Started workload: %s", found.Name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "started", "name": found.Name})
}
// apiStopWorkload godoc
// @Summary Stop a running workload
// @Description Stops the containers for a workload without removing them
// @Tags workloads
// @Param name path string true "Workload name"
// @Success 200 {object} map[string]string
// @Failure 404 {object} ErrorResponse "Workload not found"
// @Failure 500 {object} ErrorResponse "Stop failed"
// @Router /workloads/{name}/stop [post]
func (a *Agent) apiStopWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	a.stateMu.RLock()
	var found *Workload
	var ownerPeer *Peer
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			if wl.Owner != a.hwid {
				ownerPeer = a.state.Peers[wl.Owner]
			}
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	// If workload is remote, proxy to owner
	if found.Owner != a.hwid {
		if ownerPeer == nil || !ownerPeer.Healthy {
			http.Error(w, "owner node unreachable", 502)
			return
		}
		// Proxy stop request to owner
		url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name+"/stop")
		req, _ := a.peerRequest("POST", url, nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to reach owner: %v", err), 502)
			return
		}
		defer resp.Body.Close()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Clean up iptables before stopping (need container IP)
	a.cleanupWorkloadIP(found)

	if out, err := a.composeCmd(found.Name, "stop"); err != nil {
		http.Error(w, fmt.Sprintf("stop failed: %s", out), 500)
		return
	}

	log.Printf("Stopped workload: %s", found.Name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "stopped", "name": found.Name})
}

// apiRestartWorkload godoc
// @Summary Restart a workload (force-recreate)
// @Description Recreates the workload's containers, picking up any config changes (env, extra_hosts, image updates that have been pulled). Equivalent to docker compose up -d --force-recreate. To pick up a new image, call /workloads/{name} PATCH with a new compose first, then /workloads/{name}/restart.
// @Tags workloads
// @Param name path string true "Workload name"
// @Success 200 {object} map[string]string
// @Failure 404 {object} ErrorResponse "Workload not found"
// @Failure 500 {object} ErrorResponse "Restart failed"
// @Router /workloads/{name}/restart [post]
func (a *Agent) apiRestartWorkload(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	a.stateMu.RLock()
	var found *Workload
	var ownerPeer *Peer
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			if wl.Owner != a.hwid {
				ownerPeer = a.state.Peers[wl.Owner]
			}
			break
		}
	}
	a.stateMu.RUnlock()

	if found == nil {
		http.Error(w, "not found", 404)
		return
	}

	// Remote workload: proxy to its owner.
	if found.Owner != a.hwid {
		if ownerPeer == nil || !ownerPeer.Healthy {
			http.Error(w, "owner node unreachable", 502)
			return
		}
		url := a.getPeerAPIURL(ownerPeer, "/api/workloads/"+name+"/restart")
		req, _ := a.peerRequest("POST", url, nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to reach owner: %v", err), 502)
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Local: clean up old DNAT rules first (container IP will change after recreate),
	// then force-recreate so any compose/override changes are applied, then re-set up routing.
	a.cleanupWorkloadIP(found)

	// Optionally refresh the pull in case the user wants a newer image (pull
	// uses the same image name so this is a no-op unless the registry has
	// a newer tag; not strictly required for restart but cheap).
	a.pullWithRetry(found.Name)

	if out, err := a.composeCmd(found.Name, "up", "-d", "--force-recreate", "--remove-orphans"); err != nil {
		http.Error(w, fmt.Sprintf("restart failed: %s", out), 500)
		return
	}

	if found.IP != "" {
		a.setupWorkloadIP(found)
	}

	log.Printf("Restarted workload: %s", found.Name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "restarted", "name": found.Name})
}

// apiPrePullWorkload godoc
// @Summary Pre-pull a workload's image on this node
// @Description Asks this node to pull the workload's image into its docker cache. Used by other cluster members to warm caches on potential failover targets, so failover doesn't have to download images during the outage. Returns immediately; the pull runs in the background.
// @Tags workloads
// @Param name path string true "Workload name"
// @Success 202 {object} map[string]string "Pre-pull scheduled"
// @Failure 404 {object} ErrorResponse "Workload not found in cluster state"
// @Router /workloads/{name}/prepull [post]
func (a *Agent) apiPrePullWorkload(w http.ResponseWriter, r *http.Request) {
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
		http.Error(w, "workload not in cluster state yet", 404)
		return
	}

	// If we're the owner, pulling is part of our normal deploy path -
	// nothing to pre-cache.
	if found.Owner == a.hwid {
		writeJSONStatus(w, http.StatusAccepted, map[string]string{"status": "owner-skip", "name": name})
		return
	}

	// If we can't run this workload, no point caching the image.
	if !a.isThisNodeAllowed(found) {
		writeJSONStatus(w, http.StatusAccepted, map[string]string{"status": "not-allowed-skip", "name": name})
		return
	}

	if err := a.prePullLocally(found); err != nil {
		http.Error(w, fmt.Sprintf("pre-pull failed: %v", err), 500)
		return
	}
	writeJSONStatus(w, http.StatusAccepted, map[string]string{"status": "scheduled", "name": name})
}

// apiWorkloadProxy proxies HTTP requests to workloads.
// URL format: /api/proxy/{mesh_ip}/{path...}
// If the workload is local, forwards to the container directly.
// If remote, forwards through the tunnel or mesh IP.
func (a *Agent) apiWorkloadProxy(w http.ResponseWriter, r *http.Request) {
	// Parse path: /api/proxy/10.100.1.5/some/path
	path := strings.TrimPrefix(r.URL.Path, "/api/proxy/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "mesh_ip required in path", 400)
		return
	}

	meshIP := parts[0]
	targetPath := "/"
	if len(parts) > 1 {
		targetPath = "/" + parts[1]
	}

	// Validate mesh IP format
	if net.ParseIP(meshIP) == nil {
		http.Error(w, "invalid mesh_ip", 400)
		return
	}

	// Find the workload
	a.stateMu.RLock()
	workload := a.state.Workloads[meshIP]
	var owner *Peer
	if workload != nil && workload.Owner != a.hwid {
		owner = a.state.Peers[workload.Owner]
	}
	a.stateMu.RUnlock()

	if workload == nil {
		http.Error(w, "workload not found", 404)
		return
	}

	var targetURL string
	// localTarget == true means we're about to send the request *into* a
	// workload container. Workload code is untrusted relative to the
	// cluster control plane, so we must strip the operator's API key /
	// dashboard cookies before handing the request over.
	localTarget := false

	if workload.Owner == a.hwid {
		// Local workload - forward to mesh IP directly (DNAT handles it)
		targetURL = fmt.Sprintf("http://%s%s", meshIP, targetPath)
		localTarget = true
	} else if owner != nil {
		// Remote workload - forward to owner node
		if a.tunnelDomain != "" {
			// Tunnel mode: use tunnel domain with proxy path
			targetURL = fmt.Sprintf("https://%s/api/proxy/%s%s", a.tunnelDomain, meshIP, targetPath)
		} else {
			// Direct mode: use owner's mesh IP
			targetURL = fmt.Sprintf("http://%s:%d/api/proxy/%s%s", owner.IP, a.apiPort, meshIP, targetPath)
		}
	} else {
		http.Error(w, "workload owner not found", 503)
		return
	}

	// Create proxy request
	proxyReq, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Copy headers
	for k, v := range r.Header {
		// Skip hop-by-hop headers
		if k == "Connection" || k == "Keep-Alive" || k == "Proxy-Connection" {
			continue
		}
		// Don't forward our auth credentials into workload containers -
		// those run untrusted code and would be able to steal the cluster
		// secret. Peers (remote-target) still need them to satisfy the
		// next hop's apiKeyMiddleware.
		if localTarget {
			lk := strings.ToLower(k)
			if lk == "x-api-key" || lk == "authorization" || lk == "cookie" {
				continue
			}
		}
		proxyReq.Header[k] = v
	}

	// Add forwarding headers
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	proxyReq.Header.Set("X-Forwarded-Host", r.Host)

	// Execute request
	resp, err := httpClient.Do(proxyReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("proxy error: %v", err), 502)
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)

	// Copy response body
	io.Copy(w, resp.Body)
}
