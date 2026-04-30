package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// =============================================================================
// HTTP Handlers - node management
// =============================================================================
//
//   GET    /api/nodes              list this node + healthy peers
//   DELETE /api/nodes/{id}         remove a peer; orphans its workloads
//   POST   /api/nodes/{id}/update  pull new image and restart that node
//
// getSelfContainerID is a helper used by apiUpdateNode("self") - it figures
// out which docker container is running this agent so the update path can
// rename / restart it.

// apiListNodes godoc
// @Summary List all nodes
// @Description Returns all nodes in the cluster (self + peers)
// @Tags nodes
// @Produce json
// @Success 200 {array} Peer
// @Router /nodes [get]
func (a *Agent) apiListNodes(w http.ResponseWriter, r *http.Request) {
	a.stateMu.RLock()
	nodes := []map[string]interface{}{
		{
			"id":        a.hwid,
			"name":      a.hostname,
			"ip":        a.ip,
			"healthy":   true,
			"last_seen": time.Now(),
			"is_self":   true,
			"version":   Version,
			"arch":      runtime.GOARCH,
		},
	}
	for _, p := range a.state.Peers {
		nodes = append(nodes, map[string]interface{}{
			"id":        p.ID,
			"name":      p.Name,
			"ip":        p.IP,
			"healthy":   p.Healthy,
			"last_seen": p.LastSeen,
			"is_self":   false,
			"version":   p.Version,
			"arch":      p.Arch,
		})
	}
	a.stateMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodes)
}
// apiRemoveNode godoc
// @Summary Remove a node from the cluster
// @Description Removes a peer node from the cluster. Workloads owned by that node will be orphaned and eligible for failover if revive is enabled.
// @Tags nodes
// @Produce json
// @Param id path string true "Node ID (HWID) or name"
// @Success 200 {object} map[string]string
// @Failure 400 {object} ErrorResponse "Cannot remove self"
// @Failure 404 {object} ErrorResponse "Node not found"
// @Router /nodes/{id} [delete]
func (a *Agent) apiRemoveNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["id"]

	// Can't remove self
	if nodeID == a.hwid || nodeID == a.hostname {
		http.Error(w, "cannot remove self from cluster", 400)
		return
	}

	a.stateMu.Lock()

	// Find peer by ID or name
	var found *Peer
	var foundID string
	for id, p := range a.state.Peers {
		if p.ID == nodeID || p.Name == nodeID {
			found = p
			foundID = id
			break
		}
	}

	if found == nil {
		a.stateMu.Unlock()
		http.Error(w, "node not found", 404)
		return
	}

	// Count workloads owned by this node
	var orphanedWorkloads []string
	for _, wl := range a.state.Workloads {
		if wl.Owner == found.ID {
			orphanedWorkloads = append(orphanedWorkloads, wl.Name)
		}
	}

	// Remove the peer
	delete(a.state.Peers, foundID)
	a.stateMu.Unlock()

	// Clean up IPIP tunnel to this peer
	a.removePeerTunnel(found.ID)

	// Clean up routes for workloads that were owned by this peer
	a.stateMu.Lock()
	a.updateWorkloadRoutes()
	a.stateMu.Unlock()

	a.updateHosts()
	a.saveState()
	a.broadcastState()

	log.Printf("Removed node: %s (%s), orphaned workloads: %v", found.Name, found.ID, orphanedWorkloads)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"removed":            found.Name,
		"id":                 found.ID,
		"orphaned_workloads": orphanedWorkloads,
		"message":            "node removed; orphaned workloads will failover if revive is enabled",
	})
}
// apiUpdateNode godoc
// @Summary Update a node (self-update)
// @Description Triggers a self-update of the node by pulling a new Docker image and restarting. Only works on the target node itself - requests to other nodes will be proxied.
// @Tags nodes
// @Accept json
// @Produce json
// @Param id path string true "Node ID (HWID), name, or 'self'"
// @Param request body NodeUpdateRequest true "Update request with image to pull"
// @Success 200 {object} NodeUpdateResponse
// @Failure 400 {object} ErrorResponse "Invalid request"
// @Failure 404 {object} ErrorResponse "Node not found"
// @Failure 500 {object} ErrorResponse "Update failed"
// @Router /nodes/{id}/update [post]
func (a *Agent) apiUpdateNode(w http.ResponseWriter, r *http.Request) {
	// Self-update pulls and runs an arbitrary docker image with the
	// agent's binds, capabilities, and --privileged. That's full root
	// RCE on every node in the cluster. Restrict to AdminKey - peer
	// keys must never be enough to do this. apiKeyMiddleware admits
	// peer keys for everything else, so the explicit gate here is what
	// prevents a compromised peer from owning the cluster.
	if !a.adminAuthorize(r) {
		http.Error(w, "unauthorized: admin key required for node update", http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(r)
	nodeID := vars["id"]

	// Parse request body
	var req struct {
		Image string `json:"image"` // Docker image to pull (e.g., "ghcr.io/ncwardell/jetty:2.1.0")
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", 400)
		return
	}

	if req.Image == "" {
		http.Error(w, "image is required", 400)
		return
	}

	// Check if this is for self or another node
	isSelf := nodeID == "self" || nodeID == a.hwid || nodeID == a.hostname

	if !isSelf {
		// Find the peer and proxy the request
		a.stateMu.RLock()
		var targetPeer *Peer
		for _, p := range a.state.Peers {
			if p.ID == nodeID || p.Name == nodeID {
				targetPeer = p
				break
			}
		}
		a.stateMu.RUnlock()

		if targetPeer == nil {
			http.Error(w, "node not found", 404)
			return
		}

		// Proxy request to target node. Forward the cluster AdminKey -
		// the receiver's apiUpdateNode also requires admin auth, and
		// peerRequest's SelfAPIKey would be rejected there. AdminKey is
		// gossiped cluster-wide so our local copy matches the peer's.
		proxyURL := fmt.Sprintf("http://%s:%d/api/nodes/self/update", targetPeer.IP, a.apiPort)
		reqBody, _ := json.Marshal(req)
		proxyReq, err := http.NewRequest("POST", proxyURL, strings.NewReader(string(reqBody)))
		if err != nil {
			http.Error(w, fmt.Sprintf("build proxy request: %v", err), 500)
			return
		}
		a.stateMu.RLock()
		adminKey := a.state.AdminKey
		a.stateMu.RUnlock()
		proxyReq.Header.Set("X-API-Key", adminKey)
		proxyReq.Header.Set("Content-Type", "application/json")

		resp, err := peerClient.Do(proxyReq)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to reach node: %v", err), 503)
			return
		}
		defer resp.Body.Close()

		// Forward response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Self-update: pull image and restart
	log.Printf("Starting self-update to image: %s", req.Image)

	// Step 1: Pull the new image
	pullCmd := exec.Command("docker", "pull", req.Image)
	pullOutput, err := pullCmd.CombinedOutput()
	if err != nil {
		log.Printf("Failed to pull image %s: %v\nOutput: %s", req.Image, err, pullOutput)
		http.Error(w, fmt.Sprintf("failed to pull image: %v", err), 500)
		return
	}
	log.Printf("Pulled image: %s", req.Image)

	// Step 2: Get our container ID
	containerID, err := a.getSelfContainerID()
	if err != nil {
		log.Printf("Failed to get own container ID: %v", err)
		http.Error(w, fmt.Sprintf("failed to get container ID: %v", err), 500)
		return
	}
	log.Printf("Self container ID: %s", containerID)

	// Step 3: Inspect self to get mounts and config
	inspectCmd := exec.Command("docker", "inspect", containerID)
	inspectOutput, err := inspectCmd.Output()
	if err != nil {
		log.Printf("Failed to inspect container: %v", err)
		http.Error(w, fmt.Sprintf("failed to inspect container: %v", err), 500)
		return
	}

	var containers []struct {
		HostConfig struct {
			Binds       []string `json:"Binds"`
			NetworkMode string   `json:"NetworkMode"`
			Privileged  bool     `json:"Privileged"`
			CapAdd      []string `json:"CapAdd"`
		} `json:"HostConfig"`
		Config struct {
			Env []string `json:"Env"`
		} `json:"Config"`
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(inspectOutput, &containers); err != nil {
		log.Printf("Failed to parse inspect output: %v", err)
		http.Error(w, fmt.Sprintf("failed to parse container config: %v", err), 500)
		return
	}

	if len(containers) == 0 {
		http.Error(w, "container not found", 500)
		return
	}

	container := containers[0]
	containerName := strings.TrimPrefix(container.Name, "/")

	// Step 4: Build docker run command for new container
	args := []string{"run", "-d", "--name", containerName + "-update"}

	// Add bind mounts (WARP state is persisted in /data/warp via symlink)
	hasLibModules := false
	for _, bind := range container.HostConfig.Binds {
		args = append(args, "-v", bind)
		if strings.HasPrefix(bind, "/lib/modules:") {
			hasLibModules = true
		}
	}

	// Ensure /lib/modules is mounted for kernel module loading (IPIP/GRE tunnels)
	if !hasLibModules {
		args = append(args, "-v", "/lib/modules:/lib/modules:ro")
	}

	// Add network mode
	if container.HostConfig.NetworkMode != "" {
		args = append(args, "--network", string(container.HostConfig.NetworkMode))
	}

	// Add privileged if set
	if container.HostConfig.Privileged {
		args = append(args, "--privileged")
	}

	// Add capabilities
	for _, cap := range container.HostConfig.CapAdd {
		args = append(args, "--cap-add", cap)
	}

	// Pass the cluster admin key to the new container so JETTY_SECRET
	// is non-empty after restart. Use the persisted state value (not
	// a.clusterSecret env), because joiners receive AdminKey via
	// /api/join and don't have JETTY_SECRET in their env. State is
	// also mounted via the data volume, so on a normal upgrade the
	// new container would read AdminKey from state.json anyway - this
	// just keeps the env consistent with what the operator originally
	// set, useful if state is ever wiped/restored separately.
	a.stateMu.RLock()
	adminKey := a.state.AdminKey
	a.stateMu.RUnlock()
	if adminKey != "" {
		args = append(args, "-e", "JETTY_SECRET="+adminKey)
	}

	// Add restart policy
	args = append(args, "--restart", "unless-stopped")

	// Add the image
	args = append(args, req.Image)

	log.Printf("Creating new container with: docker %v", args)

	// Step 5: Create new container (but don't start it yet - avoids port conflict)
	// Change "run" to "create" so we can control when it starts
	args[0] = "create" // Replace "run" with "create"
	// Remove -d flag since create doesn't use it
	newArgs := make([]string, 0, len(args))
	for _, arg := range args {
		if arg != "-d" {
			newArgs = append(newArgs, arg)
		}
	}

	createCmd := exec.Command("docker", newArgs...)
	createOutput, err := createCmd.CombinedOutput()
	if err != nil {
		log.Printf("Failed to create new container: %v\nOutput: %s", err, createOutput)
		http.Error(w, fmt.Sprintf("failed to create new container: %v", err), 500)
		return
	}
	newContainerID := strings.TrimSpace(string(createOutput))
	log.Printf("Created new container (not started): %s", newContainerID)

	// Respond before stopping self - the actual switch happens in the goroutine
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "updating",
		"message": "new container created, switching over",
		"image":   req.Image,
		"old_id":  containerID,
		"new_id":  newContainerID,
	})

	// Step 6: Perform the actual switch in a goroutine after response is sent
	// CRITICAL: We spawn a helper container to orchestrate the restart because
	// when we call "docker stop" on ourselves, this process dies before it can
	// start the new container. The helper runs independently and survives our death.
	go func() {
		time.Sleep(500 * time.Millisecond) // Give time for response to be sent

		// Rename old container first (while it's still running)
		log.Printf("Renaming old container %s to %s-old", containerName, containerName)
		renameOldCmd := exec.Command("docker", "rename", containerName, containerName+"-old")
		if out, err := renameOldCmd.CombinedOutput(); err != nil {
			log.Printf("Warning: failed to rename old container: %v, output: %s", err, out)
		}

		// Rename new container to original name (while it's stopped)
		log.Printf("Renaming new container to %s", containerName)
		renameNewCmd := exec.Command("docker", "rename", containerName+"-update", containerName)
		if out, err := renameNewCmd.CombinedOutput(); err != nil {
			log.Printf("Warning: failed to rename new container: %v, output: %s", err, out)
		}

		// Spawn a helper container to do the actual stop/start sequence
		// This helper runs independently and will complete even after we're killed
		// Use the same image we just pulled - it has docker CLI installed
		// The script: stops old container, starts new one, cleans up old container
		restartScript := fmt.Sprintf(
			"sleep 1 && docker stop -t 5 %s && docker start %s && docker rm %s",
			containerID, newContainerID, containerID)

		log.Printf("Spawning helper container to orchestrate restart")
		helperCmd := exec.Command("docker", "run", "--rm", "-d",
			"--entrypoint", "sh",
			"-v", "/var/run/docker.sock:/var/run/docker.sock",
			req.Image, "-c", restartScript)

		if out, err := helperCmd.CombinedOutput(); err != nil {
			log.Printf("Warning: failed to spawn helper container: %v, output: %s", err, out)
			// Fallback: try docker:cli image (smaller, commonly available)
			log.Printf("Trying docker:cli as fallback helper...")
			helperCmd = exec.Command("docker", "run", "--rm", "-d",
				"-v", "/var/run/docker.sock:/var/run/docker.sock",
				"docker:cli", "sh", "-c", restartScript)
			if out, err := helperCmd.CombinedOutput(); err != nil {
				log.Printf("Warning: docker:cli also failed: %v, output: %s", err, out)
				log.Printf("Falling back to direct restart (may fail)")

				// Last resort: non-blocking commands
				log.Printf("Stopping old container %s", containerID)
				stopCmd := exec.Command("docker", "stop", "-t", "5", containerID)
				stopCmd.Start() // Non-blocking - don't wait

				time.Sleep(100 * time.Millisecond)
				log.Printf("Starting new container %s", newContainerID)
				startCmd := exec.Command("docker", "start", newContainerID)
				startCmd.Start() // Non-blocking
			} else {
				log.Printf("Helper container (docker:cli) spawned successfully, exiting...")
			}
		} else {
			log.Printf("Helper container spawned successfully, exiting...")
		}

		// Give commands time to be sent to Docker daemon before we exit
		time.Sleep(200 * time.Millisecond)
		os.Exit(0)
	}()
}
// getSelfContainerID returns this container's ID
func (a *Agent) getSelfContainerID() (string, error) {
	// Method 1: Check hostname (often the container ID)
	hostname, _ := os.Hostname()
	if len(hostname) == 12 || len(hostname) == 64 {
		// Verify it's a valid container ID
		cmd := exec.Command("docker", "inspect", hostname, "--format", "{{.Id}}")
		if out, err := cmd.Output(); err == nil {
			return strings.TrimSpace(string(out)), nil
		}
	}

	// Method 2: Look for container named exactly "jetty" that's running
	// Use ^jetty$ to match exactly - prevents matching workload containers like jetty_nginx
	cmd := exec.Command("docker", "ps", "-q", "--filter", "name=^jetty$", "--filter", "status=running")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("docker ps failed: %w", err)
	}

	containers := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(containers) == 0 || containers[0] == "" {
		return "", fmt.Errorf("no running jetty container found")
	}

	// If multiple, find the one whose PID 1 matches our process tree
	if len(containers) == 1 {
		return containers[0], nil
	}

	// Method 3: Check /proc/1/cpuset for container ID
	cpuset, err := os.ReadFile("/proc/1/cpuset")
	if err == nil {
		// Format: /docker/<container_id>
		parts := strings.Split(strings.TrimSpace(string(cpuset)), "/")
		if len(parts) >= 3 && parts[1] == "docker" {
			return parts[2], nil
		}
	}

	// Method 4: Check cgroup
	cgroup, err := os.ReadFile("/proc/self/cgroup")
	if err == nil {
		lines := strings.Split(string(cgroup), "\n")
		for _, line := range lines {
			// Look for docker in the cgroup path
			if strings.Contains(line, "docker") {
				parts := strings.Split(line, "/")
				for _, p := range parts {
					if len(p) == 64 {
						return p, nil
					}
				}
			}
		}
	}

	// Return first container as fallback
	return containers[0], nil
}
