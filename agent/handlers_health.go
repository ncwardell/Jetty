package agent

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// HTTP Handlers - /api/health and host system metrics
// =============================================================================
//
//   GET /api/health        node and cluster health (?node=local for self only)
//
// cpuSampleLoop runs in the background and updates a.cachedCPUPercent so
// /api/health doesn't have to wait for a fresh read. getSystemStats reads
// /proc and computes load/memory/disk numbers. formatBytes and
// getHealthStatus are presentation helpers.

// apiHealth godoc
// @Summary Health check
// @Description Returns health status of this node or the entire cluster. Use node=local for just this node.
// @Tags cluster
// @Produce json
// @Param node query string false "Filter by node: 'local' for this node only, or node name/ID for specific node"
// @Success 200 {object} HealthResponse
// @Router /health [get]
func (a *Agent) apiHealth(w http.ResponseWriter, r *http.Request) {
	// /api/health is in publicPaths so unauthenticated probes work for
	// monitoring (load balancers, kuma, etc.). The rich payload below
	// (peer list, workload IPs, public IPs, system metrics) shouldn't
	// be exposed to anonymous callers - leak topology to anyone who
	// can reach the API. Authenticate the caller first; if no valid
	// key, return a minimal status only.
	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		apiKey = r.URL.Query().Get("api_key")
	}
	if !a.authorizeAPIKey(apiKey) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "ok",
			"version": Version,
		})
		return
	}

	nodeFilter := r.URL.Query().Get("node")

	// Get list of peers. Copy VALUES, not pointers: the goroutines below
	// read peer fields after this lock is released, while gossip/
	// memberlist writers mutate the same Peer structs in place under
	// stateMu - sharing pointers here is a data race.
	a.stateMu.RLock()
	peers := make([]*Peer, 0, len(a.state.Peers))
	for _, p := range a.state.Peers {
		snapshot := *p
		peers = append(peers, &snapshot)
	}
	a.stateMu.RUnlock()

	type NodeHealth struct {
		ID        string                 `json:"id"`
		Name      string                 `json:"name"`
		IP        string                 `json:"ip"`
		PublicIP  string                 `json:"public_ip,omitempty"`
		Healthy   bool                   `json:"healthy"`
		Status    string                 `json:"status"`
		Version   string                 `json:"version,omitempty"`
		Arch      string                 `json:"arch,omitempty"`
		Workloads []string               `json:"workloads"`
		System    map[string]interface{} `json:"system,omitempty"`
		Error     string                 `json:"error,omitempty"`
	}

	var results []NodeHealth
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Check if we should include this node
	// "local" is a special filter meaning "only this node"
	includeLocal := nodeFilter == "" || nodeFilter == "local" || nodeFilter == a.hwid || nodeFilter == a.hostname

	// Get local node health
	if includeLocal {
		// Gather workload info under lock, but run docker commands outside
		a.stateMu.RLock()
		type workloadInfo struct {
			name string
			ip   string
		}
		var localWLInfo []workloadInfo
		for _, wl := range a.state.Workloads {
			if wl.Owner == a.hwid {
				localWLInfo = append(localWLInfo, workloadInfo{name: wl.Name, ip: wl.IP})
			}
		}
		a.stateMu.RUnlock()

		// Check docker status outside the lock to avoid blocking
		var localWorkloads []string
		for _, wl := range localWLInfo {
			out, _ := exec.Command("docker", "ps", "-q", "-f", "label=com.docker.compose.project=jetty_"+wl.name).Output()
			status := "stopped"
			if len(strings.TrimSpace(string(out))) > 0 {
				status = "running"
			}
			localWorkloads = append(localWorkloads, fmt.Sprintf("%s:%s:%s", wl.name, wl.ip, status))
		}

		localHealth := NodeHealth{
			ID:        a.hwid,
			Name:      a.hostname,
			IP:        a.warpIP(),
			PublicIP:  a.publicIP,
			Healthy:   true,
			Status:    getHealthStatus(),
			Version:   Version,
			Arch:      runtime.GOARCH,
			Workloads: localWorkloads,
			System:    a.getSystemStats(),
		}
		results = append(results, localHealth)
	}

	// Fetch health from peers concurrently
	for _, peer := range peers {
		// Check node filter
		if nodeFilter != "" && nodeFilter != peer.ID && nodeFilter != peer.Name {
			continue
		}

		wg.Add(1)
		go func(p *Peer) {
			defer wg.Done()

			health := NodeHealth{
				ID:      p.ID,
				Name:    p.Name,
				IP:      p.IP,
				Healthy: p.Healthy,
				// Seed from the peer table; overwritten below if the
				// peer's own health response reports fresher values.
				Version: p.Version,
				Arch:    p.Arch,
			}

			// Use shorter timeout for unhealthy peers
			client := peerClient
			if !p.Healthy {
				client = unhealthyPeerClient
				health.Status = "unreachable"
				health.Error = "peer marked unhealthy"
			}

			// Skip if peer has no IP
			url := a.getPeerAPIURL(p, "/api/health?node=local")
			if url == "" {
				health.Status = "unreachable"
				health.Error = "no route to peer"
				mu.Lock()
				results = append(results, health)
				mu.Unlock()
				return
			}

			// Fetch health from peer. Use peerRequest so we authenticate
			// with our SelfAPIKey - without it, the receiving node's
			// /api/health returns only {status,version} (the M4
			// auth-gated minimal response) and we lose status, system
			// metrics, public_ip, workloads list, etc.
			req, err := a.peerRequest("GET", url, nil)
			if err != nil {
				health.Status = "unreachable"
				health.Error = err.Error()
				health.Healthy = false
				mu.Lock()
				results = append(results, health)
				mu.Unlock()
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				health.Status = "unreachable"
				health.Error = err.Error()
				health.Healthy = false
				mu.Lock()
				results = append(results, health)
				mu.Unlock()
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				health.Status = "error"
				health.Error = fmt.Sprintf("status %d", resp.StatusCode)
				mu.Lock()
				results = append(results, health)
				mu.Unlock()
				return
			}

			var peerHealth map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&peerHealth); err != nil {
				health.Status = "error"
				health.Error = "failed to decode response"
				mu.Lock()
				results = append(results, health)
				mu.Unlock()
				return
			}

			// Extract data from peer's local health response
			health.Healthy = true
			if nodes, ok := peerHealth["nodes"].([]interface{}); ok && len(nodes) > 0 {
				// Peer returned cluster format, extract first node's data
				if node, ok := nodes[0].(map[string]interface{}); ok {
					if status, ok := node["status"].(string); ok {
						health.Status = status
					}
					if pubIP, ok := node["public_ip"].(string); ok {
						health.PublicIP = pubIP
					}
					if wls, ok := node["workloads"].([]interface{}); ok {
						for _, wl := range wls {
							health.Workloads = append(health.Workloads, fmt.Sprintf("%v", wl))
						}
					}
					if sys, ok := node["system"].(map[string]interface{}); ok {
						health.System = sys
					}
					if v, ok := node["version"].(string); ok && v != "" {
						health.Version = v
					}
					if ar, ok := node["arch"].(string); ok && ar != "" {
						health.Arch = ar
					}
				}
			}

			mu.Lock()
			results = append(results, health)
			mu.Unlock()
		}(peer)
	}

	wg.Wait()

	// Calculate cluster summary
	totalNodes := len(results)
	healthyNodes := 0
	totalWorkloads := 0
	for _, h := range results {
		if h.Healthy && h.Status != "error" && h.Status != "unreachable" {
			healthyNodes++
		}
		totalWorkloads += len(h.Workloads)
	}

	clusterStatus := "healthy"
	if healthyNodes < totalNodes {
		if healthyNodes == 0 {
			clusterStatus = "degraded"
		} else {
			clusterStatus = "partial"
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"cluster_status":  clusterStatus,
		"total_nodes":     totalNodes,
		"healthy_nodes":   healthyNodes,
		"total_workloads": totalWorkloads,
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
		"nodes":           results,
	})
}

// cpuSampleLoop runs in the background and continuously samples CPU usage.
// This provides accurate CPU metrics without blocking API requests.
func (a *Agent) cpuSampleLoop() {
	getCPUTimes := func() (idle, total uint64) {
		if data, err := os.ReadFile("/proc/stat"); err == nil {
			lines := strings.Split(string(data), "\n")
			if len(lines) > 0 && strings.HasPrefix(lines[0], "cpu ") {
				fields := strings.Fields(lines[0])
				if len(fields) >= 5 {
					var sum uint64
					for i := 1; i < len(fields); i++ {
						val, _ := strconv.ParseUint(fields[i], 10, 64)
						sum += val
						// idle (field 4) and iowait (field 5) = CPU not working
						if i == 4 || i == 5 {
							idle += val
						}
					}
					total = sum
				}
			}
		}
		return
	}

	// Sample every 2 seconds for smooth, accurate readings
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var lastIdle, lastTotal uint64
	lastIdle, lastTotal = getCPUTimes()

	for {
		select {
		case <-ticker.C:
			idle, total := getCPUTimes()
			if total > lastTotal {
				idleDelta := float64(idle - lastIdle)
				totalDelta := float64(total - lastTotal)
				cpuPercent := (1.0 - idleDelta/totalDelta) * 100

				a.cachedCPUMu.Lock()
				a.cachedCPUPercent = cpuPercent
				a.cachedCPUMu.Unlock()
			}
			lastIdle, lastTotal = idle, total
		case <-a.stopCh:
			return
		}
	}
}

// getSystemStats returns CPU, memory, and disk statistics for the node
func (a *Agent) getSystemStats() map[string]interface{} {
	stats := make(map[string]interface{})

	// Get memory info from /proc/meminfo
	if memInfo, err := os.ReadFile("/proc/meminfo"); err == nil {
		var memTotal, memAvail, memFree, buffers, cached uint64
		lines := strings.Split(string(memInfo), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			value, _ := strconv.ParseUint(fields[1], 10, 64)
			switch fields[0] {
			case "MemTotal:":
				memTotal = value * 1024 // Convert KB to bytes
			case "MemAvailable:":
				memAvail = value * 1024
			case "MemFree:":
				memFree = value * 1024
			case "Buffers:":
				buffers = value * 1024
			case "Cached:":
				cached = value * 1024
			}
		}
		memUsed := memTotal - memAvail
		if memAvail == 0 {
			// Fallback for older kernels without MemAvailable
			memUsed = memTotal - memFree - buffers - cached
		}
		stats["memory_total"] = formatBytes(memTotal)
		stats["memory_used"] = formatBytes(memUsed)
		stats["memory_available"] = formatBytes(memAvail)
		if memTotal > 0 {
			stats["memory_percent"] = fmt.Sprintf("%.1f%%", float64(memUsed)/float64(memTotal)*100)
		}
	}

	// Get cached CPU usage (updated by background cpuSampleLoop)
	a.cachedCPUMu.RLock()
	cpuPercent := a.cachedCPUPercent
	a.cachedCPUMu.RUnlock()
	stats["cpu_percent"] = fmt.Sprintf("%.1f%%", cpuPercent)

	// Get load average
	if loadAvg, err := os.ReadFile("/proc/loadavg"); err == nil {
		fields := strings.Fields(string(loadAvg))
		if len(fields) >= 3 {
			stats["load_average"] = fmt.Sprintf("%s %s %s", fields[0], fields[1], fields[2])
		}
	}

	// Get disk usage for /data
	if out, err := exec.Command("df", "-B1", a.dataDir).Output(); err == nil {
		lines := strings.Split(string(out), "\n")
		if len(lines) >= 2 {
			fields := strings.Fields(lines[1])
			if len(fields) >= 5 {
				total, _ := strconv.ParseUint(fields[1], 10, 64)
				used, _ := strconv.ParseUint(fields[2], 10, 64)
				avail, _ := strconv.ParseUint(fields[3], 10, 64)
				stats["disk_total"] = formatBytes(total)
				stats["disk_used"] = formatBytes(used)
				stats["disk_available"] = formatBytes(avail)
				stats["disk_percent"] = fields[4]
			}
		}
	}

	// Get Jetty uptime (not system uptime)
	stats["uptime"] = time.Since(a.startTime).Round(time.Second).String()

	return stats
}

// formatBytes converts bytes to human-readable format
func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// getHealthStatus returns "healthy", "medium", or "full" based on resource usage
// healthy: CPU < 50% and memory < 60%
// medium: CPU 50-80% or memory 60-85%
// full: CPU > 80% or memory > 85%
func getHealthStatus() string {
	var memPercent, cpuPercent float64

	// Get memory percentage
	if memInfo, err := os.ReadFile("/proc/meminfo"); err == nil {
		var memTotal, memAvail, memFree, buffers, cached uint64
		lines := strings.Split(string(memInfo), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			value, _ := strconv.ParseUint(fields[1], 10, 64)
			switch fields[0] {
			case "MemTotal:":
				memTotal = value
			case "MemAvailable:":
				memAvail = value
			case "MemFree:":
				memFree = value
			case "Buffers:":
				buffers = value
			case "Cached:":
				cached = value
			}
		}
		memUsed := memTotal - memAvail
		if memAvail == 0 {
			memUsed = memTotal - memFree - buffers - cached
		}
		if memTotal > 0 {
			memPercent = float64(memUsed) / float64(memTotal) * 100
		}
	}

	// Get CPU percentage (quick sample)
	getCPUTimes := func() (idle, total uint64) {
		if data, err := os.ReadFile("/proc/stat"); err == nil {
			lines := strings.Split(string(data), "\n")
			if len(lines) > 0 && strings.HasPrefix(lines[0], "cpu ") {
				fields := strings.Fields(lines[0])
				if len(fields) >= 5 {
					var sum uint64
					for i := 1; i < len(fields); i++ {
						val, _ := strconv.ParseUint(fields[i], 10, 64)
						sum += val
						if i == 4 {
							idle = val
						}
					}
					total = sum
				}
			}
		}
		return
	}

	idle1, total1 := getCPUTimes()
	time.Sleep(50 * time.Millisecond) // Brief sample
	idle2, total2 := getCPUTimes()

	if total2 > total1 {
		idleDelta := float64(idle2 - idle1)
		totalDelta := float64(total2 - total1)
		cpuPercent = (1.0 - idleDelta/totalDelta) * 100
	}

	// Determine status
	if cpuPercent > 80 || memPercent > 85 {
		return "full"
	}
	if cpuPercent >= 50 || memPercent >= 60 {
		return "medium"
	}
	return "healthy"
}
