package agent

import (
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// =============================================================================
// Docker Compose
// =============================================================================

// autostartWorkloads starts all owned workloads that have autostart enabled
func (a *Agent) autostartWorkloads() {
	a.stateMu.RLock()
	var toStart []*Workload
	for _, wl := range a.state.Workloads {
		if wl.Owner == a.hwid && wl.Autostart {
			toStart = append(toStart, wl)
		}
	}
	a.stateMu.RUnlock()

	if len(toStart) == 0 {
		return
	}

	log.Printf("Auto-starting %d workload(s)...", len(toStart))
	for _, wl := range toStart {
		if err := a.deployWorkload(wl); err != nil {
			log.Printf("Failed to auto-start %s: %v", wl.Name, err)
		}
	}
}

// getComposeForArch returns the appropriate compose content for the current architecture.
// Priority: arch-specific compose (compose_amd64/compose_arm64) > default compose
func (wl *Workload) getComposeForArch() string {
	arch := runtime.GOARCH
	switch arch {
	case "amd64":
		if wl.ComposeAmd64 != "" {
			return wl.ComposeAmd64
		}
	case "arm64":
		if wl.ComposeArm64 != "" {
			return wl.ComposeArm64
		}
	}
	// Fall back to default compose
	return wl.Compose
}

// canRunOnArch checks if a workload has a compose file that can run on the given architecture.
// Returns true if:
// - The default Compose field is set (works as fallback for any arch), OR
// - An architecture-specific compose file exists for the given arch
func (wl *Workload) canRunOnArch(arch string) bool {
	// If there's a default compose, it can run on any architecture
	if wl.Compose != "" {
		return true
	}
	// Otherwise, check for arch-specific compose
	switch arch {
	case "amd64":
		return wl.ComposeAmd64 != ""
	case "arm64":
		return wl.ComposeArm64 != ""
	}
	// Unknown architecture - no compose available
	return false
}

func (a *Agent) deployWorkload(wl *Workload) error {
	dir := filepath.Join(a.composeDir, wl.Name)
	os.MkdirAll(dir, 0755)

	// Select compose based on node architecture
	composeContent := wl.getComposeForArch()
	if composeContent == "" {
		return fmt.Errorf("no compose file available for architecture %s", runtime.GOARCH)
	}

	// Write user's compose verbatim
	path := filepath.Join(dir, "docker-compose.yml")
	if err := os.WriteFile(path, []byte(composeContent), 0644); err != nil {
		return err
	}

	// Generate the extra_hosts override so containers can resolve other
	// workloads by name. See agent/composeoverride.go for the why.
	if err := a.refreshHostsOverride(wl.Name, []byte(composeContent)); err != nil {
		// Non-fatal: workload still works, just no cross-workload DNS.
		log.Printf("Warning: failed to write hosts override for %s: %v", wl.Name, err)
	}

	// Validate
	if out, err := a.composeCmd(wl.Name, "config", "--quiet"); err != nil {
		return fmt.Errorf("invalid: %s", out)
	}

	// Pull, with retries on transient errors. Common transient failures:
	// registry hiccups, DNS blips, partial layer downloads. We don't fail
	// the deploy outright on a pull error - compose can still bring the
	// workload up if a recent enough image is cached locally - but we try
	// hard to refresh.
	a.pullWithRetry(wl.Name)

	// Up
	if out, err := a.composeCmd(wl.Name, "up", "-d", "--remove-orphans"); err != nil {
		return fmt.Errorf("failed: %s", out)
	}

	// Setup mesh IP routing
	if wl.IP != "" {
		a.setupWorkloadIP(wl)
	}

	log.Printf("Deployed: %s @ %s", wl.Name, wl.IP)
	return nil
}

// triggerPeerPrePull asks every allowed peer to pre-pull the workload's
// image(s) in the background, so that if this node dies and the workload
// fails over, the new owner's `docker compose pull` is an image-cache hit
// instead of a download. Best-effort and asynchronous - we don't wait for
// the peers to actually finish pulling.
//
// Called from the create/update workload paths after the compose file
// has been written and we know which nodes are allowed targets.
func (a *Agent) triggerPeerPrePull(wl *Workload) {
	a.stateMu.RLock()
	peers := make([]*Peer, 0)
	for _, p := range a.state.Peers {
		if p.IP == "" || !p.Healthy {
			continue
		}
		// Only nodes that can actually run this workload.
		if !a.isNodeAllowed(wl, p.ID, p.Name, p.Arch) {
			continue
		}
		peers = append(peers, p)
	}
	a.stateMu.RUnlock()

	if len(peers) == 0 {
		return
	}

	// Fire and forget. Each peer's /api/workloads/{name}/prepull endpoint
	// returns immediately and runs the pull in the background on that side.
	for _, p := range peers {
		go func(peer *Peer) {
			url := a.getPeerAPIURL(peer, "/api/workloads/"+wl.Name+"/prepull")
			req, _ := a.peerRequest("POST", url, nil)
			resp, err := httpClient.Do(req)
			if err != nil {
				log.Printf("Pre-pull request to %s failed: %v", peer.Name, err)
				return
			}
			resp.Body.Close()
		}(p)
	}
}

// prePullLocally writes the workload's compose file (if not already
// present) and runs `docker compose pull`. Used by the prepull endpoint:
// other nodes in the cluster ask us to warm our image cache.
//
// Doesn't run `up` - the workload isn't deployed locally, just its image
// is pulled into Docker's cache.
func (a *Agent) prePullLocally(wl *Workload) error {
	dir := filepath.Join(a.composeDir, wl.Name)
	os.MkdirAll(dir, 0755)

	composeContent := wl.getComposeForArch()
	if composeContent == "" {
		return fmt.Errorf("no compose file available for architecture %s", runtime.GOARCH)
	}
	path := filepath.Join(dir, "docker-compose.yml")
	if err := os.WriteFile(path, []byte(composeContent), 0644); err != nil {
		return err
	}
	// Best-effort pull. Don't block: failures here just mean the
	// failover-time pull won't be cache-hit.
	go a.pullWithRetry(wl.Name)
	return nil
}

// pullWithRetry runs `compose pull` with exponential backoff. Returns the
// output of the last attempt and a flag indicating success. Used by
// deployWorkload before bringing the workload up; also exported for the
// pre-pull path on potential failover targets.
//
// Three attempts: 0s, ~2s, ~6s backoff. We don't want to block the deploy
// path forever if a registry is genuinely down - compose's `up` can still
// succeed against a cached image.
func (a *Agent) pullWithRetry(workloadName string) (string, bool) {
	var lastOut string
	for i, delay := range []time.Duration{0, 2 * time.Second, 4 * time.Second} {
		if delay > 0 {
			time.Sleep(delay)
		}
		out, err := a.composeCmd(workloadName, "pull")
		lastOut = out
		if err == nil {
			if i > 0 {
				log.Printf("Pull for %s succeeded on attempt %d", workloadName, i+1)
			}
			return out, true
		}
		log.Printf("Pull for %s attempt %d failed: %v", workloadName, i+1, err)
	}
	log.Printf("Warning: pull for %s failed after retries; will rely on cached image", workloadName)
	return lastOut, false
}

// refreshHostsOverride parses the workload's compose to discover service
// names, then writes a docker-compose.override.yml with extra_hosts for
// every workload-name -> mesh-IP mapping in the cluster.
//
// Called from deployWorkload (with the just-written user compose bytes)
// and from refreshAllOwnedHostsOverrides (after cluster state changes,
// reading the on-disk compose).
func (a *Agent) refreshHostsOverride(workloadName string, composeBytes []byte) error {
	dir := filepath.Join(a.composeDir, workloadName)
	overridePath := filepath.Join(dir, "docker-compose.override.yml")

	services, err := parseComposeServiceNames(composeBytes)
	if err != nil {
		return err
	}
	if len(services) == 0 {
		// User's compose has no services we can recognize - leave the
		// override absent rather than write a stub the user might find
		// confusing.
		os.Remove(overridePath)
		return nil
	}

	a.stateMu.RLock()
	hosts := a.currentWorkloadHosts()
	a.stateMu.RUnlock()

	_, err = writeHostsOverride(overridePath, services, hosts)
	return err
}

// refreshAllOwnedHostsOverrides regenerates the docker-compose.override.yml
// for every workload owned by this node, reading each on-disk compose to
// learn its service names. Called when the cluster's workload set changes
// so the override files stay current.
//
// We deliberately do NOT restart the running containers. Stable mesh IPs
// mean existing workloads can keep talking to each other through their
// pre-existing /etc/hosts entries; only newly-added workloads are missing,
// and the user can pick those up next time they bounce a workload (manual
// restart, image update, deploy, failover).
func (a *Agent) refreshAllOwnedHostsOverrides() {
	a.stateMu.RLock()
	owned := make([]string, 0)
	for _, wl := range a.state.Workloads {
		if wl.Owner == a.hwid {
			owned = append(owned, wl.Name)
		}
	}
	a.stateMu.RUnlock()

	for _, name := range owned {
		path := filepath.Join(a.composeDir, name, "docker-compose.yml")
		composeBytes, err := os.ReadFile(path)
		if err != nil {
			// Workload was deployed elsewhere or compose was deleted; skip.
			continue
		}
		if err := a.refreshHostsOverride(name, composeBytes); err != nil {
			log.Printf("Warning: failed to refresh hosts override for %s: %v", name, err)
		}
	}
}

// hostsOverrideReconcileLoop watches the cluster's workload-host map and
// regenerates compose overrides for our owned workloads when it changes.
// Runs at gossip-tick cadence; cheap when nothing has changed (just hashes
// the map and returns).
func (a *Agent) hostsOverrideReconcileLoop() {
	tick := time.NewTicker(GossipInterval)
	defer tick.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-tick.C:
			a.maybeRefreshHostsOverrides()
		}
	}
}

// maybeRefreshHostsOverrides hashes the current workload-host map; if it
// differs from the last seen value, regenerates override files for owned
// workloads. Idempotent and cheap.
func (a *Agent) maybeRefreshHostsOverrides() {
	a.stateMu.RLock()
	hosts := a.currentWorkloadHosts()
	a.stateMu.RUnlock()

	// Hash the sorted host map (sort for determinism).
	names := make([]string, 0, len(hosts))
	for n := range hosts {
		names = append(names, n)
	}
	sort.Strings(names)
	h := sha256.New()
	for _, n := range names {
		h.Write([]byte(n))
		h.Write([]byte{0})
		h.Write([]byte(hosts[n]))
		h.Write([]byte{0})
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))

	if sum == a.hostsOverrideHash {
		return
	}
	a.hostsOverrideHash = sum
	a.refreshAllOwnedHostsOverrides()
}

func (a *Agent) removeWorkload(wl *Workload) {
	// Clean up iptables BEFORE stopping container (need container IP)
	if wl.IP != "" {
		a.cleanupWorkloadIP(wl)
	}

	a.composeCmd(wl.Name, "down", "-v", "--remove-orphans")

	dir := filepath.Join(a.composeDir, wl.Name)
	os.RemoveAll(dir)

	log.Printf("Removed: %s", wl.Name)
}

func (a *Agent) cleanupWorkloadIP(wl *Workload) {
	// Get container IP before it's gone
	out, _ := exec.Command("docker", "ps", "-q", "-f", "label=com.docker.compose.project=jetty_"+wl.Name).Output()
	if len(out) > 0 {
		containerID := strings.Split(strings.TrimSpace(string(out)), "\n")[0]
		if containerID != "" {
			out, _ = exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerID).Output()
			containerIP := strings.TrimSpace(string(out))
			if containerIP != "" {
				// Remove DNAT rules
				exec.Command("iptables", "-t", "nat", "-D", "PREROUTING", "-d", wl.IP, "-j", "DNAT", "--to", containerIP).Run()
				exec.Command("iptables", "-t", "nat", "-D", "OUTPUT", "-d", wl.IP, "-j", "DNAT", "--to", containerIP).Run()
			}
		}
	}

	// Remove mesh IP from interface
	exec.Command("ip", "addr", "del", wl.IP+"/32", "dev", "jetty0").Run()
}

func (a *Agent) setupWorkloadIP(wl *Workload) {
	// Add IP to interface
	if err := exec.Command("ip", "addr", "add", wl.IP+"/32", "dev", "jetty0").Run(); err != nil {
		// Ignore "already exists" errors
		log.Printf("Note: adding %s to jetty0: %v (may already exist)", wl.IP, err)
	}

	// Wait for container to be ready with retry logic
	var containerIP string
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		containerIP = a.getWorkloadContainerIP(wl.Name)
		if containerIP != "" {
			break
		}
		if i < maxRetries-1 {
			time.Sleep(time.Duration(500*(i+1)) * time.Millisecond) // 500ms, 1s, 1.5s, ...
		}
	}

	if containerIP == "" {
		log.Printf("Error: couldn't get container IP for %s after %d retries", wl.Name, maxRetries)
		return
	}

	// Set up DNAT rules
	if err := exec.Command("iptables", "-t", "nat", "-A", "PREROUTING", "-d", wl.IP, "-j", "DNAT", "--to", containerIP).Run(); err != nil {
		log.Printf("Error: PREROUTING DNAT for %s: %v", wl.Name, err)
	}
	if err := exec.Command("iptables", "-t", "nat", "-A", "OUTPUT", "-d", wl.IP, "-j", "DNAT", "--to", containerIP).Run(); err != nil {
		log.Printf("Error: OUTPUT DNAT for %s: %v", wl.Name, err)
	}

	log.Printf("Routed: %s -> %s", wl.IP, containerIP)
}

// getWorkloadContainerIP returns the container IP for a workload, or empty string if not found.
func (a *Agent) getWorkloadContainerIP(name string) string {
	// Get container ID
	out, err := exec.Command("docker", "ps", "-q", "-f", "label=com.docker.compose.project=jetty_"+name).Output()
	if err != nil || len(out) == 0 {
		return ""
	}

	containerID := strings.Split(strings.TrimSpace(string(out)), "\n")[0]
	if containerID == "" {
		return ""
	}

	// Get container IP
	out, err = exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerID).Output()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(out))
}

func (a *Agent) composeCmd(name string, args ...string) (string, error) {
	dir := filepath.Join(a.composeDir, name)
	path := filepath.Join(dir, "docker-compose.yml")

	cmdArgs := []string{"compose", "-f", path, "-p", "jetty_" + name}

	// If we generated an extra_hosts override, layer it on. compose merges
	// successive -f files; later files take precedence on conflicting keys.
	overridePath := filepath.Join(dir, "docker-compose.override.yml")
	if _, err := os.Stat(overridePath); err == nil {
		cmdArgs = append(cmdArgs, "-f", overridePath)
	}

	cmdArgs = append(cmdArgs, args...)

	cmd := exec.Command("docker", cmdArgs...)
	cmd.Dir = dir

	// Inject decrypted environment variables
	// Start with current environment
	cmd.Env = os.Environ()

	// Add decrypted Jetty env vars (these can be used in docker-compose.yml)
	envData, err := a.getDecryptedEnv()
	if err != nil {
		log.Printf("Warning: failed to decrypt env data: %v", err)
	} else {
		for key, value := range envData {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", key, value))
		}
	}

	out, err := cmd.CombinedOutput()
	return string(out), err
}
