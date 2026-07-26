package agent

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// =============================================================================
// Docker Compose
// =============================================================================

// reconcileWorkloadsLoop periodically tries to deploy any owned,
// autostart-enabled workload whose containers aren't running. Two
// problems it addresses:
//
//  1. Cold-boot ordering. autostartWorkloads iterates the workload
//     map in random order, so a workload that mounts cluster-storage
//     can fire before cluster-storage itself is up - the CIFS mount
//     fails, compose up errors out, and the workload stays dead until
//     someone redeploys it manually. Reconcile retries every 30s.
//  2. Self-heal. Workloads that crash for any reason (out-of-memory,
//     image pull failure, transient docker error) get a retry pass
//     instead of being silently dead.
//
// deployWorkload's `compose up -d` is idempotent for already-running
// projects, so this loop is also safe to run against healthy
// workloads - it's just a no-op for them.
func (a *Agent) reconcileWorkloadsLoop() {
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-tick.C:
			a.reconcileWorkloads()
		}
	}
}

func (a *Agent) reconcileWorkloads() {
	a.stateMu.RLock()
	var toCheck []*Workload
	for _, wl := range a.state.Workloads {
		if wl.Owner == a.hwid && wl.Autostart {
			toCheck = append(toCheck, wl)
		}
	}
	a.stateMu.RUnlock()

	for _, wl := range toCheck {
		out, _ := exec.Command("docker", "ps", "-q",
			"-f", "label=com.docker.compose.project=jetty_"+wl.Name).Output()
		if len(strings.TrimSpace(string(out))) > 0 {
			continue // at least one container running - liveness handled by heal pass below
		}
		log.Printf("Reconcile: %s has no running containers, retrying deploy", wl.Name)
		if err := a.deployWorkload(wl); err != nil {
			log.Printf("Reconcile: deploy of %s failed: %v", wl.Name, err)
		}
	}

	// Built-in container autoheal: restart running-but-unhealthy containers.
	a.healUnhealthyContainers()
}

// healContainerCooldown bounds how often autoheal restarts the same container,
// so one that keeps coming up unhealthy isn't thrashed. Longer than a typical
// healthcheck start_period so a restart gets a fair chance to recover.
const healContainerCooldown = 120 * time.Second

// healUnhealthyContainers restarts running-but-*unhealthy* containers of the
// workloads we own. reconcileWorkloads only handles "no container running" (it
// treats any running container as fine); this covers the gap where a container
// is up but its Docker healthcheck is failing - e.g. an app whose inner process
// died (OOM, crash) while the container/supervisor stays alive. This is the
// built-in equivalent of an external autoheal sidecar, so operators don't need
// to run one alongside Jetty.
func (a *Agent) healUnhealthyContainers() {
	a.stateMu.RLock()
	owned := make([]string, 0, len(a.state.Workloads))
	for _, wl := range a.state.Workloads {
		if wl.Owner == a.hwid {
			owned = append(owned, wl.Name)
		}
	}
	a.stateMu.RUnlock()

	for _, name := range owned {
		out, _ := exec.Command("docker", "ps",
			"-f", "label=com.docker.compose.project=jetty_"+name,
			"-f", "health=unhealthy",
			"--format", "{{.ID}} {{.Names}}").Output()
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			fields := strings.Fields(line)
			cid := fields[0]
			cname := cid
			if len(fields) > 1 {
				cname = fields[1]
			}
			if !a.claimHeal(cid) {
				continue // healed recently; give it time to come back
			}
			log.Printf("Autoheal: restarting unhealthy container %s (workload %s)", cname, name)
			if rout, err := exec.Command("docker", "restart", cid).CombinedOutput(); err != nil {
				log.Printf("Autoheal: restart of %s failed: %v (%s)", cname, err, strings.TrimSpace(string(rout)))
			}
		}
	}
	a.gcHealTimes()
}

// claimHeal returns true if cid hasn't been auto-healed within the cooldown,
// recording the heal time when it does.
func (a *Agent) claimHeal(cid string) bool {
	a.healTimesMu.Lock()
	defer a.healTimesMu.Unlock()
	if last, ok := a.healTimes[cid]; ok && time.Since(last) < healContainerCooldown {
		return false
	}
	a.healTimes[cid] = time.Now()
	return true
}

// gcHealTimes drops stale cooldown entries so the map doesn't grow unbounded.
func (a *Agent) gcHealTimes() {
	a.healTimesMu.Lock()
	defer a.healTimesMu.Unlock()
	for cid, t := range a.healTimes {
		if time.Since(t) > 2*healContainerCooldown {
			delete(a.healTimes, cid)
		}
	}
}

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
	// Remove every DNAT rule referencing this workload's mesh IP. We can't
	// just `-D` with the same args we added because a single workload now
	// produces one rule per (container, port) pair, so we don't know the
	// full set up front. Walking `iptables -S` and deleting every match is
	// idempotent and survives upgrades from the old single-rule layout.
	a.removeWorkloadDNAT(wl.IP)

	// Remove mesh IP from interface
	exec.Command("ip", "addr", "del", wl.IP+"/32", "dev", "jetty0").Run()
}

// removeWorkloadDNAT deletes every DNAT rule in the nat table that targets
// the given mesh IP across PREROUTING and OUTPUT.
func (a *Agent) removeWorkloadDNAT(meshIP string) {
	for _, chain := range []string{"PREROUTING", "OUTPUT"} {
		out, err := exec.Command("iptables", "-t", "nat", "-S", chain).Output()
		if err != nil {
			continue
		}
		needle := "-d " + meshIP + "/32 "
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.HasPrefix(line, "-A ") || !strings.Contains(line, needle) {
				continue
			}
			args := strings.Fields(line)
			if len(args) < 2 {
				continue
			}
			args[0] = "-D"
			cmd := append([]string{"-t", "nat"}, args...)
			exec.Command("iptables", cmd...).Run()
		}
	}
}

func (a *Agent) setupWorkloadIP(wl *Workload) {
	// Add IP to interface
	if err := exec.Command("ip", "addr", "add", wl.IP+"/32", "dev", "jetty0").Run(); err != nil {
		// Ignore "already exists" errors
		log.Printf("Note: adding %s to jetty0: %v (may already exist)", wl.IP, err)
	}

	// Tear down any pre-existing DNAT rules for this mesh IP - on a redeploy
	// the old rules still point at the previous container's bridge IP, which
	// may have been recycled. Without this we accumulate stale rules and
	// the kernel uses whichever it matches first.
	a.removeWorkloadDNAT(wl.IP)

	// Enumerate every container's published ports and DNAT each one to its
	// owner container's bridge IP. A workload's compose can publish ports
	// from multiple services (e.g. cliproxy on :8317 and open-webui on :8080
	// in the same workload); per-port DNAT lets each service be reachable
	// on the same mesh IP at its own port.
	var ports []containerPort
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		ports = a.getWorkloadContainerPorts(wl.Name)
		if len(ports) > 0 {
			break
		}
		if i < maxRetries-1 {
			time.Sleep(time.Duration(500*(i+1)) * time.Millisecond)
		}
	}

	if len(ports) > 0 {
		for _, p := range ports {
			target := fmt.Sprintf("%s:%d", p.bridgeIP, p.port)
			// -I (insert at top) rather than -A (append): the OUTPUT chain
			// starts with Docker's own DOCKER jump for LOCAL-typed
			// destinations. Mesh IPs are bound to jetty0 and therefore look
			// LOCAL, so any host-published port from another workload (e.g.
			// hermes publishing 8080:8080) would have its DOCKER rule hijack
			// our mesh traffic to whichever container claimed that host port.
			// Inserting at the top ensures our per-port DNAT always wins.
			for _, chain := range []string{"PREROUTING", "OUTPUT"} {
				err := exec.Command("iptables", "-t", "nat", "-I", chain, "1",
					"-d", wl.IP, "-p", p.proto, "--dport", strconv.Itoa(p.port),
					"-j", "DNAT", "--to", target).Run()
				if err != nil {
					log.Printf("Error: %s DNAT for %s %s/%d -> %s: %v",
						chain, wl.IP, p.proto, p.port, target, err)
				}
			}
			log.Printf("Routed: %s:%d/%s -> %s", wl.IP, p.port, p.proto, target)
		}
		return
	}

	// Fallback: no container publishes ports. Pick the first container and
	// route all traffic to its bridge IP (preserves the original behavior
	// for worker-only workloads that don't expose anything explicitly).
	containerIP := a.getWorkloadContainerIP(wl.Name)
	if containerIP == "" {
		log.Printf("Error: couldn't get container IP for %s after %d retries", wl.Name, maxRetries)
		return
	}
	for _, chain := range []string{"PREROUTING", "OUTPUT"} {
		if err := exec.Command("iptables", "-t", "nat", "-I", chain, "1",
			"-d", wl.IP, "-j", "DNAT", "--to", containerIP).Run(); err != nil {
			log.Printf("Error: %s DNAT for %s: %v", chain, wl.IP, err)
		}
	}
	log.Printf("Routed: %s -> %s (catch-all, no published ports)", wl.IP, containerIP)
}

// containerPort describes one published port owned by one container in a workload.
type containerPort struct {
	bridgeIP string
	port     int
	proto    string // "tcp" or "udp"
}

// getWorkloadContainerPorts returns every published (host:container) port
// across every container in the workload's compose project, paired with that
// container's bridge IP. Used to set up per-port DNAT so multi-service
// workloads can expose more than one port on the same mesh IP.
func (a *Agent) getWorkloadContainerPorts(name string) []containerPort {
	out, err := exec.Command("docker", "ps",
		"-f", "label=com.docker.compose.project=jetty_"+name,
		"--format", "{{.ID}}\t{{.Ports}}",
	).Output()
	if err != nil || len(out) == 0 {
		return nil
	}

	var result []containerPort
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) < 2 || parts[0] == "" {
			continue
		}
		id := parts[0]
		ports := parts[1]

		ipOut, ipErr := exec.Command("docker", "inspect", "-f",
			"{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", id).Output()
		if ipErr != nil {
			continue
		}
		bridgeIP := strings.TrimSpace(string(ipOut))
		if bridgeIP == "" {
			continue
		}

		// Docker ps "Ports" looks like:
		//   "0.0.0.0:8080->80/tcp, [::]:8080->80/tcp, 9000/tcp"
		// We want the entries with "->" (published, externally reachable)
		// and the *container-side* port (right of "->", before "/").
		// Dedupe v4/v6 duplicates by port/proto.
		seen := map[string]bool{}
		for _, p := range strings.Split(ports, ",") {
			p = strings.TrimSpace(p)
			arrowIdx := strings.Index(p, "->")
			if arrowIdx < 0 {
				continue
			}
			rest := p[arrowIdx+2:]
			slashIdx := strings.Index(rest, "/")
			if slashIdx < 0 {
				continue
			}
			portStr := rest[:slashIdx]
			proto := rest[slashIdx+1:]
			if proto != "tcp" && proto != "udp" {
				continue
			}
			key := portStr + "/" + proto
			if seen[key] {
				continue
			}
			seen[key] = true

			port, err := strconv.Atoi(portStr)
			if err != nil {
				continue
			}
			result = append(result, containerPort{
				bridgeIP: bridgeIP,
				port:     port,
				proto:    proto,
			})
		}
	}
	return result
}

// getWorkloadContainerIP returns the container IP for a workload, or empty string if not found.
//
// Multi-container compose (e.g. an app + an init sidecar + a sync sidecar) used
// to be a coin flip: docker ps returned all of them and we picked whichever
// happened to be first by creation time. That landed traffic on the sidecar with
// no listener and produced "connection refused" downstream. Prefer the container
// that publishes a host port - that's structurally always the "main" service in
// a Jetty workload (it's how external traffic reaches it). Fall back to first
// container if no service publishes ports (rare; e.g. a worker-only workload).
func (a *Agent) getWorkloadContainerIP(name string) string {
	return a.getWorkloadContainerIPForPort(name, 0)
}

// getWorkloadContainerIPForPort returns the docker network IP of the container
// in workload `name` that publishes host port `port`. Kept as a wrapper for
// callers that only need the IP; the tunnel proxy uses
// getWorkloadContainerTargetForPort to also learn the container-side port.
func (a *Agent) getWorkloadContainerIPForPort(name string, port uint16) string {
	ip, _ := a.getWorkloadContainerTargetForPort(name, port)
	return ip
}

// getWorkloadContainerTargetForPort resolves where tunneled traffic for
// (workload, host port) should actually be dialed: the bridge IP of the
// container that PUBLISHES that host port, plus the CONTAINER-SIDE port of
// the mapping.
//
// docker ps "Ports" looks like "0.0.0.0:8222->80/tcp, [::]:8222->80/tcp":
// the number left of "->" is the HOST port, right of "->" is the container
// port. Two historical bugs lived here, both fatal for asymmetric mappings
// like vaultwarden's 8222:80:
//
//  1. the per-port container match used "->PORT/" - that matches the
//     CONTAINER side, so a lookup for host port 8222 never matched
//     "...:8222->80/tcp" and fell back to "first published container";
//  2. callers then dialed containerIP:HOSTport (8222) - but the container
//     listens on its container port (80) - producing an instant
//     connection-refused that the tunnel converted into RST+ACK back to
//     the caller. (Kernel DNAT translates the port correctly, which is
//     why the same workload responds fine to local traffic.)
//
// Symmetric mappings (8080:8080) dodged both bugs, which is how this
// survived until the first asymmetric workload crossed the tunnel.
//
// Returns ("", 0) when nothing matches at all. When the host port has no
// published mapping (port==0 or unpublished), falls back to the first
// container with any publication (then first container), returning
// containerPort==0 - callers should dial the original destination port in
// that case, preserving legacy behaviour.
func (a *Agent) getWorkloadContainerTargetForPort(name string, port uint16) (string, uint16) {
	out, err := exec.Command("docker", "ps",
		"-f", "label=com.docker.compose.project=jetty_"+name,
		"--format", "{{.ID}}\t{{.Ports}}",
	).Output()
	if err != nil || len(out) == 0 {
		return "", 0
	}

	hostMarker := ""
	if port != 0 {
		// Match the HOST side of a publication: "...:8222->..."
		hostMarker = fmt.Sprintf(":%d->", port)
	}

	var containerID, publishedID, fallbackID string
	var containerPort uint16
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) < 1 || parts[0] == "" {
			continue
		}
		id := parts[0]
		ports := ""
		if len(parts) == 2 {
			ports = parts[1]
		}
		if fallbackID == "" {
			fallbackID = id
		}
		// "->" indicates a host port publication (e.g. "0.0.0.0:80->80/tcp").
		// Internal-only EXPOSE doesn't contain "->".
		if strings.Contains(ports, "->") && publishedID == "" {
			publishedID = id
		}
		if hostMarker == "" {
			continue
		}
		// Find the mapping entry for this host port and extract the
		// container-side port from it.
		for _, entry := range strings.Split(ports, ",") {
			entry = strings.TrimSpace(entry)
			idx := strings.Index(entry, hostMarker)
			if idx < 0 {
				continue
			}
			rest := entry[idx+len(hostMarker):]
			slash := strings.Index(rest, "/")
			if slash < 0 {
				continue
			}
			if cp, perr := strconv.Atoi(rest[:slash]); perr == nil && cp > 0 && cp <= 65535 {
				containerID = id
				containerPort = uint16(cp)
			}
			break
		}
		if containerID != "" {
			break
		}
	}

	if containerID == "" {
		containerID = publishedID
		containerPort = 0 // unknown mapping - caller keeps original dst port
	}
	if containerID == "" {
		containerID = fallbackID
	}
	if containerID == "" {
		return "", 0
	}

	out, err = exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerID).Output()
	if err != nil {
		return "", 0
	}
	return strings.TrimSpace(string(out)), containerPort
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

	// If this workload defines registry_auth, materialize a throwaway
	// DOCKER_CONFIG dir holding just its credential and point this command
	// at it. Per-workload isolation means two workloads can pull from the
	// same registry host (e.g. ghcr.io) under different accounts without
	// colliding in a shared config.json. Reuses envData so we don't decrypt
	// twice. No-op (and DOCKER_CONFIG untouched) when registry_auth is unset.
	if wl := a.workloadByName(name); wl != nil && wl.RegistryAuth != nil {
		if cfgDir, cfgErr := writeRegistryConfig(wl.RegistryAuth, envData); cfgErr != nil {
			log.Printf("Warning: registry auth for %s: %v", name, cfgErr)
		} else {
			defer os.RemoveAll(cfgDir)
			cmd.Env = append(cmd.Env, "DOCKER_CONFIG="+cfgDir)
		}
	}

	out, err := cmd.CombinedOutput()
	return string(out), err
}

// workloadByName returns the workload with the given name, or nil. Workloads
// are keyed by mesh IP in state, so this is a linear scan - fine at the scale
// of a single node's workload count.
func (a *Agent) workloadByName(name string) *Workload {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			return wl
		}
	}
	return nil
}

// writeRegistryConfig resolves a workload's RegistryAuth into a temporary
// DOCKER_CONFIG directory containing a config.json with the registry
// credential, and returns the directory path. The caller owns the directory
// and must remove it. envData is the already-decrypted env store; the token
// is looked up by RegistryAuth.TokenRef.
//
// The config.json (which holds the base64 user:token, reversibly) only exists
// on disk for the duration of the docker command, with 0600 perms.
func writeRegistryConfig(ra *RegistryAuth, envData map[string]string) (string, error) {
	if ra.Registry == "" || ra.TokenRef == "" {
		return "", fmt.Errorf("registry_auth requires registry and token_ref")
	}
	token, ok := envData[ra.TokenRef]
	if !ok {
		return "", fmt.Errorf("token_ref %q not found in env store", ra.TokenRef)
	}
	user := ra.Username
	if user == "" {
		// GHCR and most registries accept any non-empty username alongside a
		// PAT; "x-access-token" is GitHub's documented convention.
		user = "x-access-token"
	}
	auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + token))
	cfg := map[string]any{
		"auths": map[string]any{
			ra.Registry: map[string]any{"auth": auth},
		},
	}
	blob, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	dir, err := os.MkdirTemp("", "jetty-dockercfg-")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), blob, 0600); err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	return dir, nil
}
