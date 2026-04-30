package agent

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

// =============================================================================
// WARP Lifecycle
// =============================================================================
//
// Cloudflare WARP is the underlay network for the cluster: every node connects
// to it and gets a 100.96.x.x IP, all of which are reachable to each other
// via Cloudflare's overlay. We use this as the "node mesh" - the layer over
// which our IPIP/GRE/userspace tunnels carry workload traffic.
//
// Two startup paths:
//   1. Bootstrap node: WARP is configured by the entrypoint script before the
//      agent starts. detectWarpIP just reads the assigned IP off the
//      CloudflareWARP interface.
//   2. Joining node: starts with no WARP token, joins via the cluster's tunnel
//      domain, receives a WARP connector token in the join response, then
//      configureWarpRuntime brings WARP up.
//
// WARP can renumber on reconnect, so ipMonitorLoop polls every 10s and
// re-announces our IP if it changes.

// detectWarpIP looks for the CloudflareWARP interface and stores its IPv4
// address on the Agent. Called at startup and whenever we suspect the IP may
// have changed.
//
// Honors JETTY_WARP_IP from the environment as an override - useful in
// container setups where the entrypoint script knows the IP earlier than
// the agent does.
func (a *Agent) detectWarpIP() {
	// Check environment variable first (set by entrypoint script)
	if ip := os.Getenv("JETTY_WARP_IP"); ip != "" {
		a.ip = ip
		log.Printf("WARP IP from environment: %s", ip)
		return
	}

	// Try to find CloudflareWARP interface
	iface, err := net.InterfaceByName("CloudflareWARP")
	if err != nil {
		// WARP not running
		return
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.To4() != nil {
			a.ip = ipnet.IP.String()
			log.Printf("WARP IP detected: %s", a.ip)
			return
		}
	}
}

// getWarpIP reads the current CloudflareWARP IPv4 address without mutating
// agent state. Used by ipMonitorLoop to detect changes.
func (a *Agent) getWarpIP() string {
	iface, err := net.InterfaceByName("CloudflareWARP")
	if err != nil {
		return ""
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return ""
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.To4() != nil {
			return ipnet.IP.String()
		}
	}
	return ""
}

// ipMonitorLoop polls every 10s for WARP IP changes. WARP renumbers on
// reconnect; when our IP changes we must:
//   - Update memberlist's advertised metadata so peers stop routing to the
//     stale address.
//   - Re-announce our IP via HTTP gossip (in case some peers haven't received
//     the memberlist update yet).
//   - Recompute our peer-to-peer IPIP/GRE tunnels (they're keyed on local IP).
func (a *Agent) ipMonitorLoop() {
	tick := time.NewTicker(IPMonitorInterval)
	defer tick.Stop()

	for {
		select {
		case <-a.stopCh:
			return
		case <-tick.C:
			newIP := a.getWarpIP()
			if newIP == "" || newIP == a.ip {
				continue
			}

			oldIP := a.ip
			a.ip = newIP
			log.Printf("WARP IP changed: %s -> %s", oldIP, newIP)

			// Notify memberlist of IP change (if using memberlist)
			a.updateMemberlistIP(newIP)

			// Re-announce our new IP to all peers (for HTTP-based gossip fallback)
			go a.announceOurIP()

			// Recreate IPIP/GRE tunnels with the new local IP. The tunnels embed
			// our local WARP IP in the encapsulation header, so a stale tunnel
			// produces packets with a stale outer source.
			a.stateMu.Lock()
			a.updateWorkloadRoutes()
			a.stateMu.Unlock()
		}
	}
}

// warpRegistrationKind queries the host's warp-cli to determine what
// kind of WARP registration is currently active.
//
// Returns one of:
//   "none"      - no registration ("Missing registration" from warp-cli)
//   "connector" - registered as a WARP Connector (mesh peer; what we want)
//   "consumer"  - registered as a regular WARP client (split-tunnel only,
//                 NOT reachable from other WARP peers)
//   "unknown"   - warp-cli not installed or returned something unexpected
//
// The connector vs consumer distinction matters because both create the
// same CloudflareWARP interface. The interface alone is not enough to
// know whether other peers can reach us - that depends on registration
// type. Connector registrations have a "Connector ID" line in
// "warp-cli registration show"; consumer registrations show
// account/email metadata instead.
func warpRegistrationKind() string {
	out, err := exec.Command("warp-cli", "--accept-tos", "registration", "show").CombinedOutput()
	s := string(out)
	if strings.Contains(s, "Missing registration") {
		return "none"
	}
	if err != nil {
		return "unknown"
	}
	if strings.Contains(s, "Connector") {
		return "connector"
	}
	return "consumer"
}

// ensureWarpConnector makes sure the host's WARP daemon is registered
// as a Connector using the given token.
//
//   - Already a connector: no-op (just nudge `connect` in case it's idle).
//   - Consumer mode: tear it down and re-register as Connector. The
//     operator's existing consumer-WARP install is replaced because a
//     cluster node MUST be reachable in both directions; consumer-only
//     means other nodes can't initiate to us.
//   - No registration: configure from scratch.
//
// Set JETTY_WARP_NO_TAKEOVER=true to opt out of the consumer→connector
// rewrite (e.g. if the operator wants to keep their consumer WARP and
// is OK with the cluster being one-way).
func (a *Agent) ensureWarpConnector(token string) error {
	if token == "" {
		return fmt.Errorf("no WARP Connector token available")
	}
	kind := warpRegistrationKind()

	if kind == "connector" {
		log.Printf("WARP already registered as Connector; ensuring connected")
		_ = exec.Command("warp-cli", "--accept-tos", "connect").Run()
		a.detectWarpIP()
		return nil
	}

	if kind == "consumer" {
		if strings.EqualFold(getEnv("JETTY_WARP_NO_TAKEOVER", "false"), "true") {
			log.Printf("!!! WARP is in consumer mode and JETTY_WARP_NO_TAKEOVER=true. Cluster mesh will only work in one direction (we can reach peers; they can NOT reach us). !!!")
			a.detectWarpIP()
			return nil
		}
		log.Printf("WARP is in consumer mode (%q). Replacing with Connector registration so the cluster mesh is bidirectional.",
			"WarpWithDnsOverHttps-style")
		_ = exec.Command("warp-cli", "--accept-tos", "disconnect").Run()
		// "registration delete" wipes the consumer registration so
		// "connector new" below can install ours without conflict.
		if out, err := exec.Command("warp-cli", "--accept-tos", "registration", "delete").CombinedOutput(); err != nil {
			log.Printf("Warning: warp-cli registration delete: %v (%s)", err, out)
		}
		// Clear the cached IP so post-configure detect picks up the new one.
		a.ip = ""
	}

	return a.configureWarpRuntime(token)
}

// configureWarpRuntime brings up WARP using a connector token received from
// the cluster join response. Called only on joining nodes - bootstrap nodes
// have WARP started by the entrypoint before the agent boots.
//
// Steps:
//  1. Start warp-svc if it isn't running.
//  2. Register the connector token (skip if already registered from a previous
//     boot - registration state persists in /data/warp).
//  3. Connect, then poll until status reports "connected" (up to 30s).
//  4. Detect the assigned IP, install our nft routing rules, and announce
//     to peers.
//
// Returns an error if WARP doesn't reach connected state in time. The
// caller (joinCluster) logs and continues - the agent will retry on the next
// peer announcement cycle.
func (a *Agent) configureWarpRuntime(token string) error {
	if token == "" {
		return fmt.Errorf("no WARP token provided")
	}

	log.Printf("Configuring WARP at runtime...")

	// Check if warp-svc is running, start it if not
	if err := exec.Command("warp-cli", "--accept-tos", "status").Run(); err != nil {
		log.Printf("Starting WARP service...")

		// RUST_LOG=error reduces verbose debug output that floods logs on
		// some distros (e.g., Arch). We don't capture stdout/stderr - the
		// service is meant to run quietly in the background.
		cmd := exec.Command("warp-svc", "--accept-tos")
		cmd.Env = append(os.Environ(), "RUST_LOG=error")
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("failed to start warp-svc: %w", err)
		}

		// Poll until warp-svc accepts CLI commands (typically <2s).
		for i := 0; i < 10; i++ {
			time.Sleep(time.Second)
			if exec.Command("warp-cli", "--accept-tos", "status").Run() == nil {
				break
			}
		}
	}

	// Skip registration if we already did it on a prior boot. Registration
	// state lives in /data/warp which is persisted across container restarts.
	output, _ := exec.Command("warp-cli", "--accept-tos", "registration", "show").CombinedOutput()
	if !strings.Contains(string(output), "Missing registration") {
		log.Printf("WARP already registered, connecting...")
	} else {
		log.Printf("Registering WARP connector...")
		cmd := exec.Command("warp-cli", "--accept-tos", "connector", "new", token)
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("failed to register WARP connector: %s", output)
		}
	}

	log.Printf("Connecting WARP...")
	if err := exec.Command("warp-cli", "--accept-tos", "connect").Run(); err != nil {
		return fmt.Errorf("failed to connect WARP: %w", err)
	}

	// Poll for connected status. Cloudflare can take a few seconds to set up
	// the tunnel, especially on first registration.
	for i := 0; i < 30; i++ {
		time.Sleep(time.Second)
		output, _ := exec.Command("warp-cli", "--accept-tos", "status").CombinedOutput()
		if strings.Contains(strings.ToLower(string(output)), "connected") {
			a.detectWarpIP()
			log.Printf("WARP connected successfully: %s", a.ip)

			if err := a.initWarpRules(); err != nil {
				log.Printf("Warning: failed to init WARP rules: %v", err)
			}

			// Tell peers our address so cross-node routing can begin.
			go a.announceOurIP()

			return nil
		}
	}

	return fmt.Errorf("WARP connection timeout")
}
