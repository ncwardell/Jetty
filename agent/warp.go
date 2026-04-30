package agent

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
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

// parseConnectorTokenID returns the connector-ID UUID embedded in a
// Cloudflare WARP Connector token. The token is the base64 encoding of
//
//	{"a":"<account>","t":"<connector-id>","s":"<secret>"}
//
// Returns "" on parse failure - callers should treat that as "can't
// reconcile" and fall back to "trust whatever's already registered".
// We never log the secret half; only the connector ID round-trips
// through this code.
func parseConnectorTokenID(token string) string {
	if token == "" {
		return ""
	}
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		// Some operators paste URL-safe variants - try that as a
		// fallback before giving up.
		decoded, err = base64.URLEncoding.DecodeString(token)
		if err != nil {
			return ""
		}
	}
	var parsed struct {
		T string `json:"t"`
	}
	if err := json.Unmarshal(decoded, &parsed); err != nil {
		return ""
	}
	return parsed.T
}

// deregisterWarp tells warp-svc to release its current registration
// with Cloudflare, freeing the device slot in the team's connector
// registry. Called on graceful shutdown so a planned restart of the
// agent doesn't leak a dangling device on Cloudflare's side.
//
// Best-effort: if warp-cli isn't present or the daemon already exited,
// we log and move on. The operator can clean up dangling devices in
// the Cloudflare dashboard if needed.
func (a *Agent) deregisterWarp() {
	if _, err := exec.LookPath("warp-cli"); err != nil {
		return
	}
	if out, err := exec.Command("warp-cli", "--accept-tos", "disconnect").CombinedOutput(); err != nil {
		log.Printf("WARP deregister: disconnect: %v (%s)", err, out)
	}
	if out, err := exec.Command("warp-cli", "--accept-tos", "registration", "delete").CombinedOutput(); err != nil {
		log.Printf("WARP deregister: registration delete: %v (%s)", err, out)
		return
	}
	// Wipe the connector-ID stamp so the next start re-registers cleanly.
	_ = os.Remove(filepath.Join(a.dataDir, "warp", "registered-connector-id"))
	log.Printf("WARP deregistered with Cloudflare (device slot freed)")
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

	// Reconcile the existing registration (if any) against the connector
	// token we were handed. Three cases:
	//
	//  1. No existing registration: register fresh.
	//  2. Existing registration matches our token: connect, done. Same
	//     device, no Cloudflare-side churn on every restart.
	//  3. Existing registration is for a DIFFERENT connector (operator
	//     changed JETTY_WARP_CONNECTOR_TOKEN, or /data/warp was migrated
	//     from another deploy): tear it down and register fresh,
	//     otherwise the new token's connector shows zero attached
	//     devices in the Cloudflare dashboard while we're still
	//     attached to the old one.
	expectedConnectorID := parseConnectorTokenID(token) // best-effort; "" if we can't parse
	stampPath := filepath.Join(a.dataDir, "warp", "registered-connector-id")
	output, _ := exec.Command("warp-cli", "--accept-tos", "registration", "show").CombinedOutput()
	needsRegister := strings.Contains(string(output), "Missing registration")
	if !needsRegister && expectedConnectorID != "" {
		stamped, _ := os.ReadFile(stampPath)
		if string(stamped) != expectedConnectorID {
			log.Printf("WARP registration is bound to a different Connector (%q) than the env token (%q); re-registering",
				string(stamped), expectedConnectorID)
			if out, err := exec.Command("warp-cli", "--accept-tos", "registration", "delete").CombinedOutput(); err != nil {
				log.Printf("Warning: warp-cli registration delete: %v (%s)", err, out)
			}
			needsRegister = true
		} else {
			log.Printf("WARP already registered to expected Connector %s, connecting...", expectedConnectorID)
		}
	} else if !needsRegister {
		log.Printf("WARP already registered, connecting...")
	}

	if needsRegister {
		log.Printf("Registering WARP connector...")
		cmd := exec.Command("warp-cli", "--accept-tos", "connector", "new", token)
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("failed to register WARP connector: %s", output)
		}
		// Stamp the registration so the next start can detect a token
		// change without having to call into Cloudflare.
		if expectedConnectorID != "" {
			_ = os.WriteFile(stampPath, []byte(expectedConnectorID), 0600)
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
