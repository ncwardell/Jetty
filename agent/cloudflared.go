package agent

import (
	"os"
	"os/exec"
	"strings"
	"time"
)

// =============================================================================
// Cloudflare Tunnel Management
// =============================================================================

// cloudflaredLogFilter filters cloudflared output to only log important messages.
// This prevents verbose debug output from flooding the logs while still capturing
// errors, connection status, and other important information.
type cloudflaredLogFilter struct {
	prefix string
}

func (f *cloudflaredLogFilter) Write(p []byte) (n int, err error) {
	line := strings.TrimSpace(string(p))
	if line == "" {
		return len(p), nil
	}

	// Only log important messages: errors, warnings, connection status
	// Filter out verbose debug output (INFO level routine messages)
	if strings.Contains(line, "ERR") ||
		strings.Contains(line, "WRN") ||
		strings.Contains(line, "error") ||
		strings.Contains(line, "failed") ||
		strings.Contains(line, "Registered") ||
		strings.Contains(line, "Unregistered") ||
		strings.Contains(line, "connected") ||
		strings.Contains(line, "Starting tunnel") {
		logInfof("[%s] %s", f.prefix, line)
	}

	return len(p), nil
}

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
	disabled := a.state.CFTunnelDisabled
	a.stateMu.RUnlock()

	if token == "" {
		return nil // No token configured
	}
	if disabled {
		// Node-local opt-out. Checked here rather than at the call sites so
		// restarts, token syncs, and the monitor's restart loop all honour
		// it - otherwise the connector resurrects itself.
		logInfof("Cloudflare tunnel not started: disabled on this node")
		return nil
	}

	a.cfStopCh = make(chan struct{})

	// Start cloudflared tunnel with --no-autoupdate to prevent background updates
	// Note: --no-autoupdate is a global flag and must come before 'tunnel'
	// Pass token via --token flag (more reliable than TUNNEL_TOKEN env var)
	a.cfCmd = exec.Command("cloudflared", "--no-autoupdate", "tunnel", "run", "--token", token)

	// Use filtered log writer to capture important messages while suppressing verbose output
	logFilter := &cloudflaredLogFilter{prefix: "cloudflared"}
	a.cfCmd.Stdout = logFilter
	a.cfCmd.Stderr = logFilter

	if err := a.cfCmd.Start(); err != nil {
		return err
	}

	logInfof("Cloudflare tunnel started (pid: %d)", a.cfCmd.Process.Pid)

	// Monitor process and restart on failure
	goSafe("monitorCloudflared", a.monitorCloudflared)

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
		logInfof("Cloudflare tunnel stopped")
	}
	a.cfCmd = nil
}

// monitorCloudflared watches the cloudflared process and restarts it if it dies.
// Uses exponential backoff with a maximum of 10 consecutive failures before giving up.
func (a *Agent) monitorCloudflared() {
	backoff := CloudflaredInitialBackoff
	failures := 0

	for {
		a.cfMu.Lock()
		cmd := a.cfCmd
		stopCh := a.cfStopCh
		a.cfMu.Unlock()

		if cmd == nil {
			return
		}

		startTime := time.Now()

		// Wait for process to exit
		err := cmd.Wait()

		// Check if we were asked to stop
		select {
		case <-stopCh:
			return
		default:
		}

		// If process ran for a while, reset the failure counter and backoff
		if time.Since(startTime) >= CloudflaredSuccessReset {
			failures = 0
			backoff = CloudflaredInitialBackoff
		} else {
			failures++
		}

		// Check if we've exceeded max failures
		if failures >= CloudflaredMaxFailures {
			logInfof("Cloudflare tunnel failed %d times consecutively, giving up. Check your JETTY_CF_TOKEN.", failures)
			return
		}

		if err != nil {
			logInfof("Cloudflare tunnel exited: %v (attempt %d/%d), restarting in %v...", err, failures, CloudflaredMaxFailures, backoff)
		} else {
			logInfof("Cloudflare tunnel exited (attempt %d/%d), restarting in %v...", failures, CloudflaredMaxFailures, backoff)
		}

		time.Sleep(backoff)

		// Exponential backoff: double the wait time for next failure, up to max
		backoff = backoff * 2
		if backoff > CloudflaredMaxBackoff {
			backoff = CloudflaredMaxBackoff
		}

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
		disabled := a.state.CFTunnelDisabled
		a.stateMu.RUnlock()

		if token == "" || disabled {
			a.cfMu.Unlock()
			return
		}

		// Note: --no-autoupdate is a global flag and must come before 'tunnel'
		// Pass token via --token flag (more reliable than TUNNEL_TOKEN env var)
		a.cfCmd = exec.Command("cloudflared", "--no-autoupdate", "tunnel", "run", "--token", token)

		// Use filtered log writer to capture important messages while suppressing verbose output
		logFilter := &cloudflaredLogFilter{prefix: "cloudflared"}
		a.cfCmd.Stdout = logFilter
		a.cfCmd.Stderr = logFilter

		if err := a.cfCmd.Start(); err != nil {
			logErrorf("Cloudflare tunnel restart failed: %v", err)
			a.cfMu.Unlock()
			return
		}
		logInfof("Cloudflare tunnel restarted (pid: %d)", a.cfCmd.Process.Pid)
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
