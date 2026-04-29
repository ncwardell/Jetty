package agent

import (
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strings"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
)

// =============================================================================
// HTTP Handlers - web terminal (workload exec + host shell)
// =============================================================================
//
// Two WebSocket endpoints, both PTY-backed:
//
//   WS /api/workloads/{name}/exec[?service=svc][&shell=/bin/bash]
//      Runs `docker exec -i -t <containerID> <shell>` on the owning node.
//      If ?service= is omitted, picks the workload's first container.
//      The workload must be local; remote workloads return 502 with a
//      hint to open the dashboard on the owning node. (WS proxying through
//      a Go HTTP handler isn't trivial; for v1 we keep it simple.)
//
//   WS /api/host/shell[?shell=/bin/bash]
//      Runs a shell directly on the host. Gated by JETTY_HOST_SHELL=true
//      because anyone with JETTY_SECRET can otherwise get root on every
//      node in the cluster. Off by default.
//
// Frame protocol (binary in both directions):
//
//   [0x00, payload...]            - terminal data (stdin from client, stdout/stderr to client)
//   [0x01, cols(u16BE), rows(u16BE)]  - resize event, client -> server
//
// Auth: WS endpoints can't carry custom headers from a browser, so the
// dashboard passes JETTY_SECRET via ?api_key=. The handler does its own
// constant-time check before upgrading - we never want an unauthenticated
// caller to even establish the WS.

const (
	termMsgData   byte = 0x00
	termMsgResize byte = 0x01
)

// termWSUpgrader uses the same permissive-origin policy as the existing
// tunnel WS endpoint. The x-origin protection here is the API key, not
// the Origin header.
var termWSUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// authorizeTerminalRequest performs the constant-time API-key check and
// writes a 401 if the request is not authenticated. Returns true on
// success.
//
// Terminal endpoints are admin-only: a peer's API key (used for
// peer-to-peer sync, tunnel, etc.) must NOT be enough to open a shell
// on another node. The dashboard authenticates with state.AdminKey, so
// an operator clicking "open terminal" still works.
//
// We DELIBERATELY require a configured AdminKey here, even though the
// rest of the API can run without one. Terminal endpoints are too
// dangerous to expose unauthenticated under any circumstances.
func (a *Agent) authorizeTerminalRequest(w http.ResponseWriter, r *http.Request) bool {
	a.stateMu.RLock()
	admin := a.state.AdminKey
	a.stateMu.RUnlock()
	if admin == "" {
		http.Error(w, "terminal disabled: admin key not configured", http.StatusUnauthorized)
		return false
	}
	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		apiKey = r.URL.Query().Get("api_key")
	}
	if subtle.ConstantTimeCompare([]byte(apiKey), []byte(admin)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

// pickShell returns the shell to run for the request. Defaults to /bin/sh
// (always present), upgrades to /bin/bash if the user asked. We don't try
// to be clever about detecting what's available - if /bin/bash isn't in
// the container, exec will fail and the user will see the error in the
// terminal.
func pickShell(r *http.Request, fallback string) string {
	shell := r.URL.Query().Get("shell")
	if shell == "" {
		return fallback
	}
	// Refuse anything that doesn't look like a path so query injection
	// can't add flags. Whitelisted shells: /bin/sh, /bin/bash, /bin/ash,
	// /bin/zsh, /usr/bin/fish.
	switch shell {
	case "/bin/sh", "/bin/bash", "/bin/ash", "/bin/zsh", "/usr/bin/fish", "/usr/local/bin/fish":
		return shell
	}
	return fallback
}

// attachPTYToWS hooks a PTY-backed cmd up to a WebSocket connection.
// Reads from the PTY and writes to the WS as type-0x00 frames; reads
// from the WS and either writes data (0x00) to the PTY or resizes (0x01).
//
// Blocks until the WS or the child process exits, whichever happens first.
// Cleans up: kills the child if still running, closes the PTY master.
func (a *Agent) attachPTYToWS(conn *websocket.Conn, cmd *exec.Cmd) {
	master, err := startPTY(cmd)
	if err != nil {
		// Send the error inline so the user sees something other than a
		// silent disconnect. Typed as terminal data so xterm.js renders it.
		errMsg := []byte("\r\n[jetty] failed to start terminal: " + err.Error() + "\r\n")
		conn.WriteMessage(websocket.BinaryMessage, append([]byte{termMsgData}, errMsg...))
		return
	}
	defer func() {
		master.Close()
		if cmd.Process != nil {
			// Best-effort kill in case the user closed the WS while the
			// child was still running.
			cmd.Process.Kill()
		}
		cmd.Wait() // reap zombie
	}()

	// pty -> ws (one goroutine, runs until master EOF)
	wsClosed := make(chan struct{})
	go func() {
		defer close(wsClosed)
		buf := make([]byte, 4096)
		for {
			n, err := master.Read(buf)
			if n > 0 {
				msg := make([]byte, n+1)
				msg[0] = termMsgData
				copy(msg[1:], buf[:n])
				if err := conn.WriteMessage(websocket.BinaryMessage, msg); err != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	// ws -> pty (this goroutine, runs until WS read fails)
	for {
		select {
		case <-wsClosed:
			return
		default:
		}
		msgType, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if msgType != websocket.BinaryMessage || len(msg) < 1 {
			continue
		}
		switch msg[0] {
		case termMsgData:
			if _, err := master.Write(msg[1:]); err != nil {
				return
			}
		case termMsgResize:
			if len(msg) < 5 {
				continue
			}
			cols := binary.BigEndian.Uint16(msg[1:3])
			rows := binary.BigEndian.Uint16(msg[3:5])
			if err := resizePTY(master, cols, rows); err != nil {
				log.Printf("Terminal resize failed: %v", err)
			}
		}
	}
}

// containerForExec returns the docker container ID to exec into for the
// given workload. With ?service=X it filters by compose service; without,
// it picks the first running container in the workload's compose project.
func (a *Agent) containerForExec(workloadName, service string) (string, error) {
	args := []string{"ps", "-q",
		"-f", "label=com.docker.compose.project=jetty_" + workloadName,
		"-f", "status=running",
	}
	if service != "" {
		args = append(args, "-f", "label=com.docker.compose.service="+service)
	}
	out, err := exec.Command("docker", args...).Output()
	if err != nil {
		return "", fmt.Errorf("docker ps: %w", err)
	}
	line := strings.TrimSpace(string(out))
	if line == "" {
		if service != "" {
			return "", fmt.Errorf("no running container for service %q in workload %q", service, workloadName)
		}
		return "", fmt.Errorf("no running container for workload %q", workloadName)
	}
	return strings.SplitN(line, "\n", 2)[0], nil
}

// apiWorkloadExec opens a docker-exec PTY into a workload's container.
//
// @Summary Open an interactive shell in a workload container (WebSocket)
// @Description Upgrades to a WebSocket and runs docker exec -i -t against the workload's first container (or ?service=X for a specific service). Use ?shell=/bin/bash to override the default /bin/sh. Auth: ?api_key=<secret> or X-API-Key header.
// @Tags terminal
// @Param name path string true "Workload name"
// @Param service query string false "Compose service name (default: first container)"
// @Param shell query string false "Shell to exec (default /bin/sh)"
// @Router /workloads/{name}/exec [get]
func (a *Agent) apiWorkloadExec(w http.ResponseWriter, r *http.Request) {
	if !a.authorizeTerminalRequest(w, r) {
		return
	}

	name := mux.Vars(r)["name"]

	// Find the workload to make sure (a) it exists, (b) we own it.
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
		http.Error(w, "workload not found", http.StatusNotFound)
		return
	}

	// v1 limitation: WS proxying through a Go HTTP handler is tricky, so
	// we don't proxy exec to remote owners. Tell the client to retry
	// against the owning node directly.
	if found.Owner != a.hwid {
		ownerName := "remote"
		ownerIP := ""
		if ownerPeer != nil {
			ownerName = ownerPeer.Name
			ownerIP = ownerPeer.IP
		}
		http.Error(w,
			fmt.Sprintf("workload runs on node %q (%s) - open the dashboard on that node to exec", ownerName, ownerIP),
			http.StatusBadGateway)
		return
	}

	containerID, err := a.containerForExec(name, r.URL.Query().Get("service"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	conn, err := termWSUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Terminal exec: WS upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	shell := pickShell(r, "/bin/sh")
	cmd := exec.Command("docker", "exec", "-i", "-t", containerID, shell)
	log.Printf("Terminal: exec into workload %s container %s shell=%s from %s",
		name, shortID(containerID, 12), shell, r.RemoteAddr)
	a.attachPTYToWS(conn, cmd)
}

// apiHostShell opens a shell on the host. Gated by JETTY_HOST_SHELL=true.
//
// @Summary Open a shell on the host (WebSocket)
// @Description Upgrades to a WebSocket and runs an interactive shell directly on the host. DISABLED by default. Set JETTY_HOST_SHELL=true to enable. Anyone with JETTY_SECRET can use this to get root on every cluster member, so enable only when you actually need it. Auth: ?api_key=<secret> or X-API-Key header.
// @Tags terminal
// @Param shell query string false "Shell to run (default /bin/bash, falls back to /bin/sh)"
// @Router /host/shell [get]
func (a *Agent) apiHostShell(w http.ResponseWriter, r *http.Request) {
	if !a.authorizeTerminalRequest(w, r) {
		return
	}
	if !a.hostShellEnabled {
		http.Error(w, "host shell disabled - set JETTY_HOST_SHELL=true on the agent to enable", http.StatusForbidden)
		return
	}

	conn, err := termWSUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Host shell: WS upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	// Default /bin/bash for the host (we know it's there - the agent's
	// own image installs bash). The shell allowlist accepts /bin/sh as
	// the safe fallback if bash somehow isn't present.
	shell := pickShell(r, "/bin/bash")
	cmd := exec.Command(shell)
	// Reasonable login-shell environment so PATH and prompt are set up.
	cmd.Env = append(cmd.Env,
		"TERM=xterm-256color",
		"HOME=/root",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	)
	log.Printf("Host shell: opened by %s", r.RemoteAddr)
	a.attachPTYToWS(conn, cmd)
}
