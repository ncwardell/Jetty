package agent

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

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
		writeError(w, http.StatusUnauthorized, "terminal disabled: admin key not configured")
		return false
	}
	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		apiKey = r.URL.Query().Get("api_key")
	}
	if subtle.ConstantTimeCompare([]byte(apiKey), []byte(admin)) != 1 {
		writeError(w, http.StatusUnauthorized, "unauthorized")
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
				logErrorf("Terminal resize failed: %v", err)
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
		writeError(w, http.StatusNotFound, "workload not found")
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
		writeError(w, http.StatusBadGateway,
			fmt.Sprintf("workload runs on node %q (%s) - open the dashboard on that node to exec", ownerName, ownerIP))
		return
	}

	containerID, err := a.containerForExec(name, r.URL.Query().Get("service"))
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	conn, err := termWSUpgrader.Upgrade(w, r, nil)
	if err != nil {
		logErrorf("Terminal exec: WS upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	shell := pickShell(r, "/bin/sh")
	cmd := exec.Command("docker", "exec", "-i", "-t", containerID, shell)
	logInfof("Terminal: exec into workload %s container %s shell=%s from %s",
		name, shortID(containerID, 12), shell, r.RemoteAddr)
	a.attachPTYToWS(conn, cmd)
}

// apiHostShell opens a shell on the host. Gated by JETTY_HOST_SHELL=true.
//
// "Host" here is literal when the container was launched with
// --pid=host: in that case PID 1 inside our /proc is the host's init,
// living in a different mount namespace than ours. We `nsenter` into
// it and the shell sees the host's filesystem, processes, and so on -
// the same picture you'd get over SSH.
//
// Without --pid=host, PID 1 is the container's own entrypoint and
// nsenter would be a no-op. We fall back to a shell in the container's
// namespace and emit a banner explaining what the user is actually
// looking at, so files in /home/<user>/ on the real host (which the
// container can't see) don't appear missing for mysterious reasons.
//
// @Summary Open a shell on the host (WebSocket)
// @Description Upgrades to a WebSocket and runs an interactive shell. With --pid=host on the docker run, this is a true host shell via nsenter. Without it, the shell runs inside the agent container and a banner explains the limitation. DISABLED by default. Set JETTY_HOST_SHELL=true to enable. Auth: ?api_key=<secret> or X-API-Key header.
// @Tags terminal
// @Param shell query string false "Shell to run (default /bin/bash, falls back to /bin/sh)"
// @Router /host/shell [get]
func (a *Agent) apiHostShell(w http.ResponseWriter, r *http.Request) {
	if !a.authorizeTerminalRequest(w, r) {
		return
	}

	// Cross-node proxy: ?node=<id> targets that peer's host shell. The
	// dashboard always serves WS from whichever agent is hosting it, so
	// without this proxy the "Host Shell" button on a remote node has
	// nowhere to connect. AdminKey is gossiped cluster-wide so we
	// authenticate to the peer with our own AdminKey.
	if nodeID := r.URL.Query().Get("node"); nodeID != "" && nodeID != a.hwid {
		a.proxyHostShell(w, r, nodeID)
		return
	}

	if !a.hostShellEnabled {
		writeError(w, http.StatusForbidden, "host shell disabled - set JETTY_HOST_SHELL=true on the agent to enable")
		return
	}

	conn, err := termWSUpgrader.Upgrade(w, r, nil)
	if err != nil {
		logErrorf("Host shell: WS upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	// Default /bin/bash for the host (we know it's there - the agent's
	// own image installs bash). The shell allowlist accepts /bin/sh as
	// the safe fallback if bash somehow isn't present.
	shell := pickShell(r, "/bin/bash")

	var cmd *exec.Cmd
	var banner string
	if hostNamespacesAvailable() {
		// nsenter into PID 1's mount, UTS, IPC, network, and pid
		// namespaces - the host's. Network is already shared via
		// --net host, but including -n keeps the namespace set
		// internally consistent.
		cmd = exec.Command("/usr/bin/nsenter",
			"-t", "1",
			"-m", "-u", "-i", "-n", "-p",
			"--", shell)
	} else {
		cmd = exec.Command(shell)
		banner = "\r\n" +
			"\x1b[33m" + // yellow
			"NOTE: this shell is running INSIDE the jetty container, not on the host.\r\n" +
			"      Files under /home/, /root/ on the host are not visible here.\r\n" +
			"      To get a real host shell, restart the agent with --pid=host on\r\n" +
			"      the docker run. The container will then nsenter into PID 1's\r\n" +
			"      namespaces automatically.\r\n" +
			"\x1b[0m\r\n"
	}
	// Reasonable login-shell environment so PATH and prompt are set up.
	cmd.Env = append(cmd.Env,
		"TERM=xterm-256color",
		"HOME=/root",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	)
	if banner != "" {
		// xterm.js will render this before the shell's first prompt.
		_ = conn.WriteMessage(websocket.BinaryMessage, append([]byte{termMsgData}, []byte(banner)...))
	}
	logInfof("Host shell: opened by %s (host_ns=%v)", r.RemoteAddr, hostNamespacesAvailable())
	a.attachPTYToWS(conn, cmd)
}

// hostNamespacesAvailable returns true when the agent's container was
// launched with --pid=host: PID 1 inside /proc is the host's init,
// living in a different mount namespace than ours. We check by
// comparing the namespace inode links - they're guaranteed-different
// if and only if we're in different namespaces.
//
// The check is read-only and runs at first request. We don't memoize
// because the result can't change at runtime (namespaces are
// process-lifecycle-stable) and the cost is two readlinks.
func hostNamespacesAvailable() bool {
	ourMnt, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		return false
	}
	pid1Mnt, err := os.Readlink("/proc/1/ns/mnt")
	if err != nil {
		return false
	}
	return ourMnt != pid1Mnt
}

// HostExecRequest is the body shape for POST /api/host/exec.
type HostExecRequest struct {
	Command string `json:"command"`           // shell -c argument; the whole string is one command line
	Timeout int    `json:"timeout,omitempty"` // seconds; default 30, max 300
}

// HostExecResponse is what the endpoint returns.
type HostExecResponse struct {
	ExitCode   int    `json:"exit_code"`
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	DurationMs int64  `json:"duration_ms"`
	Node       string `json:"node"`
	TimedOut   bool   `json:"timed_out,omitempty"`
}

// apiHostExec runs a one-shot command on the host and returns its output.
// This is the synchronous, tool-friendly counterpart to /api/host/shell:
// no WebSocket, no PTY, no interactive prompts - perfect for an LLM agent
// or a script that just wants `ls /` to come back as a string.
//
// Gating mirrors /api/host/shell - JETTY_HOST_SHELL=true must be set on
// the agent. Without --pid=host on the docker run, the command executes
// inside the agent container rather than on the host's filesystem; the
// response includes "container_namespace": true so callers can warn.
//
// @Summary Run one command on the host and return its output
// @Description One-shot exec. Requires JETTY_HOST_SHELL=true on the target agent and AdminKey auth. ?node=<id|name> proxies to a peer.
// @Tags terminal
// @Accept json
// @Produce json
// @Param node query string false "Peer ID or name; defaults to this node"
// @Param request body HostExecRequest true "Command to run"
// @Success 200 {object} HostExecResponse
// @Router /host/exec [post]
func (a *Agent) apiHostExec(w http.ResponseWriter, r *http.Request) {
	// Same auth shape as the WS host shell: AdminKey only (not peer keys).
	if !a.adminAuthorize(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized: admin key required for host exec")
		return
	}

	// Cross-node proxy: ?node=<id|name> forwards to a peer. Same pattern
	// as proxyHostRequestIfRemote in handlers_host.go.
	if nodeID := r.URL.Query().Get("node"); nodeID != "" && nodeID != "self" && nodeID != a.hwid && nodeID != a.hostname {
		a.proxyHostExec(w, r, nodeID)
		return
	}

	if !a.hostShellEnabled {
		writeError(w, http.StatusForbidden, "host exec disabled - set JETTY_HOST_SHELL=true on the agent to enable")
		return
	}

	var req HostExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if strings.TrimSpace(req.Command) == "" {
		writeError(w, http.StatusBadRequest, "command is required")
		return
	}
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 30
	}
	if timeout > 300 {
		timeout = 300
	}

	// Build the actual exec. With --pid=host we nsenter into PID 1's
	// namespaces; otherwise we run the command directly inside the agent
	// container. Same logic as apiHostShell - just synchronous.
	var argv []string
	if hostNamespacesAvailable() {
		argv = []string{"/usr/bin/nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--", "/bin/sh", "-c", req.Command}
	} else {
		argv = []string{"/bin/sh", "-c", req.Command}
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeout)*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Env = append(cmd.Env,
		"TERM=dumb",
		"HOME=/root",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	)
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf
	start := time.Now()
	err := cmd.Run()
	dur := time.Since(start)

	resp := HostExecResponse{
		Stdout:     stdoutBuf.String(),
		Stderr:     stderrBuf.String(),
		DurationMs: dur.Milliseconds(),
		Node:       a.hostname,
	}
	if ctx.Err() == context.DeadlineExceeded {
		resp.TimedOut = true
		resp.ExitCode = 124 // GNU coreutils convention
	} else if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			resp.ExitCode = exitErr.ExitCode()
		} else {
			// Process didn't start at all (e.g. nsenter missing). Surface
			// as a 500 rather than a "completed with rc=1" lie.
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("exec failed to start: %v", err))
			return
		}
	}

	logInfof("Host exec: by %s (%dms exit=%d, %d stdout / %d stderr bytes)",
		r.RemoteAddr, resp.DurationMs, resp.ExitCode, len(resp.Stdout), len(resp.Stderr))
	writeJSON(w, resp)
}

// proxyHostExec forwards a POST /api/host/exec to a peer when ?node=<id> targets one.
func (a *Agent) proxyHostExec(w http.ResponseWriter, r *http.Request, nodeID string) {
	a.stateMu.RLock()
	var target *Peer
	for _, p := range a.state.Peers {
		if p.ID == nodeID || p.Name == nodeID {
			target = p
			break
		}
	}
	admin := a.state.AdminKey
	a.stateMu.RUnlock()

	if target == nil {
		writeError(w, http.StatusNotFound, "node not found: "+nodeID)
		return
	}
	if admin == "" {
		writeError(w, http.StatusUnauthorized, "admin key not configured")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	url := a.getPeerAPIURL(target, "/api/host/exec")
	proxyReq, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "build proxy request: "+err.Error())
		return
	}
	proxyReq.Header.Set("X-API-Key", admin)
	proxyReq.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(proxyReq)
	if err != nil {
		writeError(w, http.StatusBadGateway, "reach node: "+err.Error())
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// proxyHostShell bridges a browser's WebSocket to a peer's
// /api/host/shell endpoint. Used when the dashboard is loaded on node X
// but the operator wants a shell on node Y. We dial Y first - if Y
// rejects (e.g. JETTY_HOST_SHELL=false on Y), we forward the HTTP
// status to the browser BEFORE upgrading, so the user gets a clean
// error instead of a silent disconnect.
func (a *Agent) proxyHostShell(w http.ResponseWriter, r *http.Request, nodeID string) {
	a.stateMu.RLock()
	peer := a.state.Peers[nodeID]
	admin := a.state.AdminKey
	a.stateMu.RUnlock()

	if peer == nil || peer.IP == "" {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("peer %s not found or has no WARP IP", nodeID))
		return
	}
	if admin == "" {
		// authorizeTerminalRequest already enforces this for the local
		// path, but we re-check here because the proxy uses AdminKey to
		// authenticate to the peer.
		writeError(w, http.StatusUnauthorized, "admin key not configured")
		return
	}

	// Build the peer URL. Drop ?node= so the peer doesn't try to
	// proxy onward; preserve ?shell= so the user's choice carries.
	target := url.URL{
		Scheme: "ws",
		Host:   fmt.Sprintf("%s:%d", peer.IP, a.apiPort),
		Path:   "/api/host/shell",
	}
	q := target.Query()
	if shell := r.URL.Query().Get("shell"); shell != "" {
		q.Set("shell", shell)
	}
	q.Set("api_key", admin)
	target.RawQuery = q.Encode()

	peerConn, resp, err := websocket.DefaultDialer.Dial(target.String(), nil)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			writeError(w, http.StatusBadGateway,
				fmt.Sprintf("peer %s rejected shell (HTTP %d): %s",
					peer.Name, resp.StatusCode, strings.TrimSpace(string(body))))
		} else {
			writeError(w, http.StatusBadGateway, fmt.Sprintf("dial peer host shell: %v", err))
		}
		return
	}
	defer peerConn.Close()

	clientConn, err := termWSUpgrader.Upgrade(w, r, nil)
	if err != nil {
		logErrorf("Host shell proxy: WS upgrade failed: %v", err)
		return
	}
	defer clientConn.Close()

	logInfof("Host shell proxy: %s -> peer %s (%s)", r.RemoteAddr, peer.Name, shortID(nodeID, 12))
	bridgeWebSockets(clientConn, peerConn)
}

// bridgeWebSockets shuffles messages between two WebSockets until either
// side closes. Forwards message types (binary/text/control) verbatim so
// the terminal frame protocol passes through unchanged.
//
// The two ReadMessage loops can race against each other on the writes;
// gorilla/websocket's NextWriter is not safe for concurrent use, but
// each goroutine writes to a different connection so this is fine.
func bridgeWebSockets(a, b *websocket.Conn) {
	done := make(chan struct{}, 2)

	pump := func(src, dst *websocket.Conn) {
		defer func() { done <- struct{}{} }()
		for {
			msgType, data, err := src.ReadMessage()
			if err != nil {
				return
			}
			if err := dst.WriteMessage(msgType, data); err != nil {
				return
			}
		}
	}

	go pump(a, b)
	go pump(b, a)

	<-done
	// Closing both connections wakes the still-blocked pump's
	// ReadMessage so the second goroutine exits and the deferred
	// closes (clientConn/peerConn) run cleanly.
	a.Close()
	b.Close()
	<-done
}
