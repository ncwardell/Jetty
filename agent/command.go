package agent

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

// =============================================================================
// Bounded external commands
// =============================================================================
//
// Every one-shot external command the agent runs goes through runCmd /
// runCmdOutput / runCmdCombined (or boundedCommand when the caller needs to
// set Dir/Env). Plain exec.Command(...).Run() has no deadline, and an `ip`,
// `iptables` or `docker` child that never exits - netlink contention, a fork
// under memory pressure, a docker daemon wedged on a dead overlay mount, a
// D-state child on flaky networking - blocks its caller forever. When that
// caller held stateMu (as the route path did until 2026-11, and as
// NotifyUpdate still did) the whole control plane froze: Go's RWMutex is
// writer-preferring, so one stuck writer excludes every reader, including
// apiKeyMiddleware on every API request. /api/livez kept answering because it
// only TryRLocks, so the node looked alive while serving nothing; on
// 2026-11-18 that left a node with no mesh routes to its peer's workloads.
//
// Two rules follow, and both are needed:
//
//  1. Never hold stateMu (or any lock other code paths need promptly) across
//     an exec. Snapshot under the lock, release, then exec.
//  2. Bound every exec anyway. Off the lock a hung child no longer wedges the
//     API, but it still wedges whatever loop ran it (route reconcile, the
//     iptables maintenance loop, a deploy) - forever, silently.
//
// Long-running supervised processes (cloudflared, warp-svc, interactive
// terminal shells) are deliberately NOT run through here: they are Start()ed
// and their lifetime is managed by their supervisor, so a deadline would just
// kill a healthy daemon.

const (
	// hostCommandTimeout bounds local kernel-configuration commands: ip,
	// iptables, nft, sysctl, modprobe (and df). These are netlink / xtables ops
	// that complete in milliseconds; if one has not finished in 10s it is
	// stuck, and waiting longer only delays everything queued behind it.
	hostCommandTimeout = 10 * time.Second

	// warpCLITimeout bounds warp-cli calls that only talk to the local
	// warp-svc over its IPC socket (status, connect, disconnect, show).
	warpCLITimeout = 30 * time.Second

	// warpRegisterTimeout bounds warp-cli calls that go out to Cloudflare's
	// API (connector new, registration delete). Network round-trips over a
	// possibly flaky uplink, so more headroom than local IPC.
	warpRegisterTimeout = 90 * time.Second

	// dockerQueryTimeout bounds short docker CLI calls against the local
	// daemon: ps, inspect, stats --no-stream, rename, rm -f. A healthy daemon
	// answers these in well under a second; a minute is generous enough to
	// ride out a daemon that is busy (e.g. mid image extraction) without
	// letting a wedged daemon stall the caller indefinitely.
	dockerQueryTimeout = 60 * time.Second

	// dockerLifecycleTimeout bounds container lifecycle calls (restart,
	// create, run -d) that may have to wait out a container's stop grace
	// period, which compose files can set well above Docker's 10s default.
	dockerLifecycleTimeout = 5 * time.Minute

	// dockerLongTimeout bounds docker operations whose duration scales with
	// image size and link speed: docker pull, docker compose (up/pull/down
	// can all pull images), and prune of a large build cache. This is a
	// hang backstop, not a performance expectation - a multi-GB pull over a
	// slow uplink must still succeed, but a pull that has made no progress
	// for an hour is not going to.
	dockerLongTimeout = 60 * time.Minute

	// cmdWaitDelay bounds how long Wait keeps waiting for the child's stdio
	// pipes to close after the process is killed (or exits). Without it a
	// grandchild that inherited stdout/stderr - `docker` forking a plugin,
	// `sh -c` backgrounding something - keeps the pipe open and
	// CombinedOutput blocks forever even though the context fired, which
	// would quietly defeat the whole timeout.
	cmdWaitDelay = 2 * time.Second
)

// boundedCommand builds an exec.Cmd that is killed when timeout elapses and
// whose Wait cannot be held open by inherited pipes. For callers that need to
// set Dir/Env before running; they must call the returned cancel func once
// the command has finished, and should pass the run error through
// boundedCmdErr so a timeout is reported as one.
func boundedCommand(timeout time.Duration, name string, args ...string) (*exec.Cmd, context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = cmdWaitDelay
	return cmd, ctx, cancel
}

// boundedCmdErr turns the error from a bounded command into one that says
// plainly that it timed out, wrapping the original so callers can still
// errors.As it to *exec.ExitError.
//
// Only the program name goes in the message, never argv: callers pass
// secrets as arguments (`warp-cli connector new <token>`, `docker create -e
// JETTY_SECRET=...`) and these errors are logged verbatim.
func boundedCmdErr(ctx context.Context, err error, timeout time.Duration, name string) error {
	if err == nil {
		return nil
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%s: timed out after %s: %w", name, timeout, err)
	}
	if errors.Is(err, exec.ErrWaitDelay) {
		// The process exited but something it spawned held our pipes past
		// cmdWaitDelay. Surface it - the output may be truncated.
		return fmt.Errorf("%s: output pipes held open after exit: %w", name, err)
	}
	return err
}

// runCmd runs a command to completion (exec.Cmd.Run semantics) within
// timeout.
func runCmd(timeout time.Duration, name string, args ...string) error {
	cmd, ctx, cancel := boundedCommand(timeout, name, args...)
	defer cancel()
	return boundedCmdErr(ctx, cmd.Run(), timeout, name)
}

// runCmdOutput returns the command's stdout (exec.Cmd.Output semantics:
// stderr is captured into the *exec.ExitError, not mixed into the output) -
// use it wherever the output is parsed.
func runCmdOutput(timeout time.Duration, name string, args ...string) ([]byte, error) {
	cmd, ctx, cancel := boundedCommand(timeout, name, args...)
	defer cancel()
	out, err := cmd.Output()
	return out, boundedCmdErr(ctx, err, timeout, name)
}

// runCmdCombined returns stdout and stderr interleaved (exec.Cmd.
// CombinedOutput semantics) - use it where the output is logged or shown to
// an operator.
func runCmdCombined(timeout time.Duration, name string, args ...string) ([]byte, error) {
	cmd, ctx, cancel := boundedCommand(timeout, name, args...)
	defer cancel()
	out, err := cmd.CombinedOutput()
	return out, boundedCmdErr(ctx, err, timeout, name)
}
