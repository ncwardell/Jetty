package agent

import (
	"runtime/debug"
	"time"
)

// =============================================================================
// Panic barriers
// =============================================================================
//
// A Go panic unwinds only its own goroutine, but an unrecovered one takes the
// whole process with it. net/http recovers per-request, so an HTTP handler
// panic costs one request - but nothing else here had a barrier. A panic in
// the tunnel's packet parsing, a memberlist delegate callback, or any of the
// dozen background tickers killed the agent, and with it every workload's
// supervisor on that node.
//
// Two shapes, because the right response differs:
//
//   goSafe        one-shot work (a connection, a packet, a proxied flow).
//                 Recover, log, drop that unit of work. This is where
//                 attacker-influenced input is parsed, so it is the barrier
//                 that matters most.
//
//   goSupervised  long-lived loops that must not simply stop. Recover, log,
//                 and restart after a delay. Backs off and eventually gives
//                 up so a deterministic panic cannot spin the CPU forever.
//
// Panics are not silently absorbed: every recovery logs the value and the
// stack. A recovered panic is still a bug to fix, just not an outage.

const (
	// superviseRestartDelay is the pause before restarting a panicking loop.
	// Long enough that a tight panic loop cannot saturate a core, short
	// enough that a transient fault self-heals quickly.
	superviseRestartDelay = 5 * time.Second

	// superviseMaxRestarts bounds restarts of a single loop. A loop that
	// panics this many times is broken in a way restarting will not fix;
	// giving up leaves one loud log line rather than an endless stream.
	superviseMaxRestarts = 10
)

// recoverPanic is the barrier itself, used as `defer recoverPanic(name)`.
//
// CAREFUL: recover() only takes effect when called *directly* by a deferred
// function. `defer recoverPanic(name)` works because recoverPanic is itself
// the deferred call. Wrapping it - `defer func() { recoverPanic(name) }()` -
// puts recover() one frame too deep, it returns nil, and the panic keeps
// unwinding. Deferred closures must call recover() inline; see runSupervised.
func recoverPanic(name string) interface{} {
	r := recover()
	if r != nil {
		logPanic(name, r)
	}
	return r
}

func logPanic(name string, r interface{}) {
	logErrorf("PANIC recovered in %s: %v\n%s", name, r, debug.Stack())
}

// goSafe runs fn in a new goroutine behind a panic barrier. Use for one-shot
// work where dropping the unit of work is the correct response.
func goSafe(name string, fn func()) {
	go func() {
		defer recoverPanic(name)
		fn()
	}()
}

// goSupervised runs fn in a new goroutine, restarting it if it panics. Use for
// loops that are expected to run for the life of the agent.
func goSupervised(name string, fn func()) {
	go func() {
		for attempt := 0; ; attempt++ {
			if panicked := runSupervised(name, fn); !panicked {
				return // clean return: the loop is done, leave it alone
			}
			if attempt >= superviseMaxRestarts {
				logErrorf("PANIC in %s: giving up after %d restarts", name, superviseMaxRestarts)
				return
			}
			logInfof("Restarting %s in %s (attempt %d/%d)",
				name, superviseRestartDelay, attempt+1, superviseMaxRestarts)
			time.Sleep(superviseRestartDelay)
		}
	}()
}

// runSupervised invokes fn once, reporting whether it panicked. Split out so
// the deferred recover scopes to a single invocation rather than the whole
// restart loop.
func runSupervised(name string, fn func()) (panicked bool) {
	defer func() {
		// recover() inline, not via recoverPanic: it is only effective when
		// the deferred function calls it directly.
		if r := recover(); r != nil {
			logPanic(name, r)
			panicked = true
		}
	}()
	fn()
	return false
}
