package agent

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// Both of these fired during a single real vaultwarden move on 2026-08-25.

// TestMoveUsesATimeoutSizedForDataNotForAnAPICall covers the first: the deploy
// call used httpClient's 30s timeout, but a move relocates a workload and a
// stateful one restores on arrival. That move spent 17 minutes pulling ~5.5MB
// over CIFS across a 120ms link, so it was guaranteed to report
// "context deadline exceeded" while the deploy carried on and succeeded.
func TestMoveUsesATimeoutSizedForDataNotForAnAPICall(t *testing.T) {
	if MoveDeployTimeout <= DefaultHTTPTimeout {
		t.Errorf("MoveDeployTimeout (%s) is not longer than the general client (%s); "+
			"a move that relocates real data will report failure while succeeding",
			MoveDeployTimeout, DefaultHTTPTimeout)
	}
	if moveClient.Timeout != MoveDeployTimeout {
		t.Errorf("moveClient timeout = %s, want %s", moveClient.Timeout, MoveDeployTimeout)
	}
	// Still bounded - a wedged target must not hang the caller forever.
	if MoveDeployTimeout == 0 {
		t.Error("MoveDeployTimeout is unbounded; a wedged target would hang the move indefinitely")
	}
}

// TestMoveVerifiesTheTargetBeforeReportingFailure covers the reasoning that
// makes the longer timeout sufficient rather than merely larger. A timeout
// means the request did not come back - not that the deploy failed. Reporting
// failure while the target is serving is how an operator ends up believing two
// nodes own the same workload.
func TestMoveVerifiesTheTargetBeforeReportingFailure(t *testing.T) {
	src := readAgentSource(t, "handlers_workloads.go")

	idx := strings.Index(src, "failed to deploy on target")
	if idx == -1 {
		t.Fatal("the deploy-failure path was not found")
	}
	// Look at the surrounding block, not the whole file.
	start := idx - 1200
	if start < 0 {
		start = 0
	}
	block := src[start : idx+400]

	if !strings.Contains(block, "workloadRunningOn") {
		t.Error("the move reports a deploy failure without asking the target whether " +
			"the workload is actually running there; a timeout is not evidence of failure")
	}
	// The actual call, not the word in the comment above it - matching a
	// comment is how the first version of this test passed against the bug.
	if !strings.Contains(block, "moveClient.Do(deployReq)") {
		t.Error("the move still uses the general HTTP client for the deploy call")
	}
}

// TestConcurrentDeploysOfTheSameWorkloadAreRejected covers the second bug. The
// reconcile loop ran while the move's deploy was still creating containers,
// saw `docker ps -q` return nothing (created, not yet running), decided the
// workload was down, and started a second `compose up`. Docker failed both
// with `Container "jetty_vaultwarden-init-1" is already in use`.
func TestConcurrentDeploysOfTheSameWorkloadAreRejected(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.composeDir = t.TempDir()

	// Hold the slot as an in-flight deploy would.
	a.deployInProgress.Store("app", time.Now())
	defer a.deployInProgress.Delete("app")

	// deployWorkload must refuse rather than run a second compose up. It is
	// checked before any docker work, so this never shells out.
	err := a.deployWorkload(&Workload{Name: "app", IP: "10.100.0.1", Compose: "x"})
	if !errors.Is(err, errDeployInProgress) {
		t.Fatalf("second concurrent deploy returned %v, want errDeployInProgress", err)
	}
}

func TestDeploySlotIsReleasedAfterwards(t *testing.T) {
	// A guard that never releases would wedge a workload permanently - worse
	// than the collision it prevents.
	a := newTestAgentWithDir(t)
	a.composeDir = t.TempDir()

	// A deploy that fails early (no compose for this arch) must still release.
	a.deployWorkload(&Workload{Name: "app", IP: "10.100.0.1"})

	if _, stillHeld := a.deployInProgress.Load("app"); stillHeld {
		t.Error("the in-flight deploy slot was not released after the deploy returned; " +
			"the workload would never be deployable again")
	}
}

func TestConcurrentDeploysDoNotRace(t *testing.T) {
	// Run under -race: reconcile, failover and an operator can all call this.
	a := newTestAgentWithDir(t)
	a.composeDir = t.TempDir()

	var wg sync.WaitGroup
	rejected := make(chan struct{}, 32)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.deployWorkload(&Workload{Name: "same", IP: "10.100.0.2"}); errors.Is(err, errDeployInProgress) {
				rejected <- struct{}{}
			}
		}()
	}
	wg.Wait()
	close(rejected)

	// At least some must have been refused; the point is that they cannot all
	// proceed into docker at once.
	if len(rejected) == 0 {
		t.Error("16 concurrent deploys of the same workload were all admitted; " +
			"the in-flight guard is not doing anything")
	}
}

// TestReconcileTreatsAnInFlightDeployAsFine guards the log level. An in-flight
// deploy is the normal case during a move, not an error, and logging it as one
// on every reconcile tick would train an operator to ignore reconcile errors.
func TestReconcileTreatsAnInFlightDeployAsFine(t *testing.T) {
	src := readAgentSource(t, "deploy.go")

	// Find the branch that handles it, and check what it logs *before* falling
	// through to the generic error path.
	idx := strings.Index(src, "errors.Is(err, errDeployInProgress)")
	if idx == -1 {
		t.Fatal("reconcile does not special-case errDeployInProgress; an in-flight " +
			"deploy would be logged as a failure on every tick")
	}
	rest := src[idx:]
	nextError := strings.Index(rest, "logErrorf")
	nextDebug := strings.Index(rest, "logDebugf")

	if nextDebug == -1 || (nextError != -1 && nextError < nextDebug) {
		t.Error("the errDeployInProgress branch reaches logErrorf before logDebugf; " +
			"an in-flight deploy is the expected state during a move, not a failure")
	}
}
