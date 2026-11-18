package agent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// On 2026-11-18 Radxa's agent wedged with stateMu write-held, /api/livez
// answered 200 throughout, and nothing restarted it. These pin the watchdog
// that now does.

func fastWedgeProbe(t *testing.T) {
	t.Helper()
	old := wedgeProbeWaitFor
	wedgeProbeWaitFor = 20 * time.Millisecond
	t.Cleanup(func() { wedgeProbeWaitFor = old })
}

func TestWedgeCheckRestartsOnlyAfterContinuousThreshold(t *testing.T) {
	fastWedgeProbe(t)
	a := newTestAgentWithDir(t)
	threshold := 3 * time.Minute

	// Healthy lock: never restarts, clock stays clear.
	if a.wedgeCheck(time.Now(), threshold) {
		t.Fatal("restart requested on a healthy agent")
	}
	if a.lockUnacquirableSince.Load() != 0 {
		t.Fatal("wedge clock started on a healthy agent")
	}

	release := make(chan struct{})
	held := make(chan struct{})
	go func() {
		a.stateMu.Lock()
		close(held)
		<-release
		a.stateMu.Unlock()
	}()
	<-held

	t0 := time.Now()
	if a.wedgeCheck(t0, threshold) {
		t.Fatal("first failed probe must only start the clock")
	}
	if a.wedgeCheck(t0.Add(time.Minute), threshold) {
		t.Fatal("restart requested before the threshold")
	}
	if !a.wedgeCheck(t0.Add(threshold), threshold) {
		t.Fatal("no restart after the lock was unacquirable for the full threshold")
	}
	if a.wedgeCheck(t0.Add(time.Hour), 0) {
		t.Fatal("JETTY_WEDGE_RESTART_AFTER=0 must disable the restart")
	}

	// One good probe resets the clock: the threshold is about CONTINUOUS
	// failure, not accumulated.
	close(release)
	if a.wedgeCheck(time.Now(), threshold) {
		t.Fatal("restart requested once the lock was free again")
	}
	if a.lockUnacquirableSince.Load() != 0 {
		t.Fatal("wedge clock not reset after a successful probe")
	}
}

func TestRestartWedgedDumpsStacksAndExitsNonZero(t *testing.T) {
	a := newTestAgentWithDir(t)

	var code = -1
	old := wedgeExit
	wedgeExit = func(c int) { code = c }
	t.Cleanup(func() { wedgeExit = old })

	// Silence the deliberate stderr dump for this test.
	devnull, _ := os.Open(os.DevNull)
	oldStderr := os.Stderr
	os.Stderr = devnull
	t.Cleanup(func() { os.Stderr = oldStderr; devnull.Close() })

	a.restartWedged(4*time.Minute, 3*time.Minute)

	if code == 0 || code == -1 {
		t.Fatalf("exit code = %d, want non-zero (Docker's unless-stopped restarts either way, "+
			"but a distinctive code shows it was the watchdog)", code)
	}
	files, _ := filepath.Glob(filepath.Join(a.dataDir, "wedge-dumps", "goroutines-*.txt"))
	if len(files) != 1 {
		t.Fatalf("dump files = %v, want exactly one", files)
	}
	body, _ := os.ReadFile(files[0])
	if !strings.Contains(string(body), "goroutine ") ||
		!strings.Contains(string(body), "TestRestartWedgedDumpsStacksAndExitsNonZero") {
		t.Fatal("dump does not look like an all-goroutine stack dump")
	}
}

func TestSaveWedgeDumpKeepsOnlyTheNewest(t *testing.T) {
	a := newTestAgentWithDir(t)
	dir := filepath.Join(a.dataDir, "wedge-dumps")
	os.MkdirAll(dir, 0700)
	for _, ts := range []string{"20260101T000000Z", "20260102T000000Z", "20260103T000000Z",
		"20260104T000000Z", "20260105T000000Z", "20260106T000000Z"} {
		os.WriteFile(filepath.Join(dir, "goroutines-"+ts+".txt"), []byte("x"), 0600)
	}
	if _, err := a.saveWedgeDump([]byte("new")); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "goroutines-*.txt"))
	if len(files) != wedgeDumpKeep {
		t.Fatalf("kept %d dumps, want %d", len(files), wedgeDumpKeep)
	}
	for _, f := range files {
		if strings.Contains(f, "20260101") || strings.Contains(f, "20260102") {
			t.Errorf("oldest dump %s survived pruning", f)
		}
	}
}

func TestLivezReturns503WhenWatchdogSeesAPersistentWedge(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.startTime = time.Now()
	withStuckStateLock(t, a)

	// The watchdog has been failing for two minutes. Goroutines are low -
	// a quiet node wedges without piling anything up, which is why the
	// goroutine heuristic alone said 200 on 2026-11-18.
	a.lockUnacquirableSince.Store(time.Now().Add(-2 * time.Minute).UnixNano())

	w := httptest.NewRecorder()
	a.apiLivez(w, httptest.NewRequest(http.MethodGet, "/api/livez", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 for a wedged control plane", w.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body["alive"] != false || !strings.Contains(body["diagnosis"].(string), "WEDGED") {
		t.Fatalf("body = %v, want alive=false and a WEDGED diagnosis", body)
	}
	if body["state_lock_unacquirable_seconds"].(float64) < 110 {
		t.Errorf("state_lock_unacquirable_seconds = %v, want ~120", body["state_lock_unacquirable_seconds"])
	}
}

func TestLivezStays200ForAMomentarilyBusyLock(t *testing.T) {
	// A single failed TryRLock with no watchdog history is a write in
	// flight, not a wedge. Returning 503 here would let an external health
	// check pull a healthy node out of rotation.
	a := newTestAgentWithDir(t)
	a.startTime = time.Now()
	withStuckStateLock(t, a)
	a.lockUnacquirableSince.Store(time.Now().Add(-10 * time.Second).UnixNano())

	w := httptest.NewRecorder()
	a.apiLivez(w, httptest.NewRequest(http.MethodGet, "/api/livez", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for a briefly busy lock", w.Code)
	}
}
