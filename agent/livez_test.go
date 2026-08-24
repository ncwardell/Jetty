package agent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Two incidents were misattributed for hours because a wedged agent is
// externally identical to a broken tunnel: the listener stays open (the kernel
// owns it) and nothing answers. /api/health could not report the problem
// because it takes the very lock that was stuck - and the auth middleware took
// that lock before even deciding whether the endpoint needed auth.
//
// These pin the property that makes the next one diagnosable in seconds.

// withStuckStateLock holds stateMu for write until the test finishes,
// reproducing a stalled writer.
func withStuckStateLock(t *testing.T, a *Agent) {
	t.Helper()
	held := make(chan struct{})
	release := make(chan struct{})
	go func() {
		a.stateMu.Lock()
		close(held)
		<-release
		a.stateMu.Unlock()
	}()
	<-held
	t.Cleanup(func() { close(release) })
}

func TestLivezAnswersWhileTheStateLockIsStuck(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.hostname = "node-a"
	a.hwid = "hwid-a"
	a.startTime = time.Now().Add(-time.Minute)

	withStuckStateLock(t, a)

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		a.apiLivez(w, httptest.NewRequest(http.MethodGet, "/api/livez", nil))
		done <- w
	}()

	select {
	case w := <-done:
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
		var body map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("body is not JSON: %v", err)
		}
		if body["state_lock_acquirable"] != false {
			t.Error("state_lock_acquirable should be false while a writer holds the lock")
		}
		if body["diagnosis"] == "healthy" {
			t.Error("diagnosis says healthy while the state lock is held by a stuck writer")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("/api/livez blocked while stateMu was held - it must never " +
			"take the lock it is reporting on, or it is useless in the exact " +
			"situation it exists for")
	}
}

func TestLivezReportsHealthyWhenNothingIsStuck(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.hostname = "node-a"
	a.startTime = time.Now()

	w := httptest.NewRecorder()
	a.apiLivez(w, httptest.NewRequest(http.MethodGet, "/api/livez", nil))

	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body["state_lock_acquirable"] != true {
		t.Error("state_lock_acquirable should be true on an idle agent")
	}
	if body["alive"] != true {
		t.Error("alive should be true on an idle agent")
	}
	if body["node"] != "node-a" {
		t.Errorf("node = %v, want node-a", body["node"])
	}
}

func TestAuthMiddlewareDoesNotLockForPublicPaths(t *testing.T) {
	// The middleware used to take stateMu.RLock() before checking whether the
	// path needed auth at all, so every public endpoint - health included -
	// depended on a lock it had no reason to touch.
	a := newTestAgentWithDir(t)
	a.stateMu.Lock()
	a.state.AdminKey = "secret" // force the auth path to be "enabled"
	a.stateMu.Unlock()

	withStuckStateLock(t, a)

	reached := make(chan string, 1)
	handler := a.apiKeyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached <- r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))

	for _, path := range []string{"/api/health", "/api/livez", "/api/join", "/swagger/index.html", "/"} {
		go func(p string) {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, p, nil))
		}(path)

		select {
		case got := <-reached:
			if got != path {
				t.Errorf("reached %q, expected %q", got, path)
			}
		case <-time.After(3 * time.Second):
			t.Errorf("%s blocked in the auth middleware while stateMu was held - "+
				"public paths must be checked before the lock is taken", path)
		}
	}
}

func TestAuthMiddlewareStillEnforcesKeysOnProtectedPaths(t *testing.T) {
	// Reordering must not have widened the allowlist.
	a := newTestAgentWithDir(t)
	a.stateMu.Lock()
	a.state.AdminKey = "secret"
	a.stateMu.Unlock()

	handler := a.apiKeyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/workloads", nil))
	if w.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated /api/workloads got %d, want 401", w.Code)
	}

	w = httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/workloads", nil)
	r.Header.Set("X-API-Key", "secret")
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("authenticated /api/workloads got %d, want 200", w.Code)
	}
}
