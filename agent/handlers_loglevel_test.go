package agent

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// JETTY_LOG_LEVEL only applies at startup, so debugging a live node used to
// mean restarting it - which throws away the state you restarted it to look
// at. The failures worth debugging are exactly the ones a restart erases.

func postLevel(t *testing.T, a *Agent, body, key string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/api/log-level", strings.NewReader(body))
	if key != "" {
		r.Header.Set("X-API-Key", key)
	}
	w := httptest.NewRecorder()
	a.apiSetLogLevel(w, r)
	return w
}

func TestSetLogLevelAppliesImmediately(t *testing.T) {
	restoreLogging(t)

	SetLogLevel("info")
	if levelVar.Level() != slog.LevelInfo {
		t.Fatalf("level = %v, want info", levelVar.Level())
	}
	SetLogLevel("debug")
	if levelVar.Level() != slog.LevelDebug {
		t.Errorf("level = %v after switching to debug, want debug", levelVar.Level())
	}
	SetLogLevel("error")
	if levelVar.Level() != slog.LevelError {
		t.Errorf("level = %v after switching to error, want error", levelVar.Level())
	}
}

func TestSetLogLevelRebuildsHandlerSoDebugGetsSource(t *testing.T) {
	// Swapping only the level would leave AddSource as it was at startup, so a
	// runtime switch to debug would log without source positions - the single
	// most useful thing debug adds.
	restoreLogging(t)

	var buf strings.Builder
	initLogging("info", "json", &buf)
	SetLogLevel("debug")
	logInfof("after the switch")

	var entry map[string]interface{}
	line := strings.TrimSpace(buf.String())
	if i := strings.LastIndex(line, "\n"); i != -1 {
		line = line[i+1:]
	}
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("parse: %v (line %q)", err, line)
	}
	if _, ok := entry["source"]; !ok {
		t.Error("no source position after switching to debug at runtime; " +
			"the handler was not rebuilt")
	}
}

func TestLogLevelEndpointRequiresAdmin(t *testing.T) {
	// Debug logging is verbose enough to be its own small denial of service,
	// and it raises the volume of anything logging peer or workload names.
	restoreLogging(t)
	a := newTestAgentWithDir(t)
	a.stateMu.Lock()
	a.state.AdminKey = "admin"
	a.state.Peers["p"] = &Peer{ID: "p", APIKey: "peer-key"}
	a.stateMu.Unlock()

	if w := postLevel(t, a, `{"level":"debug"}`, ""); w.Code != http.StatusUnauthorized {
		t.Errorf("no key: status %d, want 401", w.Code)
	}
	if w := postLevel(t, a, `{"level":"debug"}`, "peer-key"); w.Code != http.StatusUnauthorized {
		t.Errorf("peer key: status %d, want 401 - a peer must not be able to change our log level", w.Code)
	}
	if w := postLevel(t, a, `{"level":"debug"}`, "admin"); w.Code != http.StatusOK {
		t.Errorf("admin key: status %d, want 200", w.Code)
	}
}

func TestLogLevelEndpointRejectsUnknownLevels(t *testing.T) {
	// parseLogLevel falls back for env vars so a typo cannot stop a node
	// booting. The API must not: silently getting info when you asked for
	// "verbose" is worse than being told.
	restoreLogging(t)
	a := newTestAgentWithDir(t)
	a.stateMu.Lock()
	a.state.AdminKey = "admin"
	a.stateMu.Unlock()

	w := postLevel(t, a, `{"level":"verbose"}`, "admin")
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for an unrecognised level", w.Code)
	}
	if !strings.Contains(w.Body.String(), "debug") {
		t.Error("the error should list the valid levels")
	}

	if w := postLevel(t, a, `{}`, "admin"); w.Code != http.StatusBadRequest {
		t.Errorf("empty level: status %d, want 400", w.Code)
	}
}

func TestLogLevelEndpointReportsPreviousLevel(t *testing.T) {
	restoreLogging(t)
	a := newTestAgentWithDir(t)
	a.stateMu.Lock()
	a.state.AdminKey = "admin"
	a.stateMu.Unlock()

	SetLogLevel("warn")
	w := postLevel(t, a, `{"level":"debug"}`, "admin")

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if body["previous"] != "WARN" {
		t.Errorf("previous = %q, want WARN", body["previous"])
	}
	if body["level"] != "DEBUG" {
		t.Errorf("level = %q, want DEBUG", body["level"])
	}
	if body["note"] == "" {
		t.Error("the response should say the change does not survive a restart")
	}
}

func TestGetLogLevelNeedsNoAdmin(t *testing.T) {
	// Reading the level is diagnostic, not a control.
	restoreLogging(t)
	a := newTestAgentWithDir(t)
	SetLogLevel("warn")

	w := httptest.NewRecorder()
	a.apiGetLogLevel(w, httptest.NewRequest(http.MethodGet, "/api/log-level", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body map[string]string
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["level"] != "WARN" {
		t.Errorf("level = %q, want WARN", body["level"])
	}
}

func TestIsKnownLogLevel(t *testing.T) {
	for _, ok := range []string{"debug", "INFO", " warn ", "warning", "error"} {
		if !isKnownLogLevel(ok) {
			t.Errorf("%q should be accepted", ok)
		}
	}
	for _, bad := range []string{"", "verbose", "trace", "fatal", "off"} {
		if isKnownLogLevel(bad) {
			t.Errorf("%q should be rejected", bad)
		}
	}
}
