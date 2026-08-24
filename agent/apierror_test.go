package agent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The API used to answer with JSON on success and text/plain on failure, with
// no error shape at all - 169 bare http.Error calls against 40-odd JSON
// encoders. Anything programming against it (the dashboard, JettyOS, a script)
// had to sniff the content type to find out what it got back. Every error is
// now the same envelope.

func TestWriteErrorEmitsJSONEnvelope(t *testing.T) {
	w := httptest.NewRecorder()
	writeError(w, http.StatusNotFound, "workload not found")

	if got := w.Code; got != http.StatusNotFound {
		t.Errorf("status = %d, want 404", got)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON: %v (body: %q)", err, w.Body.String())
	}
	if body.Error != "workload not found" {
		t.Errorf("error = %q, want %q", body.Error, "workload not found")
	}
}

func TestWriteErrorEscapesHostileMessages(t *testing.T) {
	// Error strings interpolate user input (workload names, peer names). The
	// envelope has to stay parseable no matter what lands in it.
	for _, msg := range []string{
		`quotes "inside" the message`,
		"newline\ninjected",
		`{"error":"forged"}`,
		"unicode:   ",
	} {
		w := httptest.NewRecorder()
		writeError(w, http.StatusBadRequest, msg)

		var body struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Errorf("body not parseable for %q: %v", msg, err)
			continue
		}
		if body.Error != msg {
			t.Errorf("round-trip mismatch: got %q, want %q", body.Error, msg)
		}
	}
}

// TestNoBareHTTPErrorCalls is a lint-style invariant. The migration is only
// worth doing once; without a guard, the next handler written by muscle memory
// reintroduces a text/plain response and the contract quietly rots.
//
// If you genuinely need a non-JSON error (there is no such case today), add an
// explicit exemption here with the reason.
func TestNoBareHTTPErrorCalls(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	var offenders []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			if strings.Contains(line, "http.Error(") {
				offenders = append(offenders, name+":"+itoa(i+1)+": "+strings.TrimSpace(line))
			}
		}
	}

	if len(offenders) > 0 {
		t.Errorf("http.Error writes text/plain and bypasses the JSON error envelope.\n"+
			"Use writeError(w, status, msg) instead. Offenders:\n  %s",
			strings.Join(offenders, "\n  "))
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
