package agent

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newAgentWithDataDir builds a test agent with a real temp data dir so
// backup/restore can actually write files. AdminKey is set so the
// admin-gated handlers admit testAdminKey via X-API-Key.
func newAgentWithDataDir(t *testing.T) *Agent {
	t.Helper()
	dir := t.TempDir()
	composeDir := filepath.Join(dir, "compose")
	os.MkdirAll(composeDir, 0700)
	a := newTestAgent("s")
	a.dataDir = dir
	a.composeDir = composeDir
	a.state.AdminKey = testAdminKey
	return a
}

const testAdminKey = "test-admin-key"

// withAdminAuth attaches the admin X-API-Key header used by the
// admin-gated handlers in tests.
func withAdminAuth(req *http.Request) *http.Request {
	req.Header.Set("X-API-Key", testAdminKey)
	return req
}

func TestBackupRestoreRoundTrip(t *testing.T) {
	a := newAgentWithDataDir(t)

	// Seed: state.json + a workload's compose dir.
	a.state.Workloads["10.100.0.1"] = &Workload{
		Name: "nginx", IP: "10.100.0.1", Owner: a.hwid, Compose: "services:\n  web: { image: nginx }",
	}
	a.saveState()

	wlDir := filepath.Join(a.composeDir, "nginx")
	os.MkdirAll(wlDir, 0700)
	if err := os.WriteFile(filepath.Join(wlDir, "docker-compose.yml"), []byte("services:\n  web: { image: nginx }\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Backup -> bytes.
	rec := httptest.NewRecorder()
	req := withAdminAuth(httptest.NewRequest("GET", "/api/backup", nil))
	a.apiBackup(rec, req)
	if rec.Code != 200 {
		t.Fatalf("backup status: got %d, body=%s", rec.Code, rec.Body.String())
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/gzip") {
		t.Errorf("Content-Type: got %q", rec.Header().Get("Content-Type"))
	}
	archive := rec.Body.Bytes()
	if len(archive) < 100 {
		t.Fatalf("archive suspiciously small: %d bytes", len(archive))
	}

	// Verify the archive contents directly.
	gzReader, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gzReader)
	seenStateJSON := false
	seenWorkloadCompose := false
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		if hdr.Name == "state.json" {
			seenStateJSON = true
		}
		if hdr.Name == "compose/nginx/docker-compose.yml" {
			seenWorkloadCompose = true
		}
	}
	if !seenStateJSON {
		t.Error("backup missing state.json")
	}
	if !seenWorkloadCompose {
		t.Error("backup missing compose/nginx/docker-compose.yml")
	}

	// Restore into a fresh agent.
	a2 := newAgentWithDataDir(t)
	rec2 := httptest.NewRecorder()
	req2 := withAdminAuth(httptest.NewRequest("POST", "/api/restore", bytes.NewReader(archive)))
	a2.apiRestore(rec2, req2)
	if rec2.Code != 200 {
		t.Fatalf("restore status: got %d, body=%s", rec2.Code, rec2.Body.String())
	}

	// Verify state.json landed and parses.
	stateBytes, err := os.ReadFile(filepath.Join(a2.dataDir, "state.json"))
	if err != nil {
		t.Fatalf("state.json missing after restore: %v", err)
	}
	if !strings.Contains(string(stateBytes), "10.100.0.1") {
		t.Errorf("restored state.json doesn't contain expected workload IP")
	}

	// Verify compose dir landed.
	if _, err := os.Stat(filepath.Join(a2.composeDir, "nginx", "docker-compose.yml")); err != nil {
		t.Errorf("nginx compose missing after restore: %v", err)
	}
}

func TestRestoreRejectsPathTraversal(t *testing.T) {
	a := newAgentWithDataDir(t)

	// Build an evil archive that tries to escape via "../".
	var gzBuf bytes.Buffer
	gz := gzip.NewWriter(&gzBuf)
	tw := tar.NewWriter(gz)
	// Required state.json so we get past that early validation.
	stateJSON := []byte(`{"peers":{},"workloads":{}}`)
	tw.WriteHeader(&tar.Header{Name: "state.json", Size: int64(len(stateJSON)), Mode: 0644})
	tw.Write(stateJSON)
	// Path traversal entry.
	tw.WriteHeader(&tar.Header{Name: "../../../tmp/owned", Size: 4, Mode: 0644})
	tw.Write([]byte("evil"))
	tw.Close()
	gz.Close()

	rec := httptest.NewRecorder()
	req := withAdminAuth(httptest.NewRequest("POST", "/api/restore", bytes.NewReader(gzBuf.Bytes())))
	a.apiRestore(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("restore should reject path traversal, got status %d", rec.Code)
	}
}

func TestRestoreRejectsArchiveWithoutStateJSON(t *testing.T) {
	a := newAgentWithDataDir(t)

	var gzBuf bytes.Buffer
	gz := gzip.NewWriter(&gzBuf)
	tw := tar.NewWriter(gz)
	body := []byte("not a state file")
	tw.WriteHeader(&tar.Header{Name: "random.txt", Size: int64(len(body)), Mode: 0644})
	tw.Write(body)
	tw.Close()
	gz.Close()

	rec := httptest.NewRecorder()
	req := withAdminAuth(httptest.NewRequest("POST", "/api/restore", bytes.NewReader(gzBuf.Bytes())))
	a.apiRestore(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 without state.json, got %d", rec.Code)
	}
}

func TestRestoreRejectsBadStateJSON(t *testing.T) {
	a := newAgentWithDataDir(t)

	var gzBuf bytes.Buffer
	gz := gzip.NewWriter(&gzBuf)
	tw := tar.NewWriter(gz)
	body := []byte("{not valid json")
	tw.WriteHeader(&tar.Header{Name: "state.json", Size: int64(len(body)), Mode: 0644})
	tw.Write(body)
	tw.Close()
	gz.Close()

	rec := httptest.NewRecorder()
	req := withAdminAuth(httptest.NewRequest("POST", "/api/restore", bytes.NewReader(gzBuf.Bytes())))
	a.apiRestore(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bad state.json, got %d", rec.Code)
	}
}

func TestRestoreRejectsBadGzip(t *testing.T) {
	a := newAgentWithDataDir(t)
	rec := httptest.NewRecorder()
	req := withAdminAuth(httptest.NewRequest("POST", "/api/restore", strings.NewReader("not a gzip stream")))
	a.apiRestore(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for non-gzip body, got %d", rec.Code)
	}
}

// Regression: peer-API-key must NOT be enough to download the backup
// (which contains AdminKey + EncryptionKey + every peer's APIKey).
func TestBackupRejectsPeerKey(t *testing.T) {
	a := newAgentWithDataDir(t)
	a.state.Peers["p1"] = &Peer{ID: "p1", APIKey: "peer1-secret"}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/backup", nil)
	req.Header.Set("X-API-Key", "peer1-secret")
	a.apiBackup(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("peer key should be rejected, got %d", rec.Code)
	}
}

func TestRestoreRejectsPeerKey(t *testing.T) {
	a := newAgentWithDataDir(t)
	a.state.Peers["p1"] = &Peer{ID: "p1", APIKey: "peer1-secret"}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/restore", strings.NewReader("doesntmatter"))
	req.Header.Set("X-API-Key", "peer1-secret")
	a.apiRestore(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("peer key should be rejected, got %d", rec.Code)
	}
}
