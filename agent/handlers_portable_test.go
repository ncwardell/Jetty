package agent

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"sort"
	"testing"
)

func e2eAgentForPortable(t *testing.T) *Agent {
	t.Helper()
	a := newTestAgentWithDir(t)
	a.hwid = "self-hwid"
	a.hostname = "selfhost"
	a.ip = "100.96.0.1"
	a.composeDir = t.TempDir()
	a.hostsFile = t.TempDir() + "/hosts"
	a.serviceCIDR = "10.100.0.0/16"
	a.state.AdminKey = "admin"
	a.state.SelfAPIKey = "self"
	a.state.Workloads["10.100.0.1"] = &Workload{
		Name: "nginx", IP: "10.100.0.1", Owner: a.hwid,
		Compose: "services:\n  web:\n    image: nginx\n    environment:\n      - DB=${DATABASE_URL}\n",
		Tags:    []string{"prod", "web"},
	}
	a.state.Workloads["10.100.0.2"] = &Workload{
		Name: "redis", IP: "10.100.0.2", Owner: a.hwid,
		Compose: "services:\n  r:\n    image: redis\n    environment:\n      - PASS=${REDIS_PASSWORD}\n",
		Tags:    []string{"prod"},
	}
	return a
}

func TestExport_AllByDefault(t *testing.T) {
	a := e2eAgentForPortable(t)
	req := httptest.NewRequest("GET", "/api/workloads/export", nil)
	rec := httptest.NewRecorder()
	a.apiExportWorkloads(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	var out portableExport
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Workloads) != 2 {
		t.Errorf("want 2, got %d", len(out.Workloads))
	}
	if out.Version != "1" {
		t.Errorf("version: %q", out.Version)
	}
	// Referenced env keys should include both DATABASE_URL and REDIS_PASSWORD, sorted.
	sort.Strings(out.ReferencedEnvKeys)
	want := []string{"DATABASE_URL", "REDIS_PASSWORD"}
	if len(out.ReferencedEnvKeys) != 2 || out.ReferencedEnvKeys[0] != want[0] || out.ReferencedEnvKeys[1] != want[1] {
		t.Errorf("referenced env keys = %v, want %v", out.ReferencedEnvKeys, want)
	}
}

func TestExport_TagFilter(t *testing.T) {
	a := e2eAgentForPortable(t)
	req := httptest.NewRequest("GET", "/api/workloads/export?tag=web", nil)
	rec := httptest.NewRecorder()
	a.apiExportWorkloads(rec, req)
	var out portableExport
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if len(out.Workloads) != 1 || out.Workloads[0].Name != "nginx" {
		t.Errorf("expected just nginx, got %+v", out.Workloads)
	}
}

func TestExport_NamesFilter(t *testing.T) {
	a := e2eAgentForPortable(t)
	req := httptest.NewRequest("GET", "/api/workloads/export?names=redis", nil)
	rec := httptest.NewRecorder()
	a.apiExportWorkloads(rec, req)
	var out portableExport
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if len(out.Workloads) != 1 || out.Workloads[0].Name != "redis" {
		t.Errorf("expected just redis, got %+v", out.Workloads)
	}
}

func TestImport_ReassignsCollidingIP(t *testing.T) {
	a := e2eAgentForPortable(t)
	// Build a payload whose IP collides with an existing workload.
	body := importRequest{
		Mode: "skip", // default; doesn't matter for IP-only collision
		Payload: portableExport{
			Version: "1",
			Workloads: []portableWorkload{{
				Name: "newcomer", IP: "10.100.0.1",
				Compose: "services: { x: { image: nginx } }",
			}},
		},
	}
	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	a.apiImportWorkloads(rec, httptest.NewRequest("POST", "/api/workloads/import", bytes.NewReader(b)))
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var rep importReport
	_ = json.Unmarshal(rec.Body.Bytes(), &rep)
	if rep.Created != 1 || rep.Errors != 0 {
		t.Errorf("report = %+v", rep)
	}
	if rep.Entries[0].Status != "reassigned" {
		t.Errorf("status = %s, want reassigned", rep.Entries[0].Status)
	}
	if rep.Entries[0].AssignedIP == "10.100.0.1" {
		t.Errorf("IP not reassigned: %s", rep.Entries[0].AssignedIP)
	}
	if rep.Entries[0].OriginalIP != "10.100.0.1" {
		t.Errorf("original_ip lost: %s", rep.Entries[0].OriginalIP)
	}
}

func TestImport_SkipOnNameCollision(t *testing.T) {
	a := e2eAgentForPortable(t)
	body := importRequest{
		Mode: "skip",
		Payload: portableExport{
			Version: "1",
			Workloads: []portableWorkload{{
				Name: "nginx", IP: "10.100.0.99",
				Compose: "services: { x: { image: somethingelse } }",
			}},
		},
	}
	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	a.apiImportWorkloads(rec, httptest.NewRequest("POST", "/api/workloads/import", bytes.NewReader(b)))
	var rep importReport
	_ = json.Unmarshal(rec.Body.Bytes(), &rep)
	if rep.Skipped != 1 {
		t.Errorf("expected 1 skipped, got %+v", rep)
	}
	if rep.Entries[0].Status != "skipped" {
		t.Errorf("status = %s", rep.Entries[0].Status)
	}
	// Original nginx still in state.
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if a.state.Workloads["10.100.0.1"].Compose == "services: { x: { image: somethingelse } }" {
		t.Error("existing workload was overwritten despite skip mode")
	}
}

func TestImport_ReplaceOnNameCollision(t *testing.T) {
	a := e2eAgentForPortable(t)
	body := importRequest{
		Mode: "replace",
		Payload: portableExport{
			Version: "1",
			Workloads: []portableWorkload{{
				Name:    "nginx",
				Compose: "services: { x: { image: replacement } }",
				Tags:    []string{"prod"},
			}},
		},
	}
	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	a.apiImportWorkloads(rec, httptest.NewRequest("POST", "/api/workloads/import", bytes.NewReader(b)))
	var rep importReport
	_ = json.Unmarshal(rec.Body.Bytes(), &rep)
	if rep.Created != 1 || rep.Entries[0].Status != "replaced" {
		t.Errorf("report = %+v", rep)
	}
}

func TestImport_FailModeRejectsCollision(t *testing.T) {
	a := e2eAgentForPortable(t)
	body := importRequest{
		Mode: "fail",
		Payload: portableExport{
			Version: "1",
			Workloads: []portableWorkload{{
				Name: "nginx", Compose: "services: { x: { image: nginx } }",
			}},
		},
	}
	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	a.apiImportWorkloads(rec, httptest.NewRequest("POST", "/api/workloads/import", bytes.NewReader(b)))
	if rec.Code != 409 {
		t.Errorf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
	// Original nginx untouched.
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if _, ok := a.state.Workloads["10.100.0.1"]; !ok {
		t.Error("original workload was deleted despite atomic-fail")
	}
}

func TestImport_RoundTripExportThenImport(t *testing.T) {
	a := e2eAgentForPortable(t)

	// 1. Export.
	rec := httptest.NewRecorder()
	a.apiExportWorkloads(rec, httptest.NewRequest("GET", "/api/workloads/export?tag=prod", nil))
	if rec.Code != 200 {
		t.Fatalf("export status %d", rec.Code)
	}
	var exported portableExport
	_ = json.Unmarshal(rec.Body.Bytes(), &exported)
	if len(exported.Workloads) != 2 {
		t.Fatalf("expected 2, got %d", len(exported.Workloads))
	}

	// 2. Wipe state, then import.
	a.stateMu.Lock()
	a.state.Workloads = map[string]*Workload{}
	a.state.DeletedWorkloads = map[string]*DeletedWorkload{}
	a.stateMu.Unlock()

	body := importRequest{Mode: "skip", Payload: exported}
	b, _ := json.Marshal(body)
	rec2 := httptest.NewRecorder()
	a.apiImportWorkloads(rec2, httptest.NewRequest("POST", "/api/workloads/import", bytes.NewReader(b)))
	if rec2.Code != 200 {
		t.Fatalf("import status %d: %s", rec2.Code, rec2.Body.String())
	}
	var rep importReport
	_ = json.Unmarshal(rec2.Body.Bytes(), &rep)
	if rep.Created != 2 {
		t.Errorf("expected 2 created, got %+v", rep)
	}

	// 3. Workloads should be back, with their tags.
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	names := []string{}
	for _, wl := range a.state.Workloads {
		names = append(names, wl.Name)
		if len(wl.Tags) == 0 {
			t.Errorf("workload %s lost tags on round-trip", wl.Name)
		}
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "nginx" || names[1] != "redis" {
		t.Errorf("got %v", names)
	}
}
