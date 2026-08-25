package agent

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"sort"
	"testing"
)

// e2eAgentForBulk builds an agent with a few workloads and a fake
// "owner" identity so the bulk handler thinks they're all local.
// Bulk actions need to short-circuit before they actually try to run
// docker compose commands, so the actions we exercise here are
// "delete" (which does its own state mutation + tombstone) and
// "stop"/"start" (which would shell out to docker compose - we mock
// by overriding the agent's compose path so the command exists but
// hits a fake). For simplicity we focus on selector + result wiring.
func e2eAgentForBulk(t *testing.T) *Agent {
	t.Helper()
	a := newTestAgentWithDir(t)
	a.hwid = "self-hwid"
	a.hostname = "selfhost"
	a.setWarpIP("100.96.0.1")
	a.composeDir = t.TempDir()
	a.hostsFile = t.TempDir() + "/hosts"
	a.state.AdminKey = "admin-secret"
	a.state.SelfAPIKey = "self-secret"
	a.state.Workloads["10.100.0.1"] = &Workload{
		Name: "nginx", IP: "10.100.0.1", Owner: a.hwid, Tags: []string{"prod", "media"},
	}
	a.state.Workloads["10.100.0.2"] = &Workload{
		Name: "redis", IP: "10.100.0.2", Owner: a.hwid, Tags: []string{"prod"},
	}
	a.state.Workloads["10.100.0.3"] = &Workload{
		Name: "experimental", IP: "10.100.0.3", Owner: a.hwid, Tags: []string{"experimental"},
	}
	return a
}

func bulkPost(t *testing.T, a *Agent, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/api/workloads/bulk", bytes.NewReader(b))
	rec := httptest.NewRecorder()
	a.apiBulkWorkload(rec, req)
	var out map[string]any
	if rec.Code == 200 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec, out
}

func TestBulk_RequiresExactlyOneSelector(t *testing.T) {
	a := e2eAgentForBulk(t)
	cases := []map[string]any{
		{"action": "stop"}, // no selector
		{"action": "stop", "tag": "prod", "all": true},
		{"action": "stop", "tag": "prod", "names": []string{"nginx"}},
	}
	for i, c := range cases {
		rec, _ := bulkPost(t, a, c)
		if rec.Code != 400 {
			t.Errorf("case %d: expected 400, got %d", i, rec.Code)
		}
	}
}

func TestBulk_RejectsInvalidAction(t *testing.T) {
	a := e2eAgentForBulk(t)
	rec, _ := bulkPost(t, a, map[string]any{"action": "explode", "all": true})
	if rec.Code != 400 {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestBulk_TagSelectorPicksRightWorkloads(t *testing.T) {
	a := e2eAgentForBulk(t)
	rec, body := bulkPost(t, a, map[string]any{"action": "delete", "tag": "prod"})
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	selected, _ := body["selected"].([]any)
	got := make([]string, len(selected))
	for i, s := range selected {
		got[i], _ = s.(string)
	}
	sort.Strings(got)
	want := []string{"nginx", "redis"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, n := range got {
		if n != want[i] {
			t.Errorf("got[%d]=%q, want %q", i, n, want[i])
		}
	}
	// Both should report ok=true (delete is purely a state mutation,
	// no docker shellout in this test path).
	results, _ := body["results"].(map[string]any)
	for _, name := range want {
		entry, _ := results[name].(map[string]any)
		if ok, _ := entry["ok"].(bool); !ok {
			t.Errorf("result for %s not ok: %v", name, entry)
		}
	}
}

func TestBulk_NamesSelectorReportsMissing(t *testing.T) {
	a := e2eAgentForBulk(t)
	rec, body := bulkPost(t, a, map[string]any{
		"action": "delete",
		"names":  []string{"nginx", "does-not-exist"},
	})
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	results, _ := body["results"].(map[string]any)
	missing, _ := results["does-not-exist"].(map[string]any)
	if ok, _ := missing["ok"].(bool); ok {
		t.Errorf("missing workload should not be ok")
	}
	if errStr, _ := missing["error"].(string); errStr != "workload not found" {
		t.Errorf("got error %q", errStr)
	}
}

func TestBulk_AllSelector(t *testing.T) {
	a := e2eAgentForBulk(t)
	rec, body := bulkPost(t, a, map[string]any{"action": "delete", "all": true})
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	selected, _ := body["selected"].([]any)
	if len(selected) != 3 {
		t.Errorf("expected 3 selected, got %d (%v)", len(selected), selected)
	}
}
