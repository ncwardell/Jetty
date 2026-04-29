package agent

import (
	"testing"
)

func TestMergeWorkloadStateAddsNewWorkload(t *testing.T) {
	a := newTestAgent("s")
	a.hwid = "us"

	resp := &SyncResponse{
		Workloads: []*Workload{
			{Name: "nginx", IP: "10.100.0.10", Owner: "peer1", Version: 100},
		},
	}
	res := a.mergeWorkloadState(resp)
	if len(res.LostOwnership) != 0 {
		t.Errorf("LostOwnership: got %d, want 0", len(res.LostOwnership))
	}
	if a.state.Workloads["10.100.0.10"] == nil {
		t.Fatal("workload not added")
	}
}

func TestMergeWorkloadStateHigherVersionWins(t *testing.T) {
	a := newTestAgent("s")
	a.hwid = "us"
	a.state.Workloads["10.100.0.10"] = &Workload{
		Name: "nginx", IP: "10.100.0.10", Owner: "peer1", Version: 100,
	}

	// Newer version
	resp := &SyncResponse{
		Workloads: []*Workload{
			{Name: "nginx", IP: "10.100.0.10", Owner: "peer2", Version: 200},
		},
	}
	a.mergeWorkloadState(resp)
	if got := a.state.Workloads["10.100.0.10"].Owner; got != "peer2" {
		t.Errorf("owner: got %q, want peer2", got)
	}
}

func TestMergeWorkloadStateOlderVersionIgnored(t *testing.T) {
	a := newTestAgent("s")
	a.hwid = "us"
	a.state.Workloads["10.100.0.10"] = &Workload{
		Name: "nginx", IP: "10.100.0.10", Owner: "peer1", Version: 200,
	}

	// Older version: should be ignored
	resp := &SyncResponse{
		Workloads: []*Workload{
			{Name: "nginx", IP: "10.100.0.10", Owner: "peer2", Version: 100},
		},
	}
	a.mergeWorkloadState(resp)
	if got := a.state.Workloads["10.100.0.10"].Owner; got != "peer1" {
		t.Errorf("owner: got %q, want peer1 (older version should not win)", got)
	}
}

func TestMergeWorkloadStateLostOwnershipReported(t *testing.T) {
	a := newTestAgent("s")
	a.hwid = "us"
	a.state.Workloads["10.100.0.10"] = &Workload{
		Name: "nginx", IP: "10.100.0.10", Owner: "us", Version: 100,
	}

	// Peer claims ownership with newer version
	resp := &SyncResponse{
		Workloads: []*Workload{
			{Name: "nginx", IP: "10.100.0.10", Owner: "peer1", Version: 200},
		},
	}
	res := a.mergeWorkloadState(resp)
	if len(res.LostOwnership) != 1 || res.LostOwnership[0].Name != "nginx" {
		t.Fatalf("LostOwnership: got %+v, want [nginx]", res.LostOwnership)
	}
	if got := a.state.Workloads["10.100.0.10"].Owner; got != "peer1" {
		t.Errorf("owner: got %q, want peer1", got)
	}
}

func TestMergeWorkloadStateTombstoneRemovesWorkload(t *testing.T) {
	a := newTestAgent("s")
	a.hwid = "us"
	a.state.Workloads["10.100.0.10"] = &Workload{
		Name: "nginx", IP: "10.100.0.10", Owner: "peer1", Version: 100,
	}

	resp := &SyncResponse{
		DeletedWorkloads: []*DeletedWorkload{
			{IP: "10.100.0.10", Version: 200},
		},
	}
	a.mergeWorkloadState(resp)
	if _, exists := a.state.Workloads["10.100.0.10"]; exists {
		t.Error("workload should have been tombstoned")
	}
	if a.state.DeletedWorkloads["10.100.0.10"] == nil {
		t.Error("tombstone should have been recorded")
	}
}

func TestMergeWorkloadStateOlderTombstoneIgnored(t *testing.T) {
	a := newTestAgent("s")
	a.hwid = "us"
	a.state.Workloads["10.100.0.10"] = &Workload{
		Name: "nginx", IP: "10.100.0.10", Owner: "peer1", Version: 200,
	}

	// Older tombstone (e.g., delete-then-recreate scenario)
	resp := &SyncResponse{
		DeletedWorkloads: []*DeletedWorkload{
			{IP: "10.100.0.10", Version: 100},
		},
	}
	a.mergeWorkloadState(resp)
	if _, exists := a.state.Workloads["10.100.0.10"]; !exists {
		t.Error("older tombstone should not have removed the workload")
	}
}

func TestMergeWorkloadStateTombstoneSupersedingNewWorkload(t *testing.T) {
	a := newTestAgent("s")
	a.hwid = "us"
	a.state.DeletedWorkloads["10.100.0.10"] = &DeletedWorkload{
		IP: "10.100.0.10", Version: 200,
	}

	// Incoming workload has older version - tombstone wins
	resp := &SyncResponse{
		Workloads: []*Workload{
			{Name: "nginx", IP: "10.100.0.10", Owner: "peer1", Version: 100},
		},
	}
	a.mergeWorkloadState(resp)
	if _, exists := a.state.Workloads["10.100.0.10"]; exists {
		t.Error("newer tombstone should have shadowed the workload")
	}
}

func TestMergeWorkloadStateRecreateSupersedingTombstone(t *testing.T) {
	a := newTestAgent("s")
	a.hwid = "us"
	a.state.DeletedWorkloads["10.100.0.10"] = &DeletedWorkload{
		IP: "10.100.0.10", Version: 100,
	}

	// Newer workload version - clears the tombstone
	resp := &SyncResponse{
		Workloads: []*Workload{
			{Name: "nginx", IP: "10.100.0.10", Owner: "peer1", Version: 200},
		},
	}
	a.mergeWorkloadState(resp)
	if a.state.Workloads["10.100.0.10"] == nil {
		t.Fatal("recreate should have added the workload back")
	}
	if _, exists := a.state.DeletedWorkloads["10.100.0.10"]; exists {
		t.Error("tombstone should have been cleared by newer workload")
	}
}

func TestMergeWorkloadStateEnvUpdate(t *testing.T) {
	a := newTestAgent("s")
	resp := &SyncResponse{
		EnvData: map[string]string{
			"DATABASE_URL": "v2:abc",
		},
	}
	res := a.mergeWorkloadState(resp)
	if !res.EnvUpdated {
		t.Error("EnvUpdated should be true")
	}
	if a.state.EnvData["DATABASE_URL"] != "v2:abc" {
		t.Errorf("env not stored, got %q", a.state.EnvData["DATABASE_URL"])
	}
}

func TestMergeWorkloadStateEnvDeleteWithTombstone(t *testing.T) {
	a := newTestAgent("s")
	a.state.EnvData["DATABASE_URL"] = "v2:abc"

	resp := &SyncResponse{
		DeletedEnvKeys: []*DeletedEnvKey{
			{Key: "DATABASE_URL", Version: 100},
		},
	}
	res := a.mergeWorkloadState(resp)
	if !res.EnvUpdated {
		t.Error("EnvUpdated should be true after delete")
	}
	if _, exists := a.state.EnvData["DATABASE_URL"]; exists {
		t.Error("env key should have been removed")
	}
}

func TestMergeWorkloadStateTombstonedEnvKeyNotResurrected(t *testing.T) {
	a := newTestAgent("s")
	a.state.DeletedEnvKeys["DATABASE_URL"] = &DeletedEnvKey{
		Key: "DATABASE_URL", Version: 100,
	}

	// Peer happens to still have the value cached. Tombstone should win.
	resp := &SyncResponse{
		EnvData: map[string]string{
			"DATABASE_URL": "v2:stale",
		},
	}
	a.mergeWorkloadState(resp)
	if _, exists := a.state.EnvData["DATABASE_URL"]; exists {
		t.Error("tombstoned env key should not have been re-added")
	}
}
