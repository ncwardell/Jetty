package agent

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"sync"
	"testing"
)

// Multi-node convergence. The existing sync_test.go covers mergeWorkloadState
// one merge at a time; these run whole clusters through randomized operation
// and gossip orderings, which is where ordering bugs actually live.

// cluster is a set of in-memory agents that gossip by calling
// mergeWorkloadState directly - no HTTP, no docker, no timing.
type cluster struct {
	agents []*Agent
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{}
	for i := 0; i < n; i++ {
		a := &Agent{
			state:    NewState(),
			stateMu:  sync.RWMutex{},
			dataDir:  t.TempDir(),
			hwid:     fmt.Sprintf("node-%02d", i),
			hostname: fmt.Sprintf("host-%02d", i),
		}
		c.agents = append(c.agents, a)
	}
	return c
}

// snapshot renders an agent's replicated state as a comparable value.
func (c *cluster) snapshot(a *Agent) map[string]interface{} {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()

	wl := map[string]string{}
	for ip, w := range a.state.Workloads {
		wl[ip] = fmt.Sprintf("%s/%s/%d", w.Name, w.Owner, w.Version)
	}
	tomb := map[string]int64{}
	for ip, d := range a.state.DeletedWorkloads {
		tomb[ip] = d.Version
	}
	env := map[string]string{}
	for k, v := range a.state.EnvData {
		env[k] = v
	}
	envTomb := map[string]int64{}
	for k, d := range a.state.DeletedEnvKeys {
		envTomb[k] = d.Version
	}
	removed := map[string]int64{}
	for id, rp := range a.state.RemovedPeers {
		removed[id] = rp.Version
	}
	return map[string]interface{}{
		"workloads": wl, "tombstones": tomb, "env": env, "envTombstones": envTomb,
		"removedPeers": removed,
	}
}

// syncResponseOf builds the payload an agent would serve from /api/sync.
func (c *cluster) syncResponseOf(a *Agent) *SyncResponse {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()

	resp := &SyncResponse{EnvData: map[string]string{}}
	for _, w := range a.state.Workloads {
		cp := *w
		resp.Workloads = append(resp.Workloads, &cp)
	}
	for _, d := range a.state.DeletedWorkloads {
		cp := *d
		resp.DeletedWorkloads = append(resp.DeletedWorkloads, &cp)
	}
	for k, v := range a.state.EnvData {
		resp.EnvData[k] = v
	}
	for _, d := range a.state.DeletedEnvKeys {
		cp := *d
		resp.DeletedEnvKeys = append(resp.DeletedEnvKeys, &cp)
	}
	for _, rp := range a.state.RemovedPeers {
		cp := *rp
		resp.RemovedPeers = append(resp.RemovedPeers, &cp)
	}
	return resp
}

func (c *cluster) gossip(from, to *Agent) {
	resp := c.syncResponseOf(from)
	to.stateMu.Lock()
	to.mergeWorkloadState(resp)
	to.stateMu.Unlock()
}

// gossipUntilQuiet runs enough all-pairs rounds for state to settle.
func (c *cluster) gossipUntilQuiet() {
	for round := 0; round < len(c.agents)+2; round++ {
		for _, from := range c.agents {
			for _, to := range c.agents {
				if from != to {
					c.gossip(from, to)
				}
			}
		}
	}
}

func (c *cluster) assertConverged(t *testing.T) {
	t.Helper()
	want := c.snapshot(c.agents[0])
	for _, a := range c.agents[1:] {
		got := c.snapshot(a)
		if !reflect.DeepEqual(want, got) {
			t.Errorf("node %s diverged from %s\n  %s: %v\n  %s: %v",
				a.hwid, c.agents[0].hwid, c.agents[0].hwid, want, a.hwid, got)
		}
	}
}

func TestClusterConvergesUnderRandomOperationOrdering(t *testing.T) {
	// Same operations, different interleavings of local writes and gossip.
	// Every seed must end with every node holding identical state.
	for seed := int64(0); seed < 50; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			c := newCluster(t, 4)

			version := int64(1000)
			nextVersion := func() int64 { version++; return version }

			for op := 0; op < 40; op++ {
				a := c.agents[rng.Intn(len(c.agents))]
				ip := fmt.Sprintf("10.100.0.%d", rng.Intn(5)+1)

				switch rng.Intn(4) {
				case 0: // create or update a workload
					a.stateMu.Lock()
					a.state.Workloads[ip] = &Workload{
						Name: "wl" + ip[len(ip)-1:], IP: ip,
						Owner: a.hwid, Version: nextVersion(),
					}
					delete(a.state.DeletedWorkloads, ip)
					a.stateMu.Unlock()
				case 1: // delete a workload (tombstone)
					a.stateMu.Lock()
					a.state.DeletedWorkloads[ip] = &DeletedWorkload{IP: ip, Version: nextVersion()}
					delete(a.state.Workloads, ip)
					a.stateMu.Unlock()
				case 2: // set an env key
					a.stateMu.Lock()
					k := fmt.Sprintf("KEY_%d", rng.Intn(3))
					a.state.EnvData[k] = fmt.Sprintf("v%d", op)
					delete(a.state.DeletedEnvKeys, k)
					a.stateMu.Unlock()
				case 3: // one random gossip exchange mid-stream
					b := c.agents[rng.Intn(len(c.agents))]
					if a != b {
						c.gossip(a, b)
					}
				}
			}

			c.gossipUntilQuiet()
			c.assertConverged(t)
		})
	}
}

func TestClusterConvergesRegardlessOfGossipDirection(t *testing.T) {
	// Deletion propagation is the classic asymmetry: a tombstone must beat a
	// stale workload no matter which way the first exchange runs.
	for _, reverse := range []bool{false, true} {
		name := "forward"
		if reverse {
			name = "reverse"
		}
		t.Run(name, func(t *testing.T) {
			c := newCluster(t, 3)
			a, b, d := c.agents[0], c.agents[1], c.agents[2]

			a.state.Workloads["10.100.0.1"] = &Workload{
				Name: "app", IP: "10.100.0.1", Owner: a.hwid, Version: 100,
			}
			c.gossip(a, b)
			c.gossip(a, d)

			// b deletes it while d re-publishes an older revision.
			b.state.DeletedWorkloads["10.100.0.1"] = &DeletedWorkload{IP: "10.100.0.1", Version: 200}
			delete(b.state.Workloads, "10.100.0.1")
			d.state.Workloads["10.100.0.1"].Version = 150

			if reverse {
				c.gossip(d, a)
				c.gossip(b, a)
			} else {
				c.gossip(b, a)
				c.gossip(d, a)
			}

			c.gossipUntilQuiet()
			c.assertConverged(t)

			for _, ag := range c.agents {
				if _, alive := ag.state.Workloads["10.100.0.1"]; alive {
					t.Errorf("%s kept a workload the cluster deleted at a higher version", ag.hwid)
				}
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Failover claim safety
// -----------------------------------------------------------------------------

func TestShouldClaimElectsExactlyOneWhenStateAgrees(t *testing.T) {
	c := newCluster(t, 4)
	orphan := &Workload{Name: "app", IP: "10.100.0.1", Owner: "dead-node", Version: 1, Compose: "x"}

	for _, a := range c.agents {
		a.state.Workloads[orphan.IP] = orphan
		for _, peer := range c.agents {
			if peer != a {
				a.state.Peers[peer.hwid] = &Peer{ID: peer.hwid, Name: peer.hostname, Healthy: true, Arch: runtime.GOARCH}
			}
		}
	}

	claimers := 0
	for _, a := range c.agents {
		if a.shouldClaim(orphan) {
			claimers++
		}
	}
	if claimers != 1 {
		t.Errorf("%d nodes elected themselves from identical state, want exactly 1", claimers)
	}
}

func TestElectionAloneIsUnsafeWhenLoadViewsDiverge(t *testing.T) {
	// Characterization, not an aspiration: shouldClaim ranks candidates by
	// workload counts read from *local* state, so nodes whose maps have not
	// converged can both rank themselves first. This is exactly why the
	// claim is announced and re-checked before deploying - see
	// TestClaimStillHeld* below. If a future change makes the election
	// globally safe on its own, this test should start failing and be
	// deleted along with the settle.
	c := newCluster(t, 2)
	a, b := c.agents[0], c.agents[1]
	orphan := &Workload{Name: "app", IP: "10.100.0.1", Owner: "dead-node", Version: 1, Compose: "x"}

	for _, ag := range c.agents {
		ag.state.Workloads[orphan.IP] = orphan
		other := a
		if ag == a {
			other = b
		}
		ag.state.Peers[other.hwid] = &Peer{ID: other.hwid, Name: other.hostname, Healthy: true, Arch: runtime.GOARCH}
	}

	// a knows about a workload owned by b; b knows about one owned by a.
	// Each therefore believes the other is the busier node.
	a.state.Workloads["10.100.0.9"] = &Workload{Name: "x", IP: "10.100.0.9", Owner: b.hwid, Version: 1, Compose: "x"}
	b.state.Workloads["10.100.0.8"] = &Workload{Name: "y", IP: "10.100.0.8", Owner: a.hwid, Version: 1, Compose: "x"}

	if !a.shouldClaim(orphan) || !b.shouldClaim(orphan) {
		t.Skip("election no longer double-claims under divergent load views; the settle may be removable")
	}
}

func TestClaimStillHeldAcceptsOurUncontestedClaim(t *testing.T) {
	c := newCluster(t, 1)
	a := c.agents[0]
	a.state.Workloads["10.100.0.1"] = &Workload{
		Name: "app", IP: "10.100.0.1", Owner: a.hwid, Version: 500,
	}
	if !a.claimStillHeld("10.100.0.1", 500) {
		t.Error("uncontested claim reported as lost")
	}
}

func TestClaimStillHeldRejectsPeerTakeover(t *testing.T) {
	// The case that matters: two nodes claimed concurrently, the peer's
	// higher version won the merge, and we must not start the container.
	c := newCluster(t, 1)
	a := c.agents[0]
	a.state.Workloads["10.100.0.1"] = &Workload{
		Name: "app", IP: "10.100.0.1", Owner: "other-node", Version: 600,
	}
	if a.claimStillHeld("10.100.0.1", 500) {
		t.Error("claim reported as held after a peer took ownership")
	}
}

func TestClaimStillHeldRejectsNewerRepublish(t *testing.T) {
	c := newCluster(t, 1)
	a := c.agents[0]
	a.state.Workloads["10.100.0.1"] = &Workload{
		Name: "app", IP: "10.100.0.1", Owner: a.hwid, Version: 700,
	}
	if a.claimStillHeld("10.100.0.1", 500) {
		t.Error("claim reported as held after a newer record superseded it")
	}
}

func TestClaimStillHeldRejectsDeletedWorkload(t *testing.T) {
	c := newCluster(t, 1)
	if c.agents[0].claimStillHeld("10.100.0.1", 500) {
		t.Error("claim reported as held for a workload that no longer exists")
	}
}

func TestLosingClaimantConvergesWithoutDeploying(t *testing.T) {
	// End to end at the state layer: both nodes claim, they gossip, and
	// exactly one still holds its claim afterwards. The other's
	// claimStillHeld is false, which is what stops it deploying.
	c := newCluster(t, 2)
	a, b := c.agents[0], c.agents[1]

	base := &Workload{Name: "app", IP: "10.100.0.1", Owner: "dead-node", Version: 1}
	for _, ag := range c.agents {
		cp := *base
		ag.state.Workloads[base.IP] = &cp
	}

	aVersion, bVersion := int64(1000), int64(1001) // b claimed marginally later
	a.state.Workloads[base.IP].Owner, a.state.Workloads[base.IP].Version = a.hwid, aVersion
	b.state.Workloads[base.IP].Owner, b.state.Workloads[base.IP].Version = b.hwid, bVersion

	c.gossipUntilQuiet()
	c.assertConverged(t)

	held := 0
	if a.claimStillHeld(base.IP, aVersion) {
		held++
	}
	if b.claimStillHeld(base.IP, bVersion) {
		held++
	}
	if held != 1 {
		t.Errorf("%d nodes still hold the claim after convergence, want exactly 1", held)
	}
}
