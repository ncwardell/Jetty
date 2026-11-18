package agent

import (
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// =============================================================================
// Stateful failover guard
// =============================================================================
//
// 2026-11-18: vaultwarden (named volumes, backed up every 60s to cluster
// storage by sidecars) was failed over to Hetzner while the Radxa copy was
// still live and taking writes - Radxa's agent was wedged, so to the cluster
// it looked dead after FailoverGracePeriod (30s). The new copy restored from
// a backup that turned out to be six days old. Two live copies, diverging.
//
// For a stateless workload a duplicate is harmless and fast failover is the
// whole point. For a stateful one, "the owner's agent stopped answering"
// is very weak evidence that the owner's CONTAINERS stopped serving: the
// agent can be wedged, restarting, updating, or partitioned from peers while
// users still reach the workload. Starting a second copy from backup then
// guarantees divergence, and the merge that follows used to make it worse
// (see releaseLostWorkload).
//
// So stateful workloads get a much longer grace (JETTY_STATEFUL_FAILOVER_GRACE,
// default 1h, 0 = never automatically) and an explicit per-workload switch
// (auto_failover). The safe behaviour is the default; fast failover for a
// stateful workload is an explicit opt-in.
//
// Backup freshness is NOT checked: backups are written by the workload's own
// sidecars straight to cluster storage, and Jetty has no record of when the
// last one succeeded (its own /api/backup covers cluster state, not workload
// data). The long grace is what stands in for it. If sidecars ever publish a
// heartbeat Jetty can read, this is where a freshness gate belongs.

// StatefulFailoverGraceDefault is how long a stateful workload's owner must be
// continuously unreachable before a peer claims it. Long because the cost of
// a false positive is data divergence, while the cost of waiting is downtime
// on a node that is usually coming back by itself (a restart, an update, a
// wedge the watchdog resolves in minutes, a home connection that drops).
const StatefulFailoverGraceDefault = 1 * time.Hour

// statefulFailoverGrace reads JETTY_STATEFUL_FAILOVER_GRACE. 0 means stateful
// workloads are never failed over automatically.
func statefulFailoverGrace() time.Duration {
	return getEnvDuration("JETTY_STATEFUL_FAILOVER_GRACE", StatefulFailoverGraceDefault)
}

// failoverPolicy is the decision for one workload.
type failoverPolicy struct {
	auto     bool          // may be claimed automatically at all
	grace    time.Duration // owner must be unreachable this long first
	stateful bool
	reason   string // for logs
}

// failoverPolicyFor decides how (and whether) wl may be failed over. Revive is
// checked by the caller; this layers on top of it.
func failoverPolicyFor(wl *Workload) failoverPolicy {
	stateful := workloadIsStateful(wl)
	if wl.AutoFailover != nil {
		if !*wl.AutoFailover {
			return failoverPolicy{auto: false, stateful: stateful, reason: "auto_failover=false"}
		}
		return failoverPolicy{auto: true, grace: FailoverGracePeriod, stateful: stateful,
			reason: "auto_failover=true"}
	}
	if !stateful {
		return failoverPolicy{auto: true, grace: FailoverGracePeriod, reason: "stateless"}
	}
	grace := statefulFailoverGrace()
	if grace == 0 {
		return failoverPolicy{auto: false, stateful: true,
			reason: "stateful, and JETTY_STATEFUL_FAILOVER_GRACE=0"}
	}
	return failoverPolicy{auto: true, grace: grace, stateful: true, reason: "stateful"}
}

// workloadIsStateful reports whether wl keeps data on its node. The explicit
// field wins; otherwise any compose variant that persists data marks it.
// Checking every arch variant (not just ours) keeps the answer identical on
// every node, which matters because every node evaluates failover itself.
func workloadIsStateful(wl *Workload) bool {
	if wl.Stateful != nil {
		return *wl.Stateful
	}
	for _, c := range []string{wl.Compose, wl.ComposeAmd64, wl.ComposeArm64} {
		if c != "" && composeIsStateful(c) {
			return true
		}
	}
	return false
}

// composeIsStateful reports whether a compose file persists data locally:
// any top-level named volume, or any service volume that is a named or
// anonymous volume or a writable bind mount. Read-only mounts, tmpfs and the
// docker socket do not count. A compose file we cannot parse counts as
// stateful - when unsure, choose the behaviour that cannot lose data.
func composeIsStateful(compose string) bool {
	var doc struct {
		Services map[string]struct {
			Volumes []yaml.Node `yaml:"volumes"`
		} `yaml:"services"`
		Volumes map[string]yaml.Node `yaml:"volumes"`
	}
	if err := yaml.Unmarshal([]byte(compose), &doc); err != nil {
		return true
	}
	if len(doc.Volumes) > 0 {
		return true
	}
	for _, svc := range doc.Services {
		for i := range svc.Volumes {
			if serviceVolumePersists(&svc.Volumes[i]) {
				return true
			}
		}
	}
	return false
}

// serviceVolumePersists classifies one entry of a service's volumes list, in
// either compose syntax.
func serviceVolumePersists(n *yaml.Node) bool {
	switch n.Kind {
	case yaml.ScalarNode:
		// Short syntax: [SOURCE:]TARGET[:MODE]
		parts := strings.Split(n.Value, ":")
		if len(parts) == 1 {
			return true // anonymous volume - data lives in it
		}
		src := parts[0]
		if len(parts) >= 3 && hasMountOption(parts[len(parts)-1], "ro") {
			return false
		}
		return !isDockerSocket(src)
	case yaml.MappingNode:
		var long struct {
			Type     string `yaml:"type"`
			Source   string `yaml:"source"`
			ReadOnly bool   `yaml:"read_only"`
		}
		if err := n.Decode(&long); err != nil {
			return true
		}
		if long.ReadOnly {
			return false
		}
		switch long.Type {
		case "tmpfs", "npipe", "image", "cluster":
			return false
		case "bind":
			return !isDockerSocket(long.Source)
		default: // "volume" or unset (compose defaults to volume)
			return true
		}
	}
	return false
}

func hasMountOption(mode, opt string) bool {
	for _, o := range strings.Split(mode, ",") {
		if strings.TrimSpace(o) == opt {
			return true
		}
	}
	return false
}

// isDockerSocket: mounting the docker socket is control, not data.
func isDockerSocket(src string) bool {
	return strings.HasSuffix(src, "/docker.sock")
}

// warnFailoverHeld logs that a claim is being withheld by the stateful
// policy, rate-limited per workload. Keyed separately from the
// marooned/partitioned throttle so one does not suppress the other.
func (a *Agent) warnFailoverHeld(wl *Workload, down time.Duration, p failoverPolicy) {
	key := "failover-held:" + wl.IP
	a.maroonedLoggedMu.Lock()
	if last, ok := a.maroonedLogged[key]; ok && time.Since(last) < 10*time.Minute {
		a.maroonedLoggedMu.Unlock()
		return
	}
	a.maroonedLogged[key] = time.Now()
	a.maroonedLoggedMu.Unlock()

	if !p.auto {
		logWarnf("Failover: NOT claiming %s (owner %s unreachable for %v) - automatic failover is off (%s). "+
			"Move it manually if the owner is really gone.",
			wl.Name, shortID(wl.Owner, 12), down.Round(time.Second), p.reason)
		return
	}
	logWarnf("Failover: holding %s (owner %s unreachable for %v) - %s workload, waiting for %v before claiming "+
		"so a live copy on the owner is not duplicated from backup. Set auto_failover=true on the workload to "+
		"fail it over on the short grace, or move it manually.",
		wl.Name, shortID(wl.Owner, 12), down.Round(time.Second), p.reason, p.grace)
}

// preserveLostWorkload decides whether our local copy (local) of a workload we
// no longer own must be kept rather than removed, given what state now holds
// at its IP (current). Pure, so the decision is testable without docker.
//
// Preserve only when the workload still exists under the same name (it moved,
// it was not deleted) and our copy is stateful. Stateless copies are cheap to
// recreate and are removed as before; a deleted workload is removed with its
// volumes because deleting it is exactly what the operator asked for.
func preserveLostWorkload(local, current *Workload) bool {
	if current == nil || current.Name != local.Name {
		return false
	}
	return workloadIsStateful(local)
}

// releaseLostWorkload stops our local copy of a workload whose ownership the
// cluster has given to another node.
//
// It used to be removeWorkload for everything, which is `docker compose down
// -v`: it destroys every named volume. For a stateful workload that loses
// ownership through failover, the local copy is very often the one with the
// NEWEST data - it was live the whole time the cluster thought us dead, while
// the claimant restored from backup. Deleting it destroyed the only copy of
// those writes, and the stale restore won by default.
//
// So for stateful workloads we stop the containers (no traffic, no writes,
// mesh IP released so the cluster routes to the new owner) and keep the
// volumes and compose directory, and say loudly what happened and what the
// operator's options are. We deliberately do NOT re-claim the workload:
// ownership is last-write-wins, the claimant may have taken writes too, and
// no automated rule can tell which copy is the one to keep.
//
// A workload that is gone from state entirely was deleted (a tombstone won),
// not moved, and is removed exactly as before.
func (a *Agent) releaseLostWorkload(wl *Workload) {
	a.stateMu.RLock()
	current := a.state.Workloads[wl.IP]
	newOwner := ""
	if current != nil {
		newOwner = current.Owner
		if p := a.state.Peers[newOwner]; p != nil && p.Name != "" {
			newOwner = p.Name
		}
	}
	a.stateMu.RUnlock()

	if !preserveLostWorkload(wl, current) {
		a.removeWorkload(wl)
		return
	}

	logErrorf("SPLIT-BRAIN RISK: stateful workload %s (%s) was claimed by %s while this node was running it. "+
		"Stopping the local copy and PRESERVING its volumes (no `down -v`). The two copies may have "+
		"diverged - compare their data before deleting either. To keep THIS node's copy: move the workload "+
		"back here (POST /api/workloads/%s/move {\"to\":\"%s\"}); its preserved volumes are reused, and the "+
		"other node's copy is removed with its volumes. To keep the new owner's copy: once reconciled, delete "+
		"the stale local data with `docker compose -p jetty_%s down -v` on this node - otherwise a later move "+
		"back here would start from these old volumes instead of restoring.",
		wl.Name, wl.IP, shortID(newOwner, 12), wl.Name, a.hostname, wl.Name)

	if wl.IP != "" {
		a.cleanupWorkloadIP(wl)
	}
	if out, err := a.composeCmd(wl.Name, "stop"); err != nil {
		logErrorf("Failed to stop local copy of %s: %v: %s", wl.Name, err, strings.TrimSpace(out))
		return
	}
	logInfof("Stopped local copy of %s (containers and volumes preserved)", wl.Name)
}
