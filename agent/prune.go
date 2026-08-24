package agent

import (
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// =============================================================================
// Docker image auto-prune
// =============================================================================
//
// Self-updates strand images: every time jetty updates itself or a workload
// re-pulls a moving tag (webomata's :latest changes on every build), the
// previous image loses its tag and sits on disk forever. On a long-lived
// node this quietly eats tens of GB (observed: 51 images, 4 in use, ~26 GB
// dead weight on a 38 GB root disk) - and SD-card nodes hit the wall much
// sooner. imagePruneLoop reclaims that space on a slow cadence.
//
// Safety model, in increasing aggressiveness:
//   1. Dangling images (untagged layers - exactly what a moving-tag re-pull
//      leaves behind): always safe, running containers can't reference them.
//   2. Unused TAGGED images older than JETTY_IMAGE_PRUNE_UNTIL (default
//      168h/7 days): images for stopped-but-recreatable workloads may be
//      removed; the next start re-pulls them. The age filter protects
//      freshly pre-pulled images (pending moves/updates).
//   3. Build cache older than the same cutoff.
//
// Opt out with JETTY_IMAGE_PRUNE=false; tune the age with
// JETTY_IMAGE_PRUNE_UNTIL (Go duration, e.g. "72h").

const (
	imagePruneInterval  = 24 * time.Hour
	imagePruneBootDelay = 30 * time.Minute
	imagePruneDefault   = "168h"
)

// imagePruneLoop runs pruneImages every imagePruneInterval. The first run
// waits imagePruneBootDelay so a node that just booted (possibly mid
// self-update, possibly about to receive moved workloads) doesn't prune
// images that are about to be needed.
func (a *Agent) imagePruneLoop() {
	if strings.EqualFold(getEnv("JETTY_IMAGE_PRUNE", "true"), "false") {
		logInfof("Image prune: disabled (JETTY_IMAGE_PRUNE=false)")
		return
	}
	age := getEnv("JETTY_IMAGE_PRUNE_UNTIL", imagePruneDefault)
	if _, err := time.ParseDuration(age); err != nil {
		logInfof("Image prune: invalid JETTY_IMAGE_PRUNE_UNTIL %q, using %s", age, imagePruneDefault)
		age = imagePruneDefault
	}

	select {
	case <-time.After(imagePruneBootDelay):
	case <-a.stopCh:
		return
	}

	tick := time.NewTicker(imagePruneInterval)
	defer tick.Stop()
	for {
		a.pruneImages(age)
		select {
		case <-tick.C:
		case <-a.stopCh:
			return
		}
	}
}

// pruneImages reclaims disk space from dangling images, old unused images,
// and old build cache. Errors are logged and skipped - prune is maintenance,
// never worth failing loudly over.
func (a *Agent) pruneImages(age string) {
	total := 0.0

	// 1. Dangling (untagged) images - the moving-tag re-pull leftovers.
	total += runPrune("docker", "image", "prune", "-f")

	// 2. Unused tagged images older than the cutoff. Running containers'
	// images are never touched; stopped workloads re-pull on next start.
	total += runPrune("docker", "image", "prune", "-a", "-f", "--filter", "until="+age)

	// 3. Build cache older than the cutoff.
	total += runPrune("docker", "builder", "prune", "-f", "--filter", "until="+age)

	if total > 0 {
		logInfof("Image prune: reclaimed ~%.1f MB", total)
	}
}

// runPrune executes a docker prune command and returns the reclaimed size in
// MB parsed from its "Total reclaimed space: ..." output (0 on any failure).
func runPrune(name string, args ...string) float64 {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		logErrorf("Image prune: %s %s failed: %v", name, strings.Join(args, " "), err)
		return 0
	}
	// Output ends with e.g. "Total reclaimed space: 4.881GB" (or "0B").
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		val, ok := strings.CutPrefix(line, "Total reclaimed space: ")
		if !ok {
			continue
		}
		return parseDockerSize(val)
	}
	return 0
}

// parseDockerSize converts docker's human size strings ("4.881GB", "512.3MB",
// "1.2kB", "0B") to MB. Unknown formats return 0.
func parseDockerSize(s string) float64 {
	s = strings.TrimSpace(s)
	units := []struct {
		suffix string
		mb     float64
	}{
		{"TB", 1024 * 1024}, {"GB", 1024}, {"MB", 1}, {"kB", 1.0 / 1024}, {"B", 1.0 / (1024 * 1024)},
	}
	for _, u := range units {
		if num, ok := strings.CutSuffix(s, u.suffix); ok {
			f, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
			if err != nil {
				return 0
			}
			return f * u.mb
		}
	}
	return 0
}
