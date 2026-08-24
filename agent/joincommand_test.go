package agent

import (
	"os"
	"strings"
	"testing"
)

// readAgentSource reads a file from this package's source directory. Used by
// the guard tests that assert on how code is written rather than what it does
// - log levels, absent API calls - which have no runtime handle to check.
func readAgentSource(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// The Cloudflare Mesh migration broke the generated join command and nothing
// noticed: buildJoinDockerRun kept emitting JETTY_JOIN and JETTY_JOIN_TOKEN
// only, so every node joined with the cluster-shared WARP token. Mesh
// registers shared-token nodes as active-passive replicas of one identity and
// the passive ones drop all traffic - while still joining, gossiping, and
// showing green in the dashboard.
//
// The dashboard has no JS test harness, so these assert against the embedded
// HTML. Coarse, but they pin the exact thing that rotted, and they fail in
// milliseconds instead of after a node is mis-provisioned.

func TestJoinCommandCarriesPerNodeWarpToken(t *testing.T) {
	html := string(dashboardHTML)

	for _, want := range []struct{ needle, why string }{
		{"createTokenWarpToken", "the create-token modal needs a field for the per-node Mesh token"},
		// The env var and the interpolation must appear *together*. Checking
		// for them separately passes even when the value is never substituted,
		// which produces a command that looks right and provisions a
		// blackhole node.
		{`JETTY_WARP_CONNECTOR_TOKEN='${warpToken}'`, "the generated docker run must interpolate the token, not just name the variable"},
	} {
		if !strings.Contains(html, want.needle) {
			t.Errorf("dashboard is missing %q: %s", want.needle, want.why)
		}
	}
}

func TestJoinCommandWarnsWhenMeshTokenOmitted(t *testing.T) {
	// Jetty cannot mint these tokens - they come from the Cloudflare
	// dashboard - so a blank field is always possible. The only defence is
	// that it is loud, on the screen the operator actually reads.
	html := string(dashboardHTML)

	if !strings.Contains(html, "tokenResultWarpWarning") {
		t.Error("the token-result screen has no warning element for a missing Mesh token; " +
			"that screen is the one operators read, and a blank token is silent everywhere else")
	}
	if !strings.Contains(html, "passive replica") {
		t.Error("the warning does not explain the failure mode; " +
			"'drops all traffic while looking healthy' is the part that matters")
	}
}

func TestSharedWarpTokenFallbackWarnsNotInforms(t *testing.T) {
	// Guards the level, not the text. A node silently becoming a traffic
	// blackhole is not an informational event, and anyone filtering to warn+
	// in production must still see it.
	src := readAgentSource(t, "join.go")

	idx := strings.Index(src, "adopting the cluster-shared token")
	if idx == -1 {
		t.Fatal("shared-token fallback log message not found in join.go")
	}
	// Walk back to the start of the call to see which helper is used.
	start := strings.LastIndex(src[:idx], "log")
	if start == -1 || !strings.HasPrefix(src[start:], "logWarnf") {
		t.Errorf("shared-token fallback should log at warn level, got %q",
			firstToken(src[start:]))
	}
}

func firstToken(s string) string {
	if i := strings.IndexAny(s, "("); i != -1 {
		return s[:i]
	}
	if len(s) > 20 {
		return s[:20]
	}
	return s
}
