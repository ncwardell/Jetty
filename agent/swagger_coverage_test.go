package agent

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The spec drifted to 16 endpoints behind the code because nothing connected
// "I registered a route" to "I documented a route". CI regenerates docs/ and
// fails on a diff, which catches a stale *spec* — but regeneration is happy to
// produce a spec that silently omits a handler carrying no @Router annotation.
// This closes that second gap, at the source, before CI.

var (
	routeRe  = regexp.MustCompile(`r\.(?:HandleFunc|PathPrefix)\("([^"]+)"\s*,\s*(?:a\.)?(\w+)`)
	routerRe = regexp.MustCompile(`@Router\s+(\S+)`)
)

// notAPIRoutes are served by the agent but are deliberately absent from the
// OpenAPI spec: they are not part of the API surface.
var notAPIRoutes = map[string]string{
	"/":         "the dashboard HTML",
	"/swagger/": "the Swagger UI itself",
}

func TestEveryRegisteredRouteHasASwaggerAnnotation(t *testing.T) {
	api, err := os.ReadFile("api.go")
	if err != nil {
		t.Fatalf("read api.go: %v", err)
	}

	// Collect every handler function that carries an @Router annotation.
	annotated := map[string]bool{}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		// An annotation block sits directly above its func; associate each
		// @Router with the next function declaration that follows it.
		lines := strings.Split(string(src), "\n")
		pendingRouter := false
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if routerRe.MatchString(trimmed) {
				pendingRouter = true
				continue
			}
			if pendingRouter && strings.HasPrefix(trimmed, "func ") {
				if i := strings.Index(trimmed, ") api"); i != -1 {
					rest := trimmed[i+2:]
					if j := strings.IndexAny(rest, "("); j != -1 {
						annotated[rest[:j]] = true
					}
				}
				pendingRouter = false
			}
		}
	}

	var missing []string
	for _, m := range routeRe.FindAllStringSubmatch(string(api), -1) {
		path, handler := m[1], m[2]
		if reason, ok := notAPIRoutes[strings.TrimPrefix(path, "/api")]; ok {
			_ = reason
			continue
		}
		if !annotated[handler] {
			missing = append(missing, path+" -> "+handler)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("these routes are registered but carry no @Router annotation, "+
			"so they are invisible in docs/swagger.json:\n  %s\n\n"+
			"Add a godoc annotation block above the handler, then run: go generate ./...",
			strings.Join(missing, "\n  "))
	}
}
