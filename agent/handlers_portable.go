package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
)

// =============================================================================
// HTTP Handlers - workload import / export
// =============================================================================
//
// GET  /api/workloads/export?tag=<tag>&names=a,b,c
// POST /api/workloads/import   {mode, reassign_ips, payload}
//
// Use case: take a working set of workloads (often filtered by tag),
// dump them as JSON, paste / restore on a different cluster, on a
// freshly wiped same cluster, or as a manual backup.
//
// Export drops runtime-only fields (Owner, Version) so the same
// payload imports cleanly on any cluster: a fresh cluster won't have
// our peer IDs, and an existing one shouldn't be told who currently
// "owns" the workload - it'll re-elect on import.
//
// Import has three collision-resolution behaviors:
//   - mode="skip"    (default): if a workload by name already exists,
//                    skip it. If just the IP collides, reassign a new
//                    one from the service CIDR (with reassign_ips=
//                    true; otherwise skip). Returns a per-workload
//                    report.
//   - mode="replace": delete-then-create on name collision (same
//                    tombstone semantics as DELETE /api/workloads).
//                    IPs still reassigned on collision.
//   - mode="fail":   first collision aborts. Nothing is written. Use
//                    when you absolutely need atomic import.
//
// Mesh IP collisions auto-reassign by default because the cluster
// resolves workloads by name through /etc/hosts - the IP is a
// transport detail, not an identity. A name collision is a real
// conflict and is reported loudly.

// portableExport is the on-the-wire JSON for export/import.
type portableExport struct {
	Version           string             `json:"version"` // "1"
	ExportedAt        time.Time          `json:"exported_at"`
	Workloads         []portableWorkload `json:"workloads"`
	ReferencedEnvKeys []string           `json:"referenced_env_keys,omitempty"` // env keys used in the compose strings (names only, never values)
}

// portableWorkload mirrors Workload but drops Owner and Version - the
// destination cluster will set both. Tags travel as-is.
type portableWorkload struct {
	Name         string   `json:"name"`
	IP           string   `json:"ip,omitempty"` // hint; auto-reassigned on collision
	Compose      string   `json:"compose,omitempty"`
	ComposeAmd64 string   `json:"compose_amd64,omitempty"`
	ComposeArm64 string   `json:"compose_arm64,omitempty"`
	Revive       bool     `json:"revive"`
	Autostart    bool     `json:"autostart"`
	AllowedNodes []string `json:"allowed_nodes,omitempty"`
	Tags         []string `json:"tags,omitempty"`
}

// envRefRe captures ${VAR_NAME} style references in compose YAML.
// We deliberately don't try to parse YAML - regex on the raw string
// catches all the practical cases.
var envRefRe = regexp.MustCompile(`\$\{([A-Z_][A-Z0-9_]*)\}`)

// apiExportWorkloads godoc
// @Summary Export workloads as JSON
// @Description Returns a portable JSON payload of selected workloads (filtered by ?tag=X or ?names=a,b,c, or all when no filter). Includes the list of env-var keys referenced in compose YAML so the operator knows what to set on the destination cluster. Owner+Version are stripped so the payload imports cleanly anywhere.
// @Tags workloads
// @Param tag query string false "Filter to workloads carrying this tag"
// @Param names query string false "Comma-separated list of workload names"
// @Produce json
// @Success 200 {object} PortableExport
// @Router /workloads/export [get]
func (a *Agent) apiExportWorkloads(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	tag := strings.ToLower(strings.TrimSpace(q.Get("tag")))
	var names map[string]bool
	if raw := q.Get("names"); raw != "" {
		names = make(map[string]bool)
		for _, n := range strings.Split(raw, ",") {
			n = strings.TrimSpace(n)
			if n != "" {
				names[n] = true
			}
		}
	}

	a.stateMu.RLock()
	var picked []*Workload
	for _, wl := range a.state.Workloads {
		if tag != "" && !workloadHasTag(wl, tag) {
			continue
		}
		if names != nil && !names[wl.Name] {
			continue
		}
		picked = append(picked, wl)
	}
	a.stateMu.RUnlock()

	sort.Slice(picked, func(i, j int) bool { return picked[i].Name < picked[j].Name })

	out := portableExport{
		Version:    "1",
		ExportedAt: time.Now().UTC(),
		Workloads:  make([]portableWorkload, 0, len(picked)),
	}
	envKeys := map[string]bool{}
	for _, wl := range picked {
		out.Workloads = append(out.Workloads, portableWorkload{
			Name:         wl.Name,
			IP:           wl.IP,
			Compose:      wl.Compose,
			ComposeAmd64: wl.ComposeAmd64,
			ComposeArm64: wl.ComposeArm64,
			Revive:       wl.Revive,
			Autostart:    wl.Autostart,
			AllowedNodes: append([]string(nil), wl.AllowedNodes...),
			Tags:         append([]string(nil), wl.Tags...),
		})
		for _, src := range []string{wl.Compose, wl.ComposeAmd64, wl.ComposeArm64} {
			for _, m := range envRefRe.FindAllStringSubmatch(src, -1) {
				envKeys[m[1]] = true
			}
		}
	}
	for k := range envKeys {
		out.ReferencedEnvKeys = append(out.ReferencedEnvKeys, k)
	}
	sort.Strings(out.ReferencedEnvKeys)

	// Suggest a filename - browsers honor this on direct GET.
	stamp := time.Now().UTC().Format("20060102-150405")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename="jetty-workloads-%s.json"`, stamp))
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(out)
}

// importRequest is the body of POST /api/workloads/import.
type importRequest struct {
	Mode        string         `json:"mode,omitempty"`         // skip | replace | fail (default: skip)
	ReassignIPs *bool          `json:"reassign_ips,omitempty"` // default: true
	Payload     portableExport `json:"payload"`
}

// importEntry is one line of the import report.
type importEntry struct {
	Name       string `json:"name"`
	Status     string `json:"status"` // imported | replaced | reassigned | skipped | error
	Detail     string `json:"detail,omitempty"`
	OriginalIP string `json:"original_ip,omitempty"`
	AssignedIP string `json:"assigned_ip,omitempty"`
}

type importReport struct {
	Mode    string        `json:"mode"`
	Total   int           `json:"total"`
	Created int           `json:"created"`
	Skipped int           `json:"skipped"`
	Errors  int           `json:"errors"`
	Entries []importEntry `json:"entries"`
}

// apiImportWorkloads godoc
// @Summary Import workloads from a previous export
// @Description Restores workloads from a payload produced by /api/workloads/export. mode controls name-collision handling (skip|replace|fail). reassign_ips (default true) auto-allocates a fresh mesh IP when the requested one is taken. Returns a per-workload report.
// @Tags workloads
// @Accept json
// @Produce json
// @Param request body ImportRequest true "Mode + payload"
// @Success 200 {object} ImportReport
// @Failure 400 {object} ErrorResponse "Invalid mode or payload"
// @Failure 409 {object} ErrorResponse "Atomic-fail mode hit a collision"
// @Router /workloads/import [post]
func (a *Agent) apiImportWorkloads(w http.ResponseWriter, r *http.Request) {
	var req importRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid JSON: "+err.Error())
		return
	}

	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	if mode == "" {
		mode = "skip"
	}
	switch mode {
	case "skip", "replace", "fail":
	default:
		writeError(w, 400, "mode must be one of: skip, replace, fail")
		return
	}

	reassignIPs := true
	if req.ReassignIPs != nil {
		reassignIPs = *req.ReassignIPs
	}

	if req.Payload.Version != "1" && req.Payload.Version != "" {
		writeError(w, 400, fmt.Sprintf("unsupported export version %q (this agent supports v1)", req.Payload.Version))
		return
	}

	report := importReport{Mode: mode, Entries: []importEntry{}}
	report.Total = len(req.Payload.Workloads)

	// In "fail" mode, validate the entire payload up-front so a late
	// failure doesn't leave half the import committed. This means
	// scanning for name collisions, IP collisions (when reassignIPs
	// is false), and invalid fields before doing any state mutations.
	if mode == "fail" {
		if err := a.validateImportPayload(req.Payload.Workloads, reassignIPs); err != nil {
			writeError(w, 409, "import validation failed: "+err.Error())
			return
		}
	}

	for _, src := range req.Payload.Workloads {
		entry := a.importOne(src, mode, reassignIPs)
		report.Entries = append(report.Entries, entry)
		switch entry.Status {
		case "imported", "replaced", "reassigned":
			report.Created++
		case "skipped":
			report.Skipped++
		case "error":
			report.Errors++
		}
	}

	// Trigger a single sync round at the end so the imported
	// workloads propagate to peers in one shot.
	a.broadcastState()
	a.updateHosts()

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(report)
	log.Printf("Import: mode=%s total=%d created=%d skipped=%d errors=%d",
		mode, report.Total, report.Created, report.Skipped, report.Errors)
}

// validateImportPayload returns the first conflict found, used only
// in mode=fail to reject before mutating state.
func (a *Agent) validateImportPayload(workloads []portableWorkload, reassignIPs bool) error {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	existingNames := make(map[string]bool, len(a.state.Workloads))
	for _, wl := range a.state.Workloads {
		existingNames[wl.Name] = true
	}
	for _, src := range workloads {
		if !validNamePattern.MatchString(src.Name) {
			return fmt.Errorf("invalid workload name %q", src.Name)
		}
		if src.Compose == "" && src.ComposeAmd64 == "" && src.ComposeArm64 == "" {
			return fmt.Errorf("%s: no compose payload", src.Name)
		}
		if existingNames[src.Name] {
			return fmt.Errorf("name collision: %s", src.Name)
		}
		if src.IP != "" {
			if _, taken := a.state.Workloads[src.IP]; taken && !reassignIPs {
				return fmt.Errorf("ip collision: %s -> %s (set reassign_ips=true to auto-reassign)", src.Name, src.IP)
			}
		}
	}
	return nil
}

// importOne handles one workload from the import payload.
func (a *Agent) importOne(src portableWorkload, mode string, reassignIPs bool) importEntry {
	entry := importEntry{Name: src.Name, OriginalIP: src.IP}

	// Static validation - same rules as apiCreateWorkload.
	if !validNamePattern.MatchString(src.Name) {
		entry.Status = "error"
		entry.Detail = "invalid workload name"
		return entry
	}
	if src.Compose == "" && src.ComposeAmd64 == "" && src.ComposeArm64 == "" {
		entry.Status = "error"
		entry.Detail = "no compose payload"
		return entry
	}
	if src.IP != "" {
		if ip := strings.TrimSpace(src.IP); ip == "" {
			src.IP = ""
		}
	}
	if normTags, badTag := normalizeTags(src.Tags); badTag != "" {
		entry.Status = "error"
		entry.Detail = fmt.Sprintf("invalid tag %q", badTag)
		return entry
	} else {
		src.Tags = normTags
	}

	// Name collision branch: skip / replace per mode.
	a.stateMu.RLock()
	var existing *Workload
	var existingIP string
	for ip, wl := range a.state.Workloads {
		if wl.Name == src.Name {
			existing = wl
			existingIP = ip
			break
		}
	}
	a.stateMu.RUnlock()

	if existing != nil {
		switch mode {
		case "skip":
			entry.Status = "skipped"
			entry.Detail = "name already exists"
			return entry
		case "replace":
			// Delete the existing one first so we can recreate cleanly.
			if err := a.localDelete(existing.Name, existingIP); err != nil {
				entry.Status = "error"
				entry.Detail = "replace failed: " + err.Error()
				return entry
			}
			entry.Status = "replaced"
		case "fail":
			// validateImportPayload should have caught this. Defensive.
			entry.Status = "error"
			entry.Detail = "name collision (atomic mode)"
			return entry
		}
	} else {
		entry.Status = "imported"
	}

	// IP allocation: if the requested IP is free, use it; otherwise
	// reassign (if allowed) or fail.
	finalIP := src.IP
	a.stateMu.Lock()
	if finalIP != "" {
		if _, taken := a.state.Workloads[finalIP]; taken {
			if !reassignIPs {
				a.stateMu.Unlock()
				entry.Status = "error"
				entry.Detail = "ip collision and reassign_ips=false"
				return entry
			}
			finalIP = ""
		}
	}
	if finalIP == "" {
		finalIP = a.allocateServiceIP()
		if finalIP == "" {
			a.stateMu.Unlock()
			entry.Status = "error"
			entry.Detail = "no available IPs in service CIDR"
			return entry
		}
		if entry.Status == "imported" && src.IP != "" {
			entry.Status = "reassigned"
		}
	}
	entry.AssignedIP = finalIP

	// Build the new workload.
	wl := &Workload{
		Name:         src.Name,
		IP:           finalIP,
		Compose:      src.Compose,
		ComposeAmd64: src.ComposeAmd64,
		ComposeArm64: src.ComposeArm64,
		Revive:       src.Revive,
		Autostart:    src.Autostart,
		AllowedNodes: append([]string(nil), src.AllowedNodes...),
		Tags:         append([]string(nil), src.Tags...),
		Owner:        a.hwid,
		Version:      time.Now().UnixNano(),
	}
	a.state.Workloads[finalIP] = wl
	// Clear any stale tombstone for this IP - we're re-using it.
	delete(a.state.DeletedWorkloads, finalIP)
	a.stateMu.Unlock()

	a.saveState()
	// Don't deploy synchronously - importing N workloads serially would
	// take forever. The state mutation is the canonical source; deploy
	// will be picked up by the next sync/tick or the operator can hit
	// /start later.
	return entry
}
