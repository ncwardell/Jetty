package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// HTTP Handler - bulk workload operations
// =============================================================================
//
// POST /api/workloads/bulk
//
// Body:
//   {
//     "tag":    "media",                   // optional, OR
//     "names":  ["nginx", "redis"],        // optional, OR
//     "all":    true,                      // optional, all workloads
//     "action": "stop"                     // start|stop|restart|delete
//   }
//
// Exactly one selector (tag, names, or all) must be set. Mixing them
// is rejected to avoid surprising AND/OR semantics.
//
// Returns a per-workload result map:
//   {
//     "selected": [...names...],
//     "results": {
//       "nginx": {"ok": true},
//       "redis": {"ok": false, "error": "owner unreachable"}
//     }
//   }
//
// Per-workload failures don't stop the run. A "best-effort" report is
// the more useful shape than all-or-nothing rollback for these
// actions - if 1/10 stops fail, the operator wants the other 9 done.

type bulkRequest struct {
	Tag    string   `json:"tag,omitempty"`
	Names  []string `json:"names,omitempty"`
	All    bool     `json:"all,omitempty"`
	Action string   `json:"action"`
}

type bulkResult struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type bulkSelected struct {
	name  string
	ip    string
	owner string
	local bool
}

// apiBulkWorkload godoc
// @Summary Bulk workload action by selector
// @Description Runs start/stop/restart/delete against a set of workloads selected by tag, names, or all. Exactly one selector must be set. Returns a per-workload result map; remote workloads are proxied to their owner.
// @Tags workloads
// @Accept json
// @Produce json
// @Param request body BulkRequest true "Selector + action"
// @Success 200 {object} BulkResponse
// @Failure 400 {object} ErrorResponse "Bad selector or action"
// @Router /workloads/bulk [post]
func (a *Agent) apiBulkWorkload(w http.ResponseWriter, r *http.Request) {
	var req bulkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	switch req.Action {
	case "start", "stop", "restart", "delete":
	default:
		writeError(w, http.StatusBadRequest, "action must be one of: start, stop, restart, delete")
		return
	}

	selectors := 0
	if req.Tag != "" {
		selectors++
	}
	if len(req.Names) > 0 {
		selectors++
	}
	if req.All {
		selectors++
	}
	if selectors != 1 {
		writeError(w, http.StatusBadRequest, "exactly one selector required: 'tag', 'names', or 'all'")
		return
	}

	a.stateMu.RLock()
	tag := strings.ToLower(strings.TrimSpace(req.Tag))
	wantedNames := make(map[string]bool, len(req.Names))
	for _, n := range req.Names {
		wantedNames[n] = true
	}
	var selected []bulkSelected
	for ip, wl := range a.state.Workloads {
		switch {
		case req.All:
			// pass through
		case tag != "":
			if !workloadHasTag(wl, tag) {
				continue
			}
		case len(req.Names) > 0:
			if !wantedNames[wl.Name] {
				continue
			}
		}
		selected = append(selected, bulkSelected{
			name:  wl.Name,
			ip:    ip,
			owner: wl.Owner,
			local: wl.Owner == a.hwid,
		})
	}
	a.stateMu.RUnlock()

	sort.Slice(selected, func(i, j int) bool { return selected[i].name < selected[j].name })

	// Names that didn't match anything are reported up-front so the
	// caller knows their list was wrong.
	results := make(map[string]bulkResult)
	if len(req.Names) > 0 {
		matched := make(map[string]bool, len(selected))
		for _, s := range selected {
			matched[s.name] = true
		}
		for _, n := range req.Names {
			if !matched[n] {
				results[n] = bulkResult{OK: false, Error: "workload not found"}
			}
		}
	}

	var resultsMu sync.Mutex
	var wg sync.WaitGroup
	// Bound concurrency. 8 in flight is enough for homelab clusters
	// and avoids piling proxied requests onto a single peer.
	sem := make(chan struct{}, 8)

	for _, s := range selected {
		s := s
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			err := a.applyBulkAction(s, req.Action)
			resultsMu.Lock()
			defer resultsMu.Unlock()
			if err != nil {
				results[s.name] = bulkResult{OK: false, Error: err.Error()}
			} else {
				results[s.name] = bulkResult{OK: true}
			}
		}()
	}
	wg.Wait()

	names := make([]string, len(selected))
	for i, s := range selected {
		names[i] = s.name
	}
	resp := map[string]interface{}{
		"selected": names,
		"results":  results,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
	log.Printf("Bulk %s: %d workloads", req.Action, len(selected))
}

// applyBulkAction runs the action against a single workload, either
// locally via the same primitives the single-workload handlers use,
// or by proxying to the owner.
func (a *Agent) applyBulkAction(s bulkSelected, action string) error {
	if !s.local {
		return a.proxyBulkAction(s.name, s.owner, action)
	}

	switch action {
	case "start":
		return a.localStart(s.name)
	case "stop":
		return a.localStop(s.name)
	case "restart":
		if err := a.localStop(s.name); err != nil {
			return fmt.Errorf("stop: %w", err)
		}
		return a.localStart(s.name)
	case "delete":
		return a.localDelete(s.name, s.ip)
	}
	return fmt.Errorf("unknown action %q", action)
}

func (a *Agent) localStart(name string) error {
	a.stateMu.RLock()
	var found *Workload
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			break
		}
	}
	a.stateMu.RUnlock()
	if found == nil {
		return fmt.Errorf("workload not found")
	}
	if out, err := a.composeCmd(found.Name, "start"); err != nil {
		return fmt.Errorf("compose start: %s", out)
	}
	a.setupWorkloadIP(found)
	return nil
}

func (a *Agent) localStop(name string) error {
	a.stateMu.RLock()
	var found *Workload
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			break
		}
	}
	a.stateMu.RUnlock()
	if found == nil {
		return fmt.Errorf("workload not found")
	}
	if out, err := a.composeCmd(found.Name, "stop"); err != nil {
		return fmt.Errorf("compose stop: %s", out)
	}
	return nil
}

// localDelete mirrors apiDeleteWorkload's local-cleanup branch:
// remove from state, write tombstone, remove containers, broadcast.
func (a *Agent) localDelete(name, ip string) error {
	a.stateMu.RLock()
	var found *Workload
	for _, wl := range a.state.Workloads {
		if wl.Name == name {
			found = wl
			break
		}
	}
	a.stateMu.RUnlock()
	if found == nil {
		return fmt.Errorf("workload not found")
	}

	a.stateMu.Lock()
	delete(a.state.Workloads, ip)
	a.state.DeletedWorkloads[ip] = &DeletedWorkload{
		IP:      ip,
		Version: time.Now().UnixNano(),
	}
	a.stateMu.Unlock()

	a.removeWorkload(found)
	a.updateHosts()
	a.saveState()
	a.broadcastState()
	return nil
}

// proxyBulkAction sends the per-workload action to the workload's
// owner node via the existing single-workload endpoint.
func (a *Agent) proxyBulkAction(name string, ownerID string, action string) error {
	a.stateMu.RLock()
	owner := a.state.Peers[ownerID]
	a.stateMu.RUnlock()
	if owner == nil || !owner.Healthy {
		return fmt.Errorf("owner node unreachable")
	}

	method, path := bulkProxyEndpoint(action, name)
	url := a.getPeerAPIURL(owner, path)
	req, err := a.peerRequest(method, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("call owner: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("owner returned %d", resp.StatusCode)
	}
	return nil
}

func bulkProxyEndpoint(action, name string) (method, path string) {
	switch action {
	case "start":
		return "POST", "/api/workloads/" + name + "/start"
	case "stop":
		return "POST", "/api/workloads/" + name + "/stop"
	case "restart":
		return "POST", "/api/workloads/" + name + "/restart"
	case "delete":
		return "DELETE", "/api/workloads/" + name
	}
	return "", ""
}

// workloadHasTag returns true if wl carries the given (already
// normalized: lowercased+trimmed) tag.
func workloadHasTag(wl *Workload, tag string) bool {
	for _, t := range wl.Tags {
		if t == tag {
			return true
		}
	}
	return false
}
