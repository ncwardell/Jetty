package agent

import (
	"io"
	"net/http"
	"os/exec"
	"sort"
	"strings"
)

// =============================================================================
// HTTP Handlers - host-level Docker introspection
// =============================================================================
//
// These endpoints surface all Docker containers / compose projects on the
// host, not just the Jetty-managed ones. Useful when Jetty is sharing a
// box with other docker workloads and you want one pane of glass.
//
//   GET /api/host/containers   flat list of every container on the host
//   GET /api/host/compose      same data grouped by docker-compose project
//
// Per-node only - cluster-wide aggregation is a UI concern; the dashboard
// can fan out via /api/proxy/{ip}/... to other nodes.
//
// Filters out Jetty's own agent container so we don't show ourselves.

// HostContainer is the per-container shape returned by /api/host/containers.
type HostContainer struct {
	ID             string   `json:"id"`     // short ID
	Name           string   `json:"name"`   // primary container name (without leading /)
	Image          string   `json:"image"`  // image:tag
	State          string   `json:"state"`  // "running", "exited", "paused", ...
	Status         string   `json:"status"` // human-readable e.g. "Up 5 hours"
	ComposeProject string   `json:"compose_project,omitempty"`
	ComposeService string   `json:"compose_service,omitempty"`
	Ports          []string `json:"ports,omitempty"` // e.g. "0.0.0.0:80->80/tcp"
	ManagedByJetty bool     `json:"managed_by_jetty"`
	WorkloadName   string   `json:"workload_name,omitempty"` // populated when managed_by_jetty
	CreatedAt      int64    `json:"created_at,omitempty"`    // unix seconds
}

// HostComposeProject groups containers by compose project for the
// project-oriented view.
type HostComposeProject struct {
	Name           string          `json:"name"`
	ManagedByJetty bool            `json:"managed_by_jetty"`
	WorkloadName   string          `json:"workload_name,omitempty"` // populated when managed_by_jetty
	Containers     []HostContainer `json:"containers"`
}

// parseHostContainerLine parses one tab-separated line from `docker ps`
// (with the hostContainersFormat template) into a HostContainer.
// Returns false if the line is empty or doesn't have enough fields.
//
// Public for testability - the production path in listHostContainers
// calls this for each line.
func parseHostContainerLine(line string) (HostContainer, bool) {
	if line == "" {
		return HostContainer{}, false
	}
	fields := strings.Split(line, "\t")
	if len(fields) < 9 {
		return HostContainer{}, false
	}

	// Names is comma-separated if a container has multiple names; take
	// the first and strip the leading "/" docker prefixes.
	name := strings.TrimPrefix(strings.SplitN(fields[1], ",", 2)[0], "/")

	hc := HostContainer{
		ID:             shortID(fields[0], 12),
		Name:           name,
		Image:          fields[2],
		State:          fields[3],
		Status:         fields[4],
		ComposeProject: fields[7],
		ComposeService: fields[8],
	}
	if strings.HasPrefix(hc.ComposeProject, "jetty_") {
		hc.ManagedByJetty = true
		hc.WorkloadName = strings.TrimPrefix(hc.ComposeProject, "jetty_")
	}

	// Ports: "0.0.0.0:80->80/tcp, 0.0.0.0:443->443/tcp"
	if ports := fields[5]; ports != "" {
		parts := strings.Split(ports, ",")
		for i, p := range parts {
			parts[i] = strings.TrimSpace(p)
		}
		hc.Ports = parts
	}
	return hc, true
}

// hostContainersFormat is the tab-separated docker-ps template we use to
// extract just the fields we want. We avoid `--format json` because it
// joins labels with commas and label values can themselves contain commas
// (CI_DOCKER_VERSION={"Platform":{...}}, etc.), which makes splitting
// unreliable. Tab-separated single-value extraction sidesteps that:
// docker's own template language pulls each label out for us.
//
// Fields, in order:
//
//	ID  Names  Image  State  Status  Ports  CreatedAt
//	compose.project  compose.service
const hostContainersFormat = `{{.ID}}` + "\t" +
	`{{.Names}}` + "\t" +
	`{{.Image}}` + "\t" +
	`{{.State}}` + "\t" +
	`{{.Status}}` + "\t" +
	`{{.Ports}}` + "\t" +
	`{{.CreatedAt}}` + "\t" +
	`{{.Label "com.docker.compose.project"}}` + "\t" +
	`{{.Label "com.docker.compose.service"}}`

// listHostContainers shells out to `docker ps -a` with our tab-separated
// template and returns every container on the host as a parsed list.
//
// Excludes the Jetty agent's own container (we don't need to surface
// ourselves in a list that's meant for showing "what else is running").
func (a *Agent) listHostContainers() ([]HostContainer, error) {
	out, err := exec.Command("docker", "ps", "-a", "--no-trunc", "--format", hostContainersFormat).Output()
	if err != nil {
		return nil, err
	}

	selfID, _ := a.getSelfContainerID() // ignore error; if unknown we just don't filter

	containers := make([]HostContainer, 0)
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		hc, ok := parseHostContainerLine(line)
		if !ok {
			continue
		}
		// Skip self - matched by raw (untruncated) id from the fields.
		rawID := strings.SplitN(line, "\t", 2)[0]
		if selfID != "" && (rawID == selfID || strings.HasPrefix(selfID, rawID) || strings.HasPrefix(rawID, selfID)) {
			continue
		}
		containers = append(containers, hc)
	}

	// Sort: managed-by-jetty first, then by project, then by name. Stable
	// listing order helps the UI render consistently.
	sort.SliceStable(containers, func(i, j int) bool {
		if containers[i].ManagedByJetty != containers[j].ManagedByJetty {
			return containers[i].ManagedByJetty
		}
		if containers[i].ComposeProject != containers[j].ComposeProject {
			return containers[i].ComposeProject < containers[j].ComposeProject
		}
		return containers[i].Name < containers[j].Name
	})

	return containers, nil
}

// apiHostContainers godoc
// @Summary List all Docker containers on this node
// @Description Returns every container on the host, including those not managed by Jetty. The agent's own container is excluded. Pass ?node=<id|name> to query a peer instead - useful when the dashboard is loaded via a shared Cloudflare tunnel and lands on whichever node Cloudflare picks.
// @Tags host
// @Produce json
// @Param node query string false "Peer ID or name; defaults to this node"
// @Success 200 {array} HostContainer
// @Router /host/containers [get]
func (a *Agent) apiHostContainers(w http.ResponseWriter, r *http.Request) {
	if a.proxyHostRequestIfRemote(w, r, "/api/host/containers") {
		return
	}
	containers, err := a.listHostContainers()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list containers: "+err.Error())
		return
	}
	writeJSON(w, containers)
}

// apiHostCompose godoc
// @Summary List Docker compose projects on this node
// @Description Returns containers grouped by compose project. Standalone containers (no compose project) are grouped under an empty project name. Pass ?node=<id|name> to query a peer.
// @Tags host
// @Produce json
// @Param node query string false "Peer ID or name; defaults to this node"
// @Success 200 {array} HostComposeProject
// @Router /host/compose [get]
func (a *Agent) apiHostCompose(w http.ResponseWriter, r *http.Request) {
	if a.proxyHostRequestIfRemote(w, r, "/api/host/compose") {
		return
	}
	containers, err := a.listHostContainers()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list containers: "+err.Error())
		return
	}

	// Group by compose project. Standalone containers (no project label)
	// land in a project named "" which the UI can render as "Unmanaged".
	groups := make(map[string]*HostComposeProject)
	for _, c := range containers {
		key := c.ComposeProject
		g, ok := groups[key]
		if !ok {
			g = &HostComposeProject{
				Name:           key,
				ManagedByJetty: c.ManagedByJetty,
				WorkloadName:   c.WorkloadName,
			}
			groups[key] = g
		}
		g.Containers = append(g.Containers, c)
	}

	// Stable order: jetty-managed first, then alphabetical by project name.
	out := make([]HostComposeProject, 0, len(groups))
	for _, g := range groups {
		out = append(out, *g)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ManagedByJetty != out[j].ManagedByJetty {
			return out[i].ManagedByJetty
		}
		return out[i].Name < out[j].Name
	})

	writeJSON(w, out)
}

// proxyHostRequestIfRemote handles ?node=<id|name> on host endpoints.
// Returns true when the request was handled by proxying to a peer (so
// the caller should stop); returns false for local requests (caller
// continues with normal local processing). The dashboard hits a single
// shared Cloudflare tunnel that lands on whichever node Cloudflare
// picks - without per-node addressing, every refresh shows a different
// node's containers. ?node=self is a no-op alias for the local node.
func (a *Agent) proxyHostRequestIfRemote(w http.ResponseWriter, r *http.Request, path string) bool {
	nodeID := r.URL.Query().Get("node")
	if nodeID == "" || nodeID == "self" || nodeID == a.hwid || nodeID == a.hostname {
		return false
	}

	a.stateMu.RLock()
	var target *Peer
	for _, p := range a.state.Peers {
		if p.ID == nodeID || p.Name == nodeID {
			target = p
			break
		}
	}
	a.stateMu.RUnlock()
	if target == nil {
		writeError(w, http.StatusNotFound, "node not found: "+nodeID)
		return true
	}

	url := a.getPeerAPIURL(target, path)
	req, err := a.peerRequest("GET", url, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "build proxy request: "+err.Error())
		return true
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, "reach node: "+err.Error())
		return true
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	return true
}
