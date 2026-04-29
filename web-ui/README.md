# Jetty Web UI

A static web interface for orchestrating Docker/Docker Compose workloads across your Jetty cluster.

## Quick Start

1. Open `index.html` in your browser
2. Click "Connect" and enter your Jetty cluster URL and **admin key**
3. Start managing your cluster

The admin key is what you set as `JETTY_SECRET` on the first (bootstrap) node — it's persisted into `state.AdminKey` and propagated to every other node, so the same key works against every cluster member.

## Features

### Cluster Overview
- View total nodes and their health status
- See all deployed workloads and their status
- Monitor tunnel and WARP connectivity

### Node Management
- View all nodes in the cluster
- See node details (Mesh IP, WARP IP, Tunnel host)
- View workloads running on each node
- Trigger in-place node updates (admin-only)

### Workload Management
- **Deploy**: Create new workloads with Docker Compose YAML
- **Start/Stop/Restart**: Control workload lifecycle
- **Delete**: Remove workloads and their containers
- **Logs**: View container logs
- **Move**: Migrate workloads between nodes (zero-downtime blue/green)
- **Terminal**: Open an interactive shell into any workload container (admin-only)

### Encrypted Secrets
- Set / list / fetch / delete cluster-wide environment variables
- Variables are encrypted with AES-256-GCM and synced across all nodes
- Reference them in compose files via `${VAR_NAME}`

### Join Tokens
- **Generate Token**: mint a one-time, time-bounded join token. The full token is shown exactly once in a copy-to-clipboard modal alongside an example `docker run` for the joining node.
- **List**: see pending and recently-burned tokens (pending IDs are redacted; burned ones show full so you can audit who consumed what).
- **Revoke**: invalidate a token before it's used.

### Orchestration (Drag & Drop)
- Drag workloads between nodes to migrate them
- Visual representation of workload distribution
- Real-time status updates

## Connection

The UI requires:
- **API URL**: Your Jetty cluster URL (Cloudflare tunnel URL or direct IP)
  - Example: `https://cluster.example.com` or `http://10.100.0.1:6880`
- **Admin Key**: The cluster admin key (originally set as `JETTY_SECRET` on the bootstrap node)

Connection settings are stored in localStorage for convenience.

## API Endpoints Used

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/status` | GET | Get full cluster status |
| `/api/health` | GET | Cluster health (rich payload when authenticated) |
| `/api/workloads` | GET / POST | List or deploy workloads |
| `/api/workloads/{name}` | GET / PATCH / DELETE | Get / update / delete workload |
| `/api/workloads/{name}/start`,`/stop`,`/restart` | POST | Lifecycle control |
| `/api/workloads/{name}/move` | POST | Move to another node |
| `/api/workloads/{name}/logs` | GET | Container logs |
| `/api/workloads/{name}/exec` | WS | Web terminal into a container |
| `/api/host/shell` | WS | Host shell (when `JETTY_HOST_SHELL=true`) |
| `/api/host/containers` | GET | All Docker containers on the node |
| `/api/nodes` | GET | List nodes |
| `/api/nodes/{id}/update` | POST | In-place agent update |
| `/api/env` | GET / POST | List / set encrypted env vars |
| `/api/env/{key}` | GET / DELETE | Fetch / delete a specific env var |
| `/api/tunnel` | GET / POST / DELETE | Cloudflare tunnel config |
| `/api/tokens` | POST / GET | Mint / list join tokens |
| `/api/tokens/{id}` | DELETE | Revoke a token |
| `/api/backup` | GET | Download a state backup tar.gz |
| `/api/restore` | POST | Restore from a backup tar.gz |

## Browser Compatibility

Works in all modern browsers (Chrome, Firefox, Safari, Edge).

Note: Due to CORS, you may need to configure your browser or Jetty to allow cross-origin requests if accessing from a different domain.

## Development

This is a single-file static HTML application with:
- Vanilla JavaScript (no frameworks)
- CSS custom properties for theming
- Modern ES6+ features

To modify, simply edit `index.html`. The build copies it into `agent/dashboard.html` (embedded in the Go binary).
