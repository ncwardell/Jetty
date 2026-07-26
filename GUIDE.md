# Jetty - P2P Docker Compose Orchestration

## Overview

Jetty is a decentralized container orchestration system that enables multiple nodes to coordinate Docker Compose workloads without a central master. Every node is equal - any node can accept API requests, deploy workloads, and handle failovers.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              JETTY CLUSTER                                  │
│                                                                             │
│   ┌─────────────┐      Cloudflare WARP        ┌─────────────┐              │
│   │   Node A    │◄─────────────────────────────►│   Node B    │              │
│   │ 10.100.x.x  │   (encrypted mesh via WARP)  │ 10.100.y.y  │              │
│   └──────┬──────┘                              └──────┬──────┘              │
│          │                                            │                     │
│   ┌──────┴──────┐                              ┌──────┴──────┐              │
│   │  Workloads  │                              │  Workloads  │              │
│   │  (Docker)   │                              │  (Docker)   │              │
│   └─────────────┘                              └─────────────┘              │
│                                                                             │
│                    ┌─────────────────────┐                                  │
│                    │   Cloudflare Edge   │                                  │
│                    │  cluster.example.com│                                  │
│                    └──────────┬──────────┘                                  │
│                               │                                             │
│              cloudflared ─────┴───── cloudflared                            │
│              (Node A)                (Node B)                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Concepts

### 1. Mesh Networking (Cloudflare WARP)

Each node gets a unique mesh IP (e.g., `10.100.x.x`) derived from its hardware ID. Nodes communicate via Cloudflare WARP, providing encrypted Layer 3 connectivity without port forwarding.

- **Interface**: `jetty0` - dummy interface for mesh IP binding
- **Default CIDR**: `10.100.0.0/16` (65k+ possible IPs)
- **WARP IPs**: Each node also gets a WARP IP (100.96.x.x range)
- **Peer Discovery**: Automatic via gossip protocol

### 2. Workloads

A workload is a Docker Compose application with a dedicated mesh IP:

```json
{
  "name": "nginx",
  "ip": "10.100.50.1",
  "compose": "services:\n  web:\n    image: nginx",
  "revive": true,
  "autostart": true,
  "allowed_nodes": ["node1", "node2"],
  "owner": {
    "id": "abc123...",
    "name": "node1",
    "ip": "10.100.0.1"
  },
  "version": 1704067200
}
```

- **name**: DNS hostname (accessible as `nginx` from any node)
- **ip**: Unique IP on the mesh network (10.100.x.x, auto-assigned if omitted)
- **compose**: Docker Compose YAML content
- **revive**: If true, auto-failover to another node if owner dies
- **autostart**: If true, auto-start when Jetty starts up (on the owning node)
- **allowed_nodes**: Whitelist of nodes that can run this workload
- **owner**: Full info (id, name, ip) of the node running this workload
- **version**: Unix timestamp for conflict resolution

### 3. Gossip Protocol

Every 10 seconds, each node:
1. **Health checks** all known peers (`GET /api/health`)
2. **Syncs workloads** from healthy peers (`GET /api/sync`)

State propagates eventually - no central database needed.

### 4. Failover

Every 15 seconds, each node checks for orphaned workloads:
1. Is the workload's owner dead (unhealthy for 30+ seconds)?
2. Does the workload have `revive: true`?
3. Am I in the `allowed_nodes` list (if specified)?
4. Am I the lowest HWID among eligible healthy nodes? (deterministic election)

If all yes, claim and deploy the workload.

### 5. Cloudflare Tunnel

Optional external access via Cloudflare Tunnel:
- All nodes connect to the same tunnel
- Cloudflare load balances across nodes
- Single domain for the entire cluster
- No inbound firewall rules needed

---

## Quick Start

### Option 1: Docker (Recommended)

```bash
# Build
docker build -t jetty .

# Run first node (bootstrap)
docker run -d \
  --name jetty \
  --network host \
  --privileged \
  -v jetty-data:/data \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e JETTY_SECRET=my-cluster-password \
  -e JETTY_CF_TOKEN=eyJhIjoiNjA2... \
  jetty
```

> **Note:** `--network host` is required for mesh IP routing. Without it, the Jetty container cannot reach workload containers on other Docker networks via iptables DNAT rules.

### Option 2: Binary

```bash
# Build
go build -o jetty .

# Run (requires root for network configuration)
sudo JETTY_SECRET=my-cluster-password ./jetty
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `JETTY_SECRET` | (none) | **Admin / dashboard key.** Required on the bootstrap node — gets persisted as `state.AdminKey` and propagated to joiners. Optional on joiners (they receive it via `/api/join`). |
| `JETTY_JOIN` | (none) | URL of an existing cluster node (e.g., `https://cluster.example.com`). |
| `JETTY_JOIN_TOKEN` | (none) | **Required when joining.** Single-use token minted by `POST /api/tokens` on an existing node. Burned on first successful join. |
| `JETTY_CF_TOKEN` | (none) | Cloudflare tunnel token. Bootstrap-only — joiners receive it. |
| `JETTY_TUNNEL_DOMAIN` | (none) | Your Cloudflare tunnel domain (e.g., `cluster.example.com`). |
| `JETTY_WARP_CONNECTOR_TOKEN` | (none) | **Per-node** Cloudflare Mesh node token (one per machine — shared tokens collapse nodes into active-passive replicas since the 2026-04 Mesh migration). Env overrides saved state; joiners without one fall back to the cluster-shared token. |
| `JETTY_HOST_SHELL` | `false` | Set to `true` to enable `/api/host/shell`. Admin-only endpoint that gives an interactive root shell on the host. |
| `JETTY_IMAGE_PRUNE` | `true` | Daily auto-prune of stranded Docker images (dangling + unused older than cutoff + old build cache). `false` disables. |
| `JETTY_IMAGE_PRUNE_UNTIL` | `168h` | Age cutoff for pruning unused tagged images/build cache. |
| `JETTY_PUBLIC_IP` | (auto) | Override public IP detection (useful in containers). |
| `JETTY_DATA_DIR` | `/data` | Directory for state and compose files. |
| `JETTY_API_PORT` | `6880` | REST API port. |
| `JETTY_SERVICE_CIDR` | `10.100.0.0/16` | Mesh network CIDR for workload IPs. |

---

## Cluster Operations

### Bootstrap a New Cluster

```bash
# Start first node. JETTY_SECRET seeds the cluster admin key.
JETTY_SECRET=mypassword JETTY_CF_TOKEN=eyJ... ./jetty
```

The first node generates its own `SelfAPIKey` and the cluster `EncryptionKey`. `JETTY_SECRET` is copied into `state.AdminKey` once and persisted; subsequent restarts ignore the env var (the persisted state wins).

### Join an Existing Cluster

```bash
# 1. On any existing node, mint a one-time join token.
curl -X POST https://cluster.example.com/api/tokens \
  -H "X-API-Key: mypassword" \
  -H "Content-Type: application/json" \
  -d '{"ttl_seconds": 3600, "note": "node3"}'
# {"token":"8h2y...","expires_at":"...","note":"node3"}

# 2. Bring up the new node with that token.
JETTY_JOIN=https://cluster.example.com \
JETTY_JOIN_TOKEN=8h2y...the-token... \
./jetty
```

The joiner generates its own `SelfAPIKey`, sends it in the join request, and receives:
- The cluster `AdminKey` (so its dashboard works without `JETTY_SECRET` set)
- The cluster `EncryptionKey` (so it can decrypt env_data)
- The full peer list with each peer's `APIKey` (so it can call any peer)
- WARP and tunnel tokens (so it can bring up Cloudflare connectivity)

The token is burned on first successful join. Replays return 401.

### Check Cluster Status

```bash
curl http://localhost:6880/api/status
```

```json
{
  "node": {
    "id": "abc123...",
    "name": "node1",
    "ip": "10.100.42.1",
    "warp_ip": "100.96.0.5"
  },
  "peers": [
    {"id": "def456...", "name": "node2", "ip": "10.100.87.3", "healthy": true}
  ],
  "workloads": [...],
  "tunnel": {
    "configured": true,
    "running": true
  },
  "warp": {
    "enabled": true,
    "ip": "100.96.0.5"
  }
}
```

### Aggregate Cluster Health

```bash
# Get health from all nodes (cluster-wide)
curl http://localhost:6880/api/health

# Get health from just this node
curl http://localhost:6880/api/health?node=local

# Filter by specific node name
curl http://localhost:6880/api/health?node=node1
```

```json
{
  "cluster_status": "healthy",
  "total_nodes": 3,
  "healthy_nodes": 3,
  "total_workloads": 10,
  "timestamp": "2024-01-02T15:04:05Z",
  "nodes": [
    {
      "id": "abc123...",
      "name": "node1",
      "ip": "100.96.0.1",
      "healthy": true,
      "status": "healthy",
      "workloads": ["nginx", "redis"]
    }
  ]
}
```

---

## Workload Management

### Deploy a Workload

```bash
curl -X POST http://localhost:6880/api/workloads \
  -H "Content-Type: application/json" \
  -d '{
    "name": "nginx",
    "ip": "10.100.50.1",
    "revive": true,
    "autostart": true,
    "allowed_nodes": ["node1", "node2"],
    "compose": "services:\n  web:\n    image: nginx:alpine\n    ports:\n      - \"80:80\""
  }'
```

If `ip` is omitted, one will be automatically assigned from the mesh CIDR.

If `allowed_nodes` is specified and the current node is not in the list, the request will be proxied to an allowed node.

### List Workloads

```bash
# All workloads
curl http://localhost:6880/api/workloads

# Filter by node
curl http://localhost:6880/api/workloads?node=node1
```

### Get Workload Details

```bash
curl http://localhost:6880/api/workloads/nginx
```

Returns container runtime info if the workload is local:
```json
{
  "name": "nginx",
  "ip": "10.100.50.1",
  "owner": {
    "id": "abc123...",
    "name": "node1",
    "ip": "10.100.0.1"
  },
  "is_local": true,
  "containers": [
    {
      "id": "abc123...",
      "name": "jetty_nginx-web-1",
      "image": "nginx:alpine",
      "status": "running",
      "running": true,
      "health": "healthy",
      "cpu_percent": "0.50%",
      "memory_usage": "15MiB / 1.94GiB"
    }
  ]
}
```

### Update Workload

```bash
# Update metadata only (no redeploy)
curl -X PATCH http://localhost:6880/api/workloads/nginx \
  -H "Content-Type: application/json" \
  -d '{
    "revive": false,
    "allowed_nodes": ["node1", "node2", "node3"]
  }'

# Update compose or ip (triggers redeploy)
curl -X PATCH http://localhost:6880/api/workloads/nginx \
  -H "Content-Type: application/json" \
  -d '{
    "compose": "services:\n  web:\n    image: nginx:latest",
    "ip": "10.100.50.2"
  }'
```

### View Workload Logs

```bash
curl http://localhost:6880/api/workloads/nginx/logs
```

### Start/Stop Workload

```bash
# Start
curl -X POST http://localhost:6880/api/workloads/nginx/start

# Stop
curl -X POST http://localhost:6880/api/workloads/nginx/stop
```

### Move Workload to Another Node

Uses zero-downtime blue-green deployment:
1. Deploy on target node (both serve traffic)
2. Remove from source node

```bash
curl -X POST http://localhost:6880/api/workloads/nginx/move \
  -H "Content-Type: application/json" \
  -d '{"to": "node2"}'
```

### Delete Workload

```bash
curl -X DELETE http://localhost:6880/api/workloads/nginx
```

---

## Node Allowlist

Restrict which nodes can run a workload using `allowed_nodes`:

```bash
curl -X POST http://localhost:6880/api/workloads \
  -d '{
    "name": "database",
    "allowed_nodes": ["node1", "node2"],
    "compose": "..."
  }'
```

**Behavior:**
- If `allowed_nodes` is empty or null, any node can run the workload
- During deployment, if current node is not allowed, request is proxied to an allowed node
- During failover, only allowed nodes participate in election
- Nodes can be specified by name or HWID

---

## Cloudflare Tunnel Setup

### 1. Create Tunnel in Cloudflare Dashboard

1. Go to Cloudflare Zero Trust → Networks → Tunnels
2. Create a new tunnel
3. Copy the tunnel token

### 2. Configure First Node

```bash
JETTY_CF_TOKEN=eyJhIjoiNjA2... JETTY_SECRET=pass ./jetty
```

### 3. Or Set via API

```bash
curl -X POST http://localhost:6880/api/tunnel \
  -H "Content-Type: application/json" \
  -d '{"token": "eyJhIjoiNjA2..."}'
```

The token automatically propagates to all nodes. Each node connects to the same tunnel, providing redundancy.

### 4. Configure Tunnel Route

In Cloudflare Dashboard, set the tunnel to route to:
- **Service**: `http://localhost:6880`
- **Domain**: `cluster.example.com`

Now `cluster.example.com` hits any healthy node in your cluster.

---

## Cloudflare WARP

WARP provides Layer 3 networking through Cloudflare's network, enabling true IP-level connectivity between nodes without any port forwarding.

### Setting Up WARP Connector

1. Go to Cloudflare Zero Trust → Settings → WARP Client
2. Enable "WARP Connector"
3. Create a new connector and copy the token
4. Configure your Private Network routes (the mesh CIDR, e.g., 10.100.0.0/16)

### Running Jetty with WARP

```bash
docker run -d --name jetty \
  --network host \
  --privileged \
  --device /dev/net/tun \
  -v jetty-data:/data \
  -v /var/lib/cloudflare-warp:/var/lib/cloudflare-warp \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e JETTY_SECRET=my-cluster-password \
  -e JETTY_WARP_CONNECTOR_TOKEN=your-connector-token \
  jetty
```

### Status Check

```bash
curl http://localhost:6880/api/status | jq '.warp'
```

```json
{
  "enabled": true,
  "ip": "100.96.0.5"
}
```

---

## API Reference

Every endpoint requires `X-API-Key` (or `?api_key=`) matching one of: AdminKey, this node's SelfAPIKey, or any registered peer's APIKey. The exceptions are listed under "Public" below. Endpoints marked **admin-only** require the AdminKey specifically.

### Public

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/health` | GET | Health probe. Returns rich payload to authenticated callers, minimal `{status, version}` otherwise. |
| `POST /api/join` | POST | Join cluster (validates `JoinToken` in body). |
| `GET /swagger/...` | GET | API docs UI. |

### Cluster

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/status` | GET | Full cluster status (peers, workloads, tunnel) |
| `GET /api/sync` | GET | Full state dump for peer pull |
| `POST /api/peer-announce` | POST | Peer announcement |
| `POST /api/heartbeat` | POST | Tunnel-mode liveness ping |
| `POST /api/tunnel/sync` | POST | Cloudflare tunnel token broadcast |

### Join Tokens (admin-only)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `POST /api/tokens` | POST | Mint a one-time join token. Body: `{"ttl_seconds": 3600, "note": "..."}` (both optional). |
| `GET /api/tokens` | GET | List pending and recently-burned tokens. Pending token IDs are redacted. |
| `DELETE /api/tokens/{id}` | DELETE | Revoke a token (works on burned tokens too — expunges the audit record). |

### Workloads

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/workloads` | GET | List all workloads |
| `POST /api/workloads` | POST | Deploy new workload |
| `GET /api/workloads/{name}` | GET | Get workload details |
| `PATCH /api/workloads/{name}` | PATCH | Update workload (incl. tags) |
| `DELETE /api/workloads/{name}` | DELETE | Remove workload |
| `POST /api/workloads/{name}/move` | POST | Move to another node |
| `POST /api/workloads/{name}/start` | POST | Start workload |
| `POST /api/workloads/{name}/stop` | POST | Stop workload |
| `POST /api/workloads/{name}/restart` | POST | Restart workload |
| `GET /api/workloads/{name}/logs` | GET | Get container logs |
| `POST /api/workloads/bulk` | POST | Bulk action by tag, names, or all. Body: `{tag\|names\|all, action: start\|stop\|restart\|delete}`. |
| `GET /api/workloads/export` | GET | Export selected workloads as JSON (`?tag=X` or `?names=a,b`). Returns `{version, workloads, referenced_env_keys}`. |
| `POST /api/workloads/import` | POST | Restore from a previous export. Body: `{mode: skip\|replace\|fail, reassign_ips, payload}`. Returns a per-workload report. |

**Tags.** Each workload optionally carries a `tags []string`. Tag strings are validated against `^[a-z0-9][a-z0-9_:-]{0,62}$` (lowercase + dash/underscore/colon) so the `env:prod`-style namespacing convention works without us needing a key=value labels system. Tags are normalized (lowercased, deduped, sorted) on every ingest path so equality comparisons are trivial. The dashboard derives a stable color per tag via `hash(tag) → HSL hue`.

**Bulk actions.** Exactly one selector — `tag`, `names`, or `all` — must be set; mixing returns 400. Each workload's action runs in parallel (bounded to 8 concurrent), proxying to the owner node when the workload is remote. Failures don't stop the run; you get a per-workload report:

```json
{ "selected": ["nginx", "redis"], "results": { "nginx": { "ok": true }, "redis": { "ok": false, "error": "owner unreachable" } } }
```

**Import collision handling.** Mesh IPs auto-reassign on collision by default (workloads are reached by hostname through the mesh DNS, so a new IP is invisible to other workloads). Name collisions follow the chosen `mode`:

- `skip` (default): leave the existing workload alone, return `status: "skipped"` for that entry.
- `replace`: delete-then-recreate (same tombstone semantics as `DELETE /api/workloads/{name}`), return `status: "replaced"`.
- `fail`: validate the entire payload up front; abort the import on any conflict, return 409, no state mutations.

Imports also tell you which `${VAR_NAME}` references the compose YAML carries (`referenced_env_keys`) so the operator knows which secrets to set on the destination cluster before importing.

### Nodes

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/nodes` | GET | List nodes |
| `DELETE /api/nodes/{id}` | DELETE | Remove node from cluster |
| `POST /api/nodes/{id}/update` | POST | **Admin-only.** Pull a new image and restart the agent on the target node. |

### Environment Variables

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/env` | GET | List env keys (values not shown) |
| `POST /api/env` | POST | Set/update env vars (batch) |
| `GET /api/env/{key}` | GET | Get decrypted value |
| `DELETE /api/env/{key}` | DELETE | Delete env var |

### Tunnel

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/tunnel` | GET | Tunnel status |
| `POST /api/tunnel` | POST | Set tunnel token |
| `DELETE /api/tunnel` | DELETE | Remove tunnel |

### Backup / Restore (admin-only)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/backup` | GET | Download a tar.gz of `state.json` + compose dir + warp dir. Set `X-Backup-Passphrase` header to wrap the response with Argon2id + AES-GCM (`JETTY-ENC-V1` format) — safe to store offsite. |
| `POST /api/restore` | POST | Atomically replace state from a backup tar.gz. If the body starts with `JETTY-ENC-V1`, the same `X-Backup-Passphrase` is required. Path-traversal protected. |
| `GET /api/backup/schedule` | GET | Read the cluster's scheduled-backup config (passphrase is redacted to `<set>` if present). |
| `POST /api/backup/schedule` | POST | Configure scheduled backups: `{interval_minutes (>=5), retention (0=unlimited), passphrase?}`. Backups land at `$dataDir/backups/jetty-backup-<timestamp>.tar.gz` (or `.enc` if a passphrase is set). Only the lowest-HWID healthy node runs each interval (same election rule as failover). |
| `DELETE /api/backup/schedule` | DELETE | Disable scheduled backups. Existing backup files on disk are left in place. |

### Key rotation (admin-only)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `POST /api/admin-key/rotate` | POST | Generate (or accept via `{new_key}` body) a fresh AdminKey. Persists locally, broadcasts to peers via memberlist so the whole cluster flips together. The new key is shown once in the response — save it from there. |
| `POST /api/peers/{id}/rotate-key` | POST | Rotate the named peer's `SelfAPIKey`. Use `id=self` for this node, or any HWID/hostname for another peer (the request is proxied). The target regenerates `SelfAPIKey`, persists, and pushes via memberlist `NodeMeta.APIKey`; other peers adopt it via `NotifyUpdate`. Old key is invalid as gossip propagates. |

### Web Terminals (admin-only)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `WS /api/workloads/{name}/exec` | WS | Interactive shell into a workload container via `docker exec`. |
| `WS /api/host/shell` | WS | Interactive shell on the host. Gated by `JETTY_HOST_SHELL=true`. |

### Host Introspection

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/host/containers` | GET | All Docker containers on this node (managed and external). |
| `GET /api/host/compose` | GET | Compose projects detected on the host. |

### Proxy

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/proxy/{ip}/{path}` | ANY | Proxy request to workload by mesh IP. Auth headers are stripped before forwarding to a local workload. |

---

## Architecture Deep Dive

### Startup Sequence

```
main.go
   │
   └── agent.New()
          │
          ├── Read environment variables
          ├── Load or generate HWID
          └── Initialize state
                 │
                 └── agent.Start()
                        │
                        ├── initNetworking()     → Create jetty0 interface
                        ├── loadState()          → Load from state.json
                        ├── joinCluster()        → If JETTY_JOIN set
                        ├── updateHosts()        → Update /etc/hosts
                        ├── startCloudflared()   → If CF token configured
                        ├── startWARP()          → If WARP token configured
                        │
                        └── Goroutines:
                            ├── runAPI()         → HTTP server on :6880
                            ├── gossipLoop()     → Every 10s
                            └── failoverLoop()   → Every 15s
```

### Join Flow

```
       Admin                  New Node                       Existing Node
         │                       │                                 │
         │ POST /api/tokens      │                                 │
         │ X-API-Key: AdminKey   │                                 │
         ├──────────────────────────────────────────────────────►│
         │                       │                                 │
         │      {token, expires_at, note}                          │
         │◄──────────────────────────────────────────────────────┤
         │                                                         │
         │ (operator pastes the token into the new node's env)    │
         │                                                         │
                                 │ POST /api/join                  │
                                 │ {join_token, id, name, ip,      │
                                 │  api_key (joiner-generated)}    │
                                 ├────────────────────────────────►│
                                 │                                 │
                                 │             1. Burn token (persist immediately)
                                 │             2. Check mesh IP collision
                                 │             3. Register peer with APIKey
                                 │                                 │
                                 │  {peers (with api_keys),        │
                                 │   workloads, cf_token,          │
                                 │   warp_token, service_cidr,     │
                                 │   admin_key, encryption_key,    │
                                 │   env_data (encrypted)}         │
                                 │◄────────────────────────────────┤
                                 │                                 │
                                 │             4. Memberlist gossip propagates
                                 │                joiner's NodeMeta (incl. APIKey)
                               5. Persist state
                               6. Start cloudflared / WARP
                                 │                                 │
                                 ▼                                 ▼
                               JOINED                         CLUSTER UPDATED
```

### Failover Flow

```
         Node A (owner)              Node B                Node C
              │                         │                      │
              │    Workload W1          │                      │
              │    revive: true         │                      │
              │    allowed: [A, B]      │                      │
              │                         │                      │
              ╳ (dies)                  │                      │
                                        │                      │
                         ┌──────────────┴──────────────┐
                         │      Every 15 seconds:      │
                         │  1. W1 owner (A) healthy?   │
                         │     → No (30s+ since seen)  │
                         │  2. W1.revive == true?      │
                         │     → Yes                   │
                         │  3. Am I in allowed_nodes?  │
                         │     → B: Yes, C: No         │
                         │  4. Am I lowest HWID?       │
                         │     → B is only eligible    │
                         └──────────────┬──────────────┘
                                        │
                    B claims W1 ────────┤
                                        ▼
                              B claims W1:
                              - Set owner = B
                              - deployWorkload(W1)
                              - Update state
```

### Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         STATE                                   │
│                                                                 │
│  Peers: map[HWID]*Peer                                          │
│    └── {ID, Name, IP, TunnelHost, WarpIP, Healthy, ...}         │
│                                                                 │
│  Workloads: map[IP]*Workload                                    │
│    └── {Name, IP, Compose, Revive, AllowedNodes, Owner, ...}    │
│                                                                 │
│  CFToken: string                                                │
│                                                                 │
└───────────────────────────┬─────────────────────────────────────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
              ▼             ▼             ▼
         Persisted      Gossiped      Routing
         state.json     to peers      configured
```

---

## Network Architecture

### Mesh IP Routing

When a workload is deployed:

```
1. Workload gets mesh IP (e.g., 10.100.50.1)

2. IP added to jetty0 interface:
   ip addr add 10.100.50.1/32 dev jetty0

3. Container gets Docker IP (e.g., 172.17.0.5)

4. DNAT rules route mesh IP to container:
   iptables -t nat -A PREROUTING -d 10.100.50.1 -j DNAT --to 172.17.0.5
   iptables -t nat -A OUTPUT -d 10.100.50.1 -j DNAT --to 172.17.0.5

5. Any node can now reach the workload via 10.100.50.1
```

### DNS Resolution

Jetty manages `/etc/hosts` on each node:

```
# JETTY START - managed by jetty, do not edit
10.100.42.1    node1
10.100.87.3    node2
10.100.50.1    nginx
10.100.50.2    redis
# JETTY END
```

Workloads can reference each other by name:
```yaml
services:
  app:
    image: myapp
    environment:
      - REDIS_HOST=redis  # Resolves to 10.100.50.2
```

---

## Web Terminals

Two WebSocket endpoints provide interactive shells from the dashboard. Both are admin-only and use a small framing protocol over the WebSocket: each frame is a single byte tag (`0x00` = data, `0x01` = resize) followed by a payload. The renderer is xterm.js loaded from a CDN.

### Workload container shell

`WS /api/workloads/{name}/exec`

Runs `docker exec -it` into the first container belonging to the named workload (filtered by `label=com.docker.compose.project=jetty_<name>`). Lets you inspect logs, run migrations, debug crashes, etc. Always available — no env-var opt-in needed.

```javascript
// dashboard pseudocode
const ws = new WebSocket(`wss://cluster.example.com/api/workloads/whoami/exec?api_key=${adminKey}`);
ws.onmessage = (e) => terminal.write(decodeFrame(e.data));
```

### Host shell

`WS /api/host/shell` — gated by `JETTY_HOST_SHELL=true` on the node.

Two independent switches both have to be set for a "real" host shell:

| Switch | What it does |
| --- | --- |
| `JETTY_HOST_SHELL=true` (env var) | Unlocks the endpoint. Without it, the handler returns `403 host shell disabled` and you can't open a shell at all. |
| `--pid=host` (docker run flag) | Puts the agent container in the host's PID namespace, so PID 1 inside the container is the host's init. Without it the endpoint still works, but `nsenter` is a no-op and you get a shell scoped to the agent container. |

Setting only `JETTY_HOST_SHELL=true` is a common pitfall — the endpoint opens, but the banner below explains why you're looking at the container's filesystem.

Spawns an interactive shell. Two modes depending on how the docker run was set up:

**With `--pid=host` (recommended).** PID 1 inside the container's `/proc` is the host's init, in a different mount namespace than the agent. `apiHostShell` notices this and runs:

```
nsenter -t 1 -m -u -i -n -p -- /bin/bash
```

The shell drops into the host's mount, UTS, IPC, network, and PID namespaces — you see real `/home/<user>`, `/etc`, host processes, host hostname. This is effectively the same access you'd get over SSH.

**Without `--pid=host`.** PID 1 inside the container is the agent's own entrypoint; `nsenter` would be a no-op. The handler falls back to a plain `exec.Command(shell)` inside the container's filesystem, and writes a banner to the WebSocket before the shell starts so it's clear what you're looking at:

```
NOTE: this shell is running INSIDE the jetty container, not on the host.
      Files under /home/, /root/ on the host are not visible here.
      To get a real host shell, restart the agent with --pid=host on
      the docker run. The container will then nsenter into PID 1's
      namespaces automatically.
```

Detection is a `readlink /proc/self/ns/mnt` vs `readlink /proc/1/ns/mnt` comparison — guaranteed-different inodes if and only if we're in different mount namespaces.

The Generate-Token modal has a checkbox that injects `JETTY_HOST_SHELL=true` into the joining node's `docker run` and adds `--pid=host`. Leave it off unless you specifically want host-shell access on that node — it can be enabled later by restarting the container with both flags set.

**Cross-node proxy.** The dashboard always serves WebSockets from whichever agent rendered it, so a "Host Shell" button on a remote node card has nowhere to connect on its own. `apiHostShell` accepts an optional `?node=<peer-id>`: when set and not equal to the local hwid, the local agent dials the peer's `/api/host/shell` (authenticating with the cluster-wide `AdminKey`) and bridges binary frames in both directions until either side closes. If the peer rejects the dial — typically `JETTY_HOST_SHELL=false` on the target — the proxy forwards the HTTP status to the browser *before* upgrading the local WS, so the user sees a clean error rather than a silent disconnect. The dashboard's "Host Shell" button passes the target node's id, so it works against any peer in the cluster.

**Why this is dangerous.** Anyone who recovers the AdminKey gets root on every node where `JETTY_HOST_SHELL=true`. The container already runs `--privileged`, so escalation paths exist anyway, but the host shell makes it a one-click drive for any admin-credential leak (dashboard left open on a coworker's laptop, admin key in shell history, etc.).

When the endpoint is reached but `JETTY_HOST_SHELL` is unset:
```
HTTP/1.1 403 Forbidden
host shell disabled - set JETTY_HOST_SHELL=true on the agent to enable
```

### Auth on the WebSocket upgrade

Both endpoints check `X-API-Key` (header) or `?api_key=` (query) against `state.AdminKey` with a constant-time compare *before* the WebSocket upgrade. Peer keys are explicitly rejected — a compromised peer cannot open shells on other nodes.

The shell is whitelisted to a small set (`/bin/bash`, `/bin/sh`, `/bin/ash`); arbitrary `?shell=` query parameters are validated against that list. Workload exec passes the container ID via argv (no shell expansion).

---

## Security

### Authentication model

Three distinct credentials, none reused for another purpose:

1. **AdminKey** (`state.AdminKey`) — operator/dashboard credential. Bootstrapped from `JETTY_SECRET` on the first node, persisted, propagated to joiners via the `/api/join` response. Required for: `apiUpdateNode`, `apiBackup`, `apiRestore`, `apiCreateToken`/`apiListTokens`/`apiDeleteToken`, the web terminal endpoints (`/api/workloads/*/exec`, `/api/host/shell`).
2. **JoinToken** (`state.JoinTokens`) — single-use bootstrap credential for adding a node. Time-bounded (default 1 hour, max 7 days), burned on first successful `apiJoin` and persisted to disk before the handler returns (a crash mid-flow can't leave a token replayable). Minted only by AdminKey holders.
3. **Peer.APIKey** — each peer's own credential. Generated by the peer at first bootstrap (`SelfAPIKey`) and registered with the cluster during `apiJoin`. Other peers learn it via the join response and via `NodeMeta.APIKey` in memberlist gossip. **Never** mutable via `apiPeerAnnounce` (that endpoint strips the field).

`apiKeyMiddleware` accepts an `X-API-Key` matching any of: AdminKey, this node's SelfAPIKey, or any registered `Peer.APIKey`. All comparisons use `subtle.ConstantTimeCompare`. The peer-key match iterates `state.Peers` linearly under a read lock (O(#peers); fine for the cluster sizes Jetty targets).

### Encryption

- **Env data**: AES-256-GCM under a 32-byte cluster `EncryptionKey`. Generated on bootstrap, propagated to joiners via `/api/join`. Replaces an older Argon2id-derived key — legacy ciphertext is migrated in place on first start of the new code.
- **WARP**: All inter-node traffic is encrypted by Cloudflare's WARP layer.
- **Cloudflare tunnel**: External API access goes over HTTPS at the Cloudflare edge. Joining a cluster over plaintext `http://` (to anything other than loopback) is refused — the join token would otherwise travel in cleartext.

### Public endpoints

`/api/health`, `/api/join`, `/swagger/`, and `/` (dashboard) are reachable without an API key. `/api/health` returns only `{status, version}` to anonymous callers; the rich payload (peer list, workload IPs, system metrics) is gated behind authentication.

### What's stripped, validated, or refused

- `Peer.APIKey` is `json:"-"` — never serialized through endpoints that return `Peer` objects (`/api/status`, `/api/nodes`). It's emitted explicitly only in the `/api/join` response (which is point-to-point with the joiner) and in memberlist `NodeMeta`.
- `apiPeerAnnounce` strips `APIKey` from the request body and refuses to repoint an existing peer's IP unless `r.RemoteAddr` matches the new IP (defense against peer impersonation by anyone holding a peer key).
- Workload names: `^[a-zA-Z0-9_-]+$` (prevents path traversal in compose-dir writes).
- Peer hostnames: `^[a-zA-Z0-9._-]+$` (prevents `/etc/hosts` line injection).
- Mesh IPs: `net.ParseIP` validated; collision-checked against existing peers and workloads.
- TAR restore: each entry is checked for `..`-traversal and absolute paths; symlinks/devices are skipped.
- `apiWorkloadProxy` strips `X-API-Key`, `Authorization`, and `Cookie` headers when forwarding to a local workload (workload code is untrusted relative to the cluster control plane).

### Threat model

Peer-API-keys are flat-equivalent in the middleware: any peer key admits every non-admin endpoint. A compromised peer can read state, drop in workload changes, and route packets, but **cannot** mint join tokens, update images, download backups, or open shells. A compromised AdminKey is full cluster ownership.

---

## Troubleshooting

### Node Can't Join

```bash
# Check if target is reachable
curl http://target-node:6880/api/health

# Verify secret matches
echo $JETTY_SECRET

# Check for mesh IP collision (will return 409)
```

### Workload Not Accessible

```bash
# Check if workload is running
curl http://localhost:6880/api/workloads/myapp

# Check Docker containers
docker ps | grep jetty_myapp

# Check iptables rules
iptables -t nat -L -n | grep 10.100.50.1
```

### Peers Showing Unhealthy

```bash
# Check WARP status
curl http://localhost:6880/api/status | jq '.warp'

# Check cluster health
curl http://localhost:6880/api/health
```

### Cloudflare Tunnel Not Working

```bash
# Check tunnel status
curl http://localhost:6880/api/tunnel

# Check cloudflared process
ps aux | grep cloudflared

# View cloudflared logs
docker logs jetty 2>&1 | grep cloudflared
```

---

## Files and Directories

```
/data/
├── state.json       # Cluster state (peers, workloads)
├── hwid             # Hardware ID (persistent node identity)
└── compose/
    ├── nginx/
    │   └── docker-compose.yml
    └── redis/
        └── docker-compose.yml
```

---

## Limitations

- **No central storage**: State is eventually consistent via gossip
- **No resource scheduling**: No CPU/memory constraints
- **No rolling updates**: Workloads are replaced, not updated in-place
- **Compose files are plaintext on disk** (under `/data/compose`). Use the encrypted env-var system (`/api/env`) for secrets and reference them from compose files via `${VAR_NAME}`.
- **Single failure domain**: If all nodes die, state is lost. Use `/api/backup` periodically.

---

## Example: Full Cluster Setup

### 1. Start Bootstrap Node

```bash
docker run -d --name jetty-node1 \
  --network host \
  --privileged \
  -v jetty1:/data \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e JETTY_SECRET=supersecret \
  -e JETTY_CF_TOKEN=eyJhIjoiNjA2... \
  jetty
```

### 2. Mint a Join Token on Node 1

```bash
TOKEN=$(curl -s -X POST http://node1-ip:6880/api/tokens \
  -H "X-API-Key: supersecret" \
  -H "Content-Type: application/json" \
  -d '{"ttl_seconds":3600,"note":"node2"}' \
  | jq -r .token)
echo "Token: $TOKEN"
```

### 3. Start Second Node with the Token

```bash
docker run -d --name jetty-node2 \
  --network host \
  --privileged \
  -v jetty2:/data \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e JETTY_JOIN=http://node1-ip:6880 \
  -e JETTY_JOIN_TOKEN=$TOKEN \
  jetty
```

The token is consumed on first successful join. If you want a third node, mint another one. `JETTY_SECRET` doesn't need to be set on joiners — they receive the admin key automatically and the dashboard works on the joining node with the same operator password.

### 3. Deploy a Workload

```bash
curl -X POST http://localhost:6880/api/workloads \
  -H "Content-Type: application/json" \
  -d '{
    "name": "whoami",
    "revive": true,
    "autostart": true,
    "compose": "services:\n  web:\n    image: traefik/whoami\n    ports:\n      - \"80:80\""
  }'
```

### 4. Access from Any Node

```bash
# From node1
curl http://whoami

# From node2
curl http://whoami

# From internet (via Cloudflare)
curl https://cluster.example.com
```
