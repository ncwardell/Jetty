<p align="center">
  <img src="https://img.shields.io/badge/orchestration-peer--to--peer-blue?style=for-the-badge" alt="P2P"/>
  <img src="https://img.shields.io/badge/kubernetes-at%20home-orange?style=for-the-badge" alt="K8s at Home"/>
  <img src="https://img.shields.io/badge/powered%20by-cloudflare-F38020?style=for-the-badge&logo=cloudflare&logoColor=white" alt="Cloudflare"/>
  <img src="https://img.shields.io/badge/master%20node-none-success?style=for-the-badge" alt="No Master"/>
  <img src="https://img.shields.io/badge/multi--arch-amd64%20%7C%20arm64-purple?style=for-the-badge" alt="Multi-Arch"/>
</p>

<h1 align="center">
<pre>
     ██╗███████╗████████╗████████╗██╗   ██╗
     ██║██╔════╝╚══██╔══╝╚══██╔══╝╚██╗ ██╔╝
     ██║█████╗     ██║      ██║    ╚████╔╝
██   ██║██╔══╝     ██║      ██║     ╚██╔╝
╚█████╔╝███████╗   ██║      ██║      ██║
 ╚════╝ ╚══════╝   ╚═╝      ╚═╝      ╚═╝
</pre>
</h1>

<h3 align="center">
  <em>🚢 Docker Swarm's unhinged cousin, powered by Cloudflare</em>
</h3>

<p align="center">
  <strong>Peer-to-peer container orchestration for people who looked at Kubernetes and said "nah"</strong>
</p>

<p align="center">
  <a href="#-features">Features</a> •
  <a href="#%EF%B8%8F-architecture">Architecture</a> •
  <a href="#-quick-start">Quick Start</a> •
  <a href="#%EF%B8%8F-multi-architecture-workloads">Multi-Arch</a> •
  <a href="#-api">API</a> •
  <a href="#-failover">Failover</a> •
  <a href="GUIDE.md">Full Guide</a>
</p>

---

## 🤔 What is this?

**Jetty** is what happens when you want container orchestration but think Kubernetes is overkill, Docker Swarm is abandonware, and Nomad requires a PhD. It's a fully decentralized, peer-to-peer Docker Compose orchestrator that uses Cloudflare WARP as its backbone.

**No masters. No etcd. No 47 YAML files. Just vibes and containers.**

Every node is equal. Any node can accept requests. Workloads failover automatically. It's like a boat without a captain, except it actually works.

> *"It's container orchestration but ghetto"* — someone, probably

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| 🌐 **Mesh Network** | Cloudflare WARP creates a private encrypted network. No port forwarding, no VPN setup, no crying. |
| 🔄 **Auto-Failover** | Node dies? Workloads with `revive: true` pop up on healthy nodes like nothing happened. |
| 👑 **No Master** | Every node is equal. Democracy but for containers. |
| 🏷️ **Internal DNS** | Workload names become hostnames. Reference `postgres` instead of memorizing IPs like a caveman. |
| 🎯 **Node Allowlist** | Pin workloads to specific nodes with `allowed_nodes`. Your GPU workload stays on the GPU node. |
| 🔵 **Zero-Downtime Moves** | Blue-green deployment when moving workloads. Old one keeps running until new one is healthy. |
| 🌍 **Cloudflare Tunnel** | Optional external access. One domain, all nodes, Cloudflare handles the load balancing. |
| 🏗️ **Multi-Architecture** | Mix AMD64 and ARM64 nodes. Workloads can have arch-specific compose files. Pi cluster? No problem. |
| 🔐 **Encrypted Secrets** | Store environment variables encrypted with AES-256-GCM. Secrets are synced cluster-wide and injected at deploy time. |
| 📊 **Web Dashboard** | Built-in UI because `curl` gets old. Manage workloads, nodes, and secrets all in one place. |
| 📜 **Swagger Docs** | Full OpenAPI spec. [Live docs here](https://nodes.secretcult.network/swagger/index.html). We're professionals. |
| 🔄 **Node Updates** | Rolling updates with `POST /api/nodes/{id}/update`. Pull new images and restart without losing state. |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CLOUDFLARE WARP MESH                              │
│                        (encrypted overlay network)                          │
└─────────────────────────────────────────────────────────────────────────────┘
         ▲                         ▲                         ▲
         │                         │                         │
         ▼                         ▼                         ▼
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│   🖥️ Node 1     │◄─────►│   🖥️ Node 2     │◄─────►│   🍓 Node 3     │
│   (amd64)       │       │   (amd64)       │       │   (arm64)       │
│                 │       │                 │       │                 │
│ Mesh: 10.100.0.1│       │ Mesh: 10.100.0.2│       │ Mesh: 10.100.0.3│
│ WARP: 100.96.x.x│       │ WARP: 100.96.x.x│       │ WARP: 100.96.x.x│
│                 │       │                 │       │                 │
│ ┌─────────────┐ │       │ ┌─────────────┐ │       │ ┌─────────────┐ │
│ │   nginx     │ │       │ │    app      │ │       │ │ nfs-server  │ │
│ │ 10.100.0.101│ │       │ │ 10.100.0.102│ │       │ │ 10.100.0.50 │ │
│ └─────────────┘ │       │ └─────────────┘ │       │ └─────────────┘ │
└─────────────────┘       └─────────────────┘       └─────────────────┘
         │                         │                         │
         └─────────────────────────┼─────────────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │     CLOUDFLARE TUNNEL       │
                    │   (optional external API)   │
                    │   cluster.yourdomain.com    │
                    └─────────────────────────────┘
```

**How it works:**
1. Each node runs a Jetty agent and connects to Cloudflare WARP
2. Nodes discover each other and gossip state every 10 seconds
3. When you deploy a workload, it gets a mesh IP (e.g., `10.100.0.50`)
4. That IP is accessible from any node in the cluster
5. If a node dies, surviving nodes detect it and revive orphaned workloads
6. No coordinator. No consensus protocol. Just deterministic elections based on hardware ID.

---

## 🚀 Quick Start

### Prerequisites

You'll need:
1. **Cloudflare account** (free tier works)
2. **WARP Connector Token** — Create in Zero Trust Dashboard → Networks → Tunnels → Create Tunnel (WARP Connector)
3. **Tunnel Token** (optional) — For external API access

### Cloudflare WARP Setup

Before deploying, configure your WARP Connector in the Zero Trust Dashboard:

1. Go to **Networks** → **Tunnels** → Select your WARP Connector
2. Under **Traffic routing**, set the mode to **"Include IPs and domains"**
3. Add the WARP CIDR: `100.96.0.0/12` (the full Cloudflare Mesh client range - node IPs are assigned from anywhere in it)

```
┌─────────────────────────────────────────────────────┐
│  Traffic Routing                                    │
│  ─────────────────                                  │
│  ● Include IPs and domains  ← SELECT THIS           │
│  ○ Exclude IPs and domains                          │
│                                                     │
│  Included IPs:                                      │
│  ┌─────────────────────────────────────────────┐    │
│  │ 100.96.0.0/12                               │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

This ensures WARP only routes traffic for the mesh network (`100.96.x.x` IPs) and doesn't mess with your regular internet traffic. Without this, your nodes will be trying to route everything through WARP like absolute maniacs.

### ⚠️ Important: Host Networking Required

> **Jetty MUST run with `--net host` and `--privileged`.**
>
> This isn't optional. Jetty needs to:
> - Create network interfaces (`jetty0`)
> - Set up IPIP tunnels between nodes
> - Manipulate iptables/nftables rules
> - Run WARP and bind to mesh IPs
>
> If you try to run it in bridge networking, it will not work. Don't even try. We've all been there.

### Bootstrap First Node

`JETTY_SECRET` is the **admin / dashboard key** — set it on the first node, it gets baked into the cluster. Joining nodes don't need it.

```bash
docker run -d \
  --name jetty \
  --privileged \
  --net host \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /lib/modules:/lib/modules:ro \
  -v jetty-data:/data \
  -e JETTY_SECRET=your-super-secret-admin-key \
  -e JETTY_WARP_CONNECTOR_TOKEN=your-warp-connector-token \
  -e JETTY_CF_TOKEN=your-cloudflare-tunnel-token \
  ghcr.io/ncwardell/jetty:latest
```

### Generate a Join Token

Every additional node joins with a **single-use token** minted by an authenticated admin. Tokens are time-bounded and burned on first use.

```bash
# Mint a token (default TTL: 1 hour, max: 7 days)
curl -X POST https://your-cluster.example.com/api/tokens \
  -H "X-API-Key: your-super-secret-admin-key" \
  -H "Content-Type: application/json" \
  -d '{"ttl_seconds": 3600, "note": "for arnold's laptop"}'

# Response:
# { "token": "8h2y...64-bytes...kf9", "expires_at": "...", "note": "for arnold's laptop" }
```

You can also do this from the dashboard: **Join Tokens → Generate Token**. The full token is shown once and only once — copy it immediately.

### Join More Nodes

```bash
docker run -d \
  --name jetty \
  --privileged \
  --net host \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /lib/modules:/lib/modules:ro \
  -v jetty-data:/data \
  -e JETTY_JOIN=https://your-tunnel-domain.com \
  -e JETTY_JOIN_TOKEN=8h2y...the-token-from-above...kf9 \
  -e JETTY_WARP_CONNECTOR_TOKEN=...this-machine-s-mesh-token... \
  ghcr.io/ncwardell/jetty:latest
```

Joining nodes get the tunnel config, admin key, and per-node API key
automatically. The join token is consumed and can never be reused.

> ### ⚠️ `JETTY_WARP_CONNECTOR_TOKEN` is per machine
>
> Create one **per node** in Cloudflare: Zero Trust → Networking → Mesh → Add a
> node. It is the one value that cannot be handed out at join time, because
> it identifies *this machine* to Cloudflare.
>
> **Omit it and the node still joins and still looks healthy** — it inherits
> the cluster-shared token, and Cloudflare Mesh registers shared-token nodes as
> active-passive replicas of a single identity. The passive ones **drop all
> traffic** while gossiping normally and showing green in the dashboard.
>
> The agent logs a warning at startup when it falls back. If a node joins
> cleanly but nothing can reach its workloads, check this first.

### Verify

```bash
# Check cluster status
curl http://localhost:6880/api/status | jq

# Or hit the dashboard
open http://localhost:6880
```

---

## 📦 Deploy a Workload

```bash
curl -X POST http://localhost:6880/api/workloads \
  -H "Content-Type: application/json" \
  -d '{
    "name": "whoami",
    "revive": true,
    "autostart": true,
    "compose": "services:\n  whoami:\n    image: traefik/whoami\n    ports:\n      - \"80:80\""
  }'
```

Jetty will:
1. Assign a mesh IP (e.g., `10.100.0.100`)
2. Create a DNS entry (`whoami` resolves to `10.100.0.100`)
3. Deploy the compose file
4. If the node dies and `revive: true`, another node picks it up

---

## 🏗️ Multi-Architecture Workloads

Got a cluster with both x86 servers and Raspberry Pis? Jetty handles it.

```bash
curl -X POST http://localhost:6880/api/workloads \
  -H "Content-Type: application/json" \
  -d '{
    "name": "myapp",
    "revive": true,
    "compose_amd64": "services:\n  app:\n    image: myapp:amd64",
    "compose_arm64": "services:\n  app:\n    image: myapp:arm64"
  }'
```

**How it works:**
- Each node reports its architecture (`amd64`, `arm64`)
- When deploying, Jetty picks the right compose file for that node
- Failover only considers nodes with compatible architecture
- No `compose` fallback? Workload only runs on matching nodes

**Example scenarios:**

| Workload Config | AMD64 Node | ARM64 Node |
|-----------------|------------|------------|
| Only `compose` | ✅ Uses it | ✅ Uses it |
| Only `compose_arm64` | ❌ Can't run | ✅ Uses it |
| `compose` + `compose_arm64` | ✅ Uses `compose` | ✅ Uses `compose_arm64` |
| `compose_amd64` + `compose_arm64` | ✅ Uses `compose_amd64` | ✅ Uses `compose_arm64` |

> **Pro tip:** Most Docker images are multi-arch these days. You only need arch-specific compose files when using images that aren't, or when you want different configs per architecture.

---

## 📡 API

Full Swagger docs at [`/swagger/index.html`](https://nodes.secretcult.network/swagger/index.html)

> **Driving Jetty from an AI agent (or just want every nuance in one place)?**
> See [`docs/AGENT_GUIDE.md`](docs/AGENT_GUIDE.md) — a self-contained operations
> reference covering auth, the full endpoint surface, private-registry pulls,
> the networking model, failover, and footguns.

### Status & Health
```bash
GET  /api/status           # Full cluster status (nodes + workloads)
GET  /api/health           # Health check (use ?node=local for single node)
```

### Workloads
```bash
GET    /api/workloads                    # List all workloads
POST   /api/workloads                    # Create workload
GET    /api/workloads/{name}             # Get workload details
PATCH  /api/workloads/{name}             # Update workload
DELETE /api/workloads/{name}             # Delete workload
POST   /api/workloads/{name}/start       # Start
POST   /api/workloads/{name}/stop        # Stop
POST   /api/workloads/{name}/move        # Move to another node (blue-green)
GET    /api/workloads/{name}/logs        # Container logs
```

### Tags & bulk operations
```bash
# Workloads can carry tags (lowercase, alphanumeric + dash/underscore/colon).
# Tags appear as colored chips in the dashboard and are the primary pivot
# for bulk operations and export/import.

POST   /api/workloads/bulk    # {tag|names|all, action: start|stop|restart|delete}
GET    /api/workloads/export  # ?tag=X or ?names=a,b - JSON dump
POST   /api/workloads/import  # {mode: skip|replace|fail, reassign_ips, payload}
```

Example: stop everything tagged `media` across the whole cluster (proxied to each owner):
```bash
curl -X POST http://localhost:6880/api/workloads/bulk \
  -H "X-API-Key: $JETTY_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"tag": "media", "action": "stop"}'
```

### Cluster & Nodes
```bash
POST   /api/join              # Join cluster (requires {join_token, id, name, ip, api_key})
GET    /api/nodes             # List nodes
DELETE /api/nodes/{id}        # Remove node
POST   /api/nodes/{id}/update # Update node (pull new image, restart)  [admin only]
```

### Join Tokens (admin only)
```bash
POST   /api/tokens            # Mint a one-time join token  {ttl_seconds?, note?}
GET    /api/tokens            # List pending + recent tokens
DELETE /api/tokens/{id}       # Revoke a token
```

### Environment Variables
```bash
GET    /api/env               # List all env variable keys
POST   /api/env               # Set env variables (batch)
GET    /api/env/{key}         # Get decrypted value
DELETE /api/env/{key}         # Delete env variable
```

### Cloudflare Tunnel
```bash
GET    /api/tunnel            # Get tunnel status
POST   /api/tunnel            # ?scope=cluster sets the token; default re-attaches this node
DELETE /api/tunnel            # Default detaches this node only; ?scope=cluster removes it everywhere

# Both mutating endpoints accept ?node=<id|name> to target a peer.
```

### Backup / Restore (admin only)
```bash
GET    /api/backup            # tar.gz of state.json + compose dir + warp dir
                              # (set X-Backup-Passphrase to wrap with Argon2id+AES-GCM)
POST   /api/restore           # Atomically replace state from a backup tar.gz / .enc
GET    /api/backup/schedule   # Read scheduled-backup config
POST   /api/backup/schedule   # {interval_minutes, retention, passphrase?}
DELETE /api/backup/schedule   # Disable scheduled backups
```

### Key rotation (admin only)
```bash
POST   /api/admin-key/rotate              # Mint a fresh AdminKey, gossip to peers
POST   /api/peers/{id}/rotate-key         # Rotate that peer's SelfAPIKey
                                          # (proxied to the target if not local)
```

### Web Terminals (admin only)
```bash
WS  /api/workloads/{name}/exec  # Interactive shell in a workload container
WS  /api/host/shell             # Host shell (gated by JETTY_HOST_SHELL=true)
```

### Proxy
```bash
ANY    /api/proxy/{ip}/{path} # Proxy request to workload by mesh IP
```

---

## 🔄 Failover

When a node goes dark (no heartbeat for 45 seconds):

```
1. 💀 Node 2 dies

2. 🔍 Gossip loop detects (every 10s health checks)
   Node 1: "Node 2 is dead"
   Node 3: "Node 2 is dead"

3. 📋 Orphaned workloads identified
   - app (revive: true) → needs new home
   - cache (revive: false) → RIP

4. 🗳️ Deterministic election
   - All nodes sort by hardware ID
   - Lowest healthy ID that's in allowed_nodes wins
   - No voting, no coordination, same answer everywhere

5. 🚀 Winner deploys workload
   - Claims the mesh IP
   - Spins up containers
   - Other nodes update their state

6. ✅ Business as usual
```

No split-brain. No consensus. Just math.

---

## 🗂️ Workload Schema

```json
{
  "name": "postgres",
  "ip": "10.100.0.50",
  "compose": "services:\n  db:\n    image: postgres:16\n    ...",
  "compose_amd64": "services:\n  db:\n    image: postgres:16-amd64\n    ...",
  "compose_arm64": "services:\n  db:\n    image: postgres:16-arm64\n    ...",
  "revive": true,
  "autostart": true,
  "allowed_nodes": ["node1", "node2"],
  "registry_auth": { "registry": "ghcr.io", "username": "bot", "token_ref": "GHCR_TOKEN" },
  "owner": {
    "id": "abc123...",
    "name": "node1",
    "ip": "100.96.0.1"
  },
  "version": 1705312200
}
```

| Field | What it do |
|-------|------------|
| `name` | Workload name. Becomes a DNS hostname. |
| `ip` | IP on the mesh network (10.100.x.x). Auto-assigned if omitted. |
| `compose` | Default Docker Compose YAML. Used if no arch-specific file matches. |
| `compose_amd64` | Optional. Compose file for AMD64 nodes. |
| `compose_arm64` | Optional. Compose file for ARM64 nodes. |
| `revive` | `true` = failover to another node if owner dies. |
| `autostart` | `true` = start when Jetty starts. |
| `allowed_nodes` | Only these nodes can run this workload. Empty = any node. |
| `registry_auth` | Optional. Auth for pulling a private image — `{registry, username?, token_ref}`. `token_ref` names an **env-store key** (not the token itself). See below. |
| `owner` | Who's currently running it. Don't set this manually. |
| `version` | Unix timestamp. Higher wins in conflicts. |

> **Multi-Arch Note:** If a workload only has `compose_arm64` (no default `compose`), it can only run on ARM64 nodes. Failover will skip incompatible architectures.

---

## 🔐 Private Registry Images

Pulling from a private registry (private GHCR, Docker Hub, GitLab, etc.)? Store
the token **once** in the encrypted env store, then reference it by name from any
workload. The token never lives in the workload itself.

```bash
# 1. Store the token (encrypted, synced to every node)
curl -X POST http://localhost:6880/api/env \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"env": {"GHCR_TOKEN": "ghp_xxxxxxxxxxxx"}}'

# 2. Reference it from the workload
curl -X POST http://localhost:6880/api/workloads \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{
    "name": "api",
    "revive": true,
    "registry_auth": { "registry": "ghcr.io", "token_ref": "GHCR_TOKEN" },
    "compose": "services:\n  api:\n    image: ghcr.io/myorg/api:latest"
  }'
```

- **One credential covers a whole account/org** — registry auth is per-host, not
  per-repo. A single `read:packages` PAT pulls every private repo in the org.
- **`username` defaults to `x-access-token`** (GitHub's PAT convention), so for
  GHCR you usually only need `registry` + `token_ref`.
- **Multiple accounts?** Store one env key each (`GHCR_FOO`, `GHCR_BAR`) and
  point each workload's `token_ref` at the right one. Per-workload Docker config
  isolation means two workloads can hit `ghcr.io` under different accounts.
- Without `registry_auth`, pulls behave exactly as before (public images).

---

## 🔌 Using Hostnames

Since workload names become DNS entries, you can do this:

```yaml
# nfs-server workload
services:
  nfs:
    image: itsthenetwork/nfs-server-alpine
    privileged: true
    environment:
      SHARED_DIRECTORY: /data
    volumes:
      - /srv/nfs:/data
```

```yaml
# some-app workload - references nfs-server by hostname
services:
  app:
    image: myapp
    volumes:
      - data:/app/data

volumes:
  data:
    driver: local
    driver_opts:
      type: nfs
      o: addr=nfs-server,rw,nfsvers=4    # ← hostname, not IP!
      device: ":/data"
```

---

## 🌍 Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `JETTY_SECRET` | **Admin / dashboard key.** Required on the first (bootstrap) node — gets persisted to state and propagated to joiners. Optional on joiners (they receive it via `/api/join`). | - |
| `JETTY_JOIN` | URL to join an existing cluster. | - |
| `JETTY_JOIN_TOKEN` | **Required when joining.** Single-use token minted by `POST /api/tokens` on an existing node. Burned on first successful join. | - |
| `JETTY_WARP_CONNECTOR_TOKEN` | **Per-node** Cloudflare Mesh node token (create one node per machine: Zero Trust → Networking → Mesh → Add a node). Env always overrides saved state. Joiners without one fall back to the cluster-shared token, but shared tokens make Cloudflare treat nodes as active-passive replicas of ONE identity (passive replicas drop traffic) — always prefer per-node. | - |
| `JETTY_CF_TOKEN` | Cloudflare Tunnel token. Bootstrap node only — joiners get it from the join response. | - |
| `JETTY_HOST_SHELL` | Set to `true` to enable the `/api/host/shell` web terminal endpoint (admin-only, dangerous). For a *real* host shell — one that sees the host's `/home`, `/etc`, processes — also pass `--pid=host` to `docker run`; without it the endpoint works but returns a shell scoped to the container. | `false` |
| `JETTY_IMAGE_PRUNE` | Auto-prune stranded Docker images daily (dangling images, unused images older than the cutoff, old build cache). Self-updates and moving-tag re-pulls otherwise leak the previous image forever. Set to `false` to disable. | `true` |
| `JETTY_IMAGE_PRUNE_UNTIL` | Age cutoff for pruning unused *tagged* images and build cache (Go duration). Images newer than this are kept (protects fresh pre-pulls). | `168h` |
| `JETTY_DATA_DIR` | Where state lives. | `/data` |
| `JETTY_API_PORT` | API port. | `6880` |
| `JETTY_TUNNEL_STACK` | Which TCP implementation terminates the userspace tunnel's *receive* side: `netstack` (gVisor — real TCP, with retransmission and congestion control) or `legacy` (the older hand-rolled proxy, kept only as a rollback). Only affects nodes that fall back to the userspace tunnel; kernel IPIP/GRE paths are unaffected. | `netstack` |
| `JETTY_LOG_LEVEL` | Startup log level: `debug`, `info`, `warn`, or `error`. `debug` also turns on source file:line. An unrecognised value falls back to `info` rather than failing startup. Change it on a **running** node with `POST /api/log-level` — restarting to enable debug destroys the state you wanted to debug. | `info` |
| `JETTY_LOG_FORMAT` | `text` (logfmt — readable in `docker logs`) or `json` (for shipping to a log collector). | `text` |
| `JETTY_SERVICE_CIDR` | Mesh network CIDR for workload IPs. | `10.100.0.0/16` |
| `JETTY_TUNNEL_DOMAIN` | Cloudflare tunnel domain (e.g., `cluster.example.com`). | - |
| `JETTY_TUNNEL_HOST` | This node's specific subdomain. | - |

---

## 📁 State Storage

```
/data/
├── state.json           # The source of truth (peers, workloads, env vars)
├── hwid                 # This node's hardware ID (used for elections)
├── warp/                # WARP connector state (persisted across updates)
└── compose/
    └── {workload}/
        └── docker-compose.yml
```

State syncs via gossip. Every node has a copy. Higher `version` wins conflicts.

The `state.json` file contains:
- **Peers**: All known nodes (id, name, mesh IP, per-peer APIKey)
- **Workloads**: All workload configurations and ownership
- **EnvData**: Encrypted environment variables (AES-256-GCM under `EncryptionKey`)
- **AdminKey**: Operator/dashboard credential, bootstrapped from `JETTY_SECRET`
- **EncryptionKey**: 32-byte AES key for env_data
- **SelfAPIKey**: This node's outbound peer credential
- **JoinTokens**: Pending and recently-burned one-time join tokens

> Permission is `0600`. Treat it like a private key — the file contains every credential the cluster uses.

---

## 🐚 Web Terminals

Two WebSocket endpoints give you an interactive shell from the dashboard, both **admin-only**:

| Endpoint | What it does | Gating |
|---|---|---|
| `WS /api/workloads/{name}/exec` | `docker exec`-style PTY into one of the workload's containers (the agent picks the first container in `docker compose -p jetty_<name>`). | Always available. Admin key only. |
| `WS /api/host/shell` | Interactive shell on the **host**. Two switches both have to be set: `JETTY_HOST_SHELL=true` (env var) unlocks the endpoint, and `--pid=host` (docker run flag) puts the agent in the host's PID namespace so `nsenter -t 1` reaches the host's init. Setting only the env var opens the endpoint but spawns a shell scoped to the container, with a banner explaining the limitation. | Off by default. Both switches are required for a real host shell. Admin key only. |

The dashboard exposes both via a 🖥️ button on workload and node detail pages. Renderer is xterm.js loaded from a CDN.

The Host Shell button works on **any** node card, not just the dashboard's own host. When you click it on a remote node, the local agent dials the peer's `/api/host/shell` (auth via the cluster-wide AdminKey), bridges binary frames, and the terminal subtitle shows `(proxied via <local node>)` so it's obvious the WebSocket is hopping through the dashboard's host. If the target node has `JETTY_HOST_SHELL=false`, you'll get a clean error rather than a silent disconnect.

> **`JETTY_HOST_SHELL=true` is dangerous.** It hands an authenticated admin a root shell on every node where it's enabled. Only turn it on if you're the only operator and you trust the admin-key handling chain (no shared dashboards, no admin key in shell history). The Generate-Token modal in the dashboard has a checkbox that toggles this on the joining node and adds the `--pid=host` flag automatically — leave it off unless you specifically want it.

---

## 🔐 Security Model

Jetty separates **three** different credentials so a compromise of one doesn't blow up the cluster:

| Credential | Where it lives | Purpose | Rotation |
|---|---|---|---|
| **AdminKey** | `state.AdminKey` (every node, persisted) | Dashboard / operator API. Required for destructive actions (node update, backup, restore, terminals, token mint). | Bootstrapped from `JETTY_SECRET` on the first node, propagated to joiners. |
| **JoinToken** | `state.JoinTokens` | Single-use bootstrap credential for adding a new node. Time-bounded, burned on first use. | Mint per join via `POST /api/tokens`. |
| **Peer.APIKey** | each peer's record on every node | Per-node credential for inter-peer calls (sync, peer-announce, heartbeat). Each node has its own `SelfAPIKey`. | Generated locally by each node at first bootstrap. |

The HTTP API enforces this with `apiKeyMiddleware`: requests must present an `X-API-Key` header (or `?api_key=` query) matching the AdminKey, this node's SelfAPIKey, or any registered peer's APIKey. All comparisons are constant-time. Admin-only endpoints (anything destructive) layer an additional admin-only check on top.

### Encrypted environment variables

Sensitive config (API keys, passwords, connection strings) are encrypted at rest with **AES-256-GCM** under a 32-byte cluster `EncryptionKey` (separate from any user credential — generated on bootstrap, propagated to joiners via the join response). Ciphertext lives in `state.json` and gossips across the cluster.

```bash
# Set multiple variables at once (admin or peer key works)
curl -X POST http://localhost:6880/api/env \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-admin-key" \
  -d '{
    "env": {
      "DATABASE_URL": "postgres://user:pass@postgres:5432/db",
      "REDIS_PASSWORD": "supersecret",
      "API_KEY": "sk-12345"
    }
  }'

# Use them in a compose file
# environment:
#   - DATABASE_URL=${DATABASE_URL}
```

```bash
# List, fetch, delete:
curl http://localhost:6880/api/env -H "X-API-Key: your-admin-key"
curl http://localhost:6880/api/env/DATABASE_URL -H "X-API-Key: your-admin-key"
curl -X DELETE http://localhost:6880/api/env/OLD_KEY -H "X-API-Key: your-admin-key"
```

> **Migration note:** Older clusters derived the AES key from `JETTY_SECRET` via Argon2id. On first start under the new code, env_data is read with the legacy key and rewritten under a freshly generated `EncryptionKey` — one-shot, idempotent. After that, `JETTY_SECRET` only matters as the AdminKey.

---

## 🆚 Jetty vs The Others

| | Kubernetes | Docker Swarm | Nomad | **Jetty** |
|---|:---:|:---:|:---:|:---:|
| Master node required | ✅ | ✅ | ✅ | ❌ |
| External etcd/consul | ✅ | ❌ | ✅ | ❌ |
| YAML files to learn | 47+ | 3 | 5 | 1 |
| Setup time | Days | Hours | Hours | **Minutes** |
| PhD required | Probably | No | Maybe | **Definitely not** |
| Production ready | ✅ | ⚠️ | ✅ | 🤷 |
| Encrypted by default | ❌ | ❌ | ❌ | ✅ (WARP) |
| Works on a Raspberry Pi | Pain | Yes | Yes | **Yes** |
| Sparks joy | ❌ | ❌ | ❌ | ✅ |

---

## 🤝 Contributing

Found a bug? Got an idea? PRs welcome. This is a ghetto project and we embrace it.

---

## 📜 License

MIT. Do whatever you want. Just don't blame us when your containers end up in the ocean.

---

<p align="center">
  <sub>Built with questionable decisions and Cloudflare's free tier</sub>
</p>

<p align="center">
  <em>⚓ Anchoring containers since you couldn't figure out Kubernetes ⚓</em>
</p>
