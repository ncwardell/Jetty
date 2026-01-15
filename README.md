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
3. Add the WARP CIDR: `100.96.0.0/16`

```
┌─────────────────────────────────────────────────────┐
│  Traffic Routing                                    │
│  ─────────────────                                  │
│  ● Include IPs and domains  ← SELECT THIS           │
│  ○ Exclude IPs and domains                          │
│                                                     │
│  Included IPs:                                      │
│  ┌─────────────────────────────────────────────┐    │
│  │ 100.96.0.0/16                               │    │
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

```bash
docker run -d \
  --name jetty \
  --privileged \
  --net host \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v jetty-data:/data \
  -e JETTY_SECRET=your-super-secret-password \
  -e JETTY_WARP_CONNECTOR_TOKEN=your-warp-connector-token \
  -e JETTY_CF_TOKEN=your-cloudflare-tunnel-token \
  ghcr.io/ncwardell/jetty:latest
```

### Join More Nodes

```bash
docker run -d \
  --name jetty \
  --privileged \
  --net host \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v jetty-data:/data \
  -e JETTY_SECRET=your-super-secret-password \
  -e JETTY_JOIN=https://your-tunnel-domain.com \
  ghcr.io/ncwardell/jetty:latest
```

> **That's it.** Joining nodes get the WARP token and tunnel config automatically from the cluster. No manual token copying.

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

### Status & Health
```bash
GET  /api/status           # Full cluster status (nodes + workloads)
GET  /api/health           # Health check
GET  /api/cluster/health   # Aggregate health from all nodes
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

### Cluster & Nodes
```bash
POST   /api/join             # Join cluster
GET    /api/nodes            # List nodes
DELETE /api/nodes/{id}       # Remove node
POST   /api/nodes/{id}/update # Update node (pull new image, restart)
```

### Environment Variables
```bash
GET    /api/env              # List all env variable keys
POST   /api/env              # Set env variables (batch)
GET    /api/env/{key}        # Get decrypted value
DELETE /api/env/{key}        # Delete env variable
```

### Cloudflare Tunnel
```bash
GET    /api/tunnel           # Get tunnel status
POST   /api/tunnel           # Configure tunnel with token
DELETE /api/tunnel           # Remove tunnel
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
  "mesh_ip": "10.100.0.50",
  "compose": "services:\n  db:\n    image: postgres:16\n    ...",
  "compose_amd64": "services:\n  db:\n    image: postgres:16-amd64\n    ...",
  "compose_arm64": "services:\n  db:\n    image: postgres:16-arm64\n    ...",
  "revive": true,
  "autostart": true,
  "allowed_nodes": ["node1", "node2"],
  "owner": {
    "id": "abc123...",
    "name": "node1",
    "mesh_ip": "10.100.0.1"
  },
  "version": 1705312200
}
```

| Field | What it do |
|-------|------------|
| `name` | Workload name. Becomes a DNS hostname. |
| `mesh_ip` | IP on the mesh network. Auto-assigned if you don't care. |
| `compose` | Default Docker Compose YAML. Used if no arch-specific file matches. |
| `compose_amd64` | Optional. Compose file for AMD64 nodes. |
| `compose_arm64` | Optional. Compose file for ARM64 nodes. |
| `revive` | `true` = failover to another node if owner dies. |
| `autostart` | `true` = start when Jetty starts. |
| `allowed_nodes` | Only these nodes can run this workload. Empty = any node. |
| `owner` | Who's currently running it. Don't set this manually. |
| `version` | Unix timestamp. Higher wins in conflicts. |

> **Multi-Arch Note:** If a workload only has `compose_arm64` (no default `compose`), it can only run on ARM64 nodes. Failover will skip incompatible architectures.

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
| `JETTY_SECRET` | Cluster password. Required. | - |
| `JETTY_WARP_CONNECTOR_TOKEN` | WARP token. Bootstrap node only. | - |
| `JETTY_CF_TOKEN` | Cloudflare Tunnel token. Bootstrap node only. | - |
| `JETTY_JOIN` | URL to join existing cluster. | - |
| `JETTY_DATA_DIR` | Where state lives. | `/data` |
| `JETTY_API_PORT` | API port. | `6880` |
| `JETTY_MESH_CIDR` | Mesh network range. | `10.100.0.0/16` |

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
- **Peers**: List of all known nodes in the cluster
- **Workloads**: All workload configurations and ownership
- **EnvData**: Encrypted environment variables (AES-256-GCM)

---

## 🔐 Encrypted Environment Variables

Store sensitive configuration (API keys, passwords, connection strings) encrypted at rest and sync them across your cluster.

### Setting Variables

```bash
# Set multiple variables at once
curl -X POST http://localhost:6880/api/env \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret" \
  -d '{
    "env": {
      "DATABASE_URL": "postgres://user:pass@postgres:5432/db",
      "REDIS_PASSWORD": "supersecret",
      "API_KEY": "sk-12345"
    }
  }'
```

### Using Variables in Workloads

Environment variables are automatically injected when deploying workloads:

```yaml
# docker-compose.yml
services:
  app:
    image: myapp:latest
    environment:
      - DATABASE_URL=${DATABASE_URL}
      - REDIS_PASSWORD=${REDIS_PASSWORD}
```

### How It Works

1. Variables are encrypted with **AES-256-GCM** using a key derived from `JETTY_SECRET`
2. Encrypted values are stored in `state.json` and synced to all nodes
3. When a workload deploys, variables are decrypted and injected as environment variables
4. Values are never logged or exposed in plain text (except via explicit `GET /api/env/{key}`)

### Managing Variables

```bash
# List all variable keys (values not shown)
curl http://localhost:6880/api/env -H "X-API-Key: your-secret"

# Get a specific variable's decrypted value
curl http://localhost:6880/api/env/DATABASE_URL -H "X-API-Key: your-secret"

# Delete a variable
curl -X DELETE http://localhost:6880/api/env/OLD_KEY -H "X-API-Key: your-secret"
```

> **Security Note:** All nodes in the cluster must use the same `JETTY_SECRET` to decrypt values. Changing the secret will make existing encrypted values unreadable.

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
