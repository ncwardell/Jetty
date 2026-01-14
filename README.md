<p align="center">
  <img src="https://img.shields.io/badge/orchestration-peer--to--peer-blue?style=for-the-badge" alt="P2P"/>
  <img src="https://img.shields.io/badge/kubernetes-at%20home-orange?style=for-the-badge" alt="K8s at Home"/>
  <img src="https://img.shields.io/badge/powered%20by-cloudflare-F38020?style=for-the-badge&logo=cloudflare&logoColor=white" alt="Cloudflare"/>
  <img src="https://img.shields.io/badge/master%20node-none-success?style=for-the-badge" alt="No Master"/>
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
  <a href="#-api">API</a> •
  <a href="#-failover">Failover</a> •
  <a href="GUIDE.md">Full Guide</a>
</p>

---

## 🤔 What is this?

**Jetty** is what happens when you want container orchestration but think Kubernetes is overkill, Docker Swarm is abandonware, and Nomad requires a PhD. It's a fully decentralized, peer-to-peer Docker Compose orchestrator that uses Cloudflare WARP as its backbone.

**No masters. No etcd. No 47 YAML files. Just vibes and containers.**

Every node is equal. Any node can accept requests. Workloads failover automatically. It's like a boat without a captain, except it actually works.

> *"It's giving container orchestration but make it ghetto"* — someone, probably

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
| 📊 **Web Dashboard** | Built-in UI because `curl` gets old. |
| 📜 **Swagger Docs** | Full OpenAPI spec at `/swagger/`. We're professionals here. |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CLOUDFLARE WARP MESH                               │
│                        (encrypted overlay network)                           │
└─────────────────────────────────────────────────────────────────────────────┘
         ▲                         ▲                         ▲
         │                         │                         │
         ▼                         ▼                         ▼
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│   🖥️ Node 1     │◄─────►│   🖥️ Node 2     │◄─────►│   🖥️ Node 3     │
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

## 📡 API

Full Swagger docs at `http://your-node:6880/swagger/`

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

### Cluster
```bash
POST   /api/join             # Join cluster
GET    /api/nodes            # List nodes
DELETE /api/nodes/{id}       # Remove node
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
  "compose": "services:\n  db:\n    image: postgres:16\n    volumes:\n      - data:/var/lib/postgresql/data\nvolumes:\n  data:",
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
| `compose` | Your Docker Compose YAML as a string. |
| `revive` | `true` = failover to another node if owner dies. |
| `autostart` | `true` = start when Jetty starts. |
| `allowed_nodes` | Only these nodes can run this workload. Empty = any node. |
| `owner` | Who's currently running it. Don't set this manually. |
| `version` | Unix timestamp. Higher wins in conflicts. |

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
├── state.json           # The source of truth
├── hwid                 # This node's hardware ID (used for elections)
└── compose/
    └── {workload}/
        └── docker-compose.yml
```

State syncs via gossip. Every node has a copy. Higher `version` wins conflicts.

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
