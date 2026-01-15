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
  "mesh_ip": "10.100.50.1",
  "compose": "services:\n  web:\n    image: nginx",
  "revive": true,
  "autostart": true,
  "allowed_nodes": ["node1", "node2"],
  "owner": {
    "id": "abc123...",
    "name": "node1",
    "mesh_ip": "10.100.0.1"
  },
  "version": 1704067200
}
```

- **name**: DNS hostname (accessible as `nginx` from any node)
- **mesh_ip**: Unique IP on the mesh network (auto-assigned if omitted)
- **compose**: Docker Compose YAML content
- **revive**: If true, auto-failover to another node if owner dies
- **autostart**: If true, auto-start when Jetty starts up (on the owning node)
- **allowed_nodes**: Whitelist of nodes that can run this workload
- **owner**: Full info (id, name, mesh_ip) of the node running this workload
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
| `JETTY_SECRET` | (none) | **Required.** Shared password all nodes must have. |
| `JETTY_CF_TOKEN` | (none) | Cloudflare tunnel token. |
| `JETTY_TUNNEL_DOMAIN` | (none) | Your Cloudflare tunnel domain (e.g., `cluster.example.com`). |
| `JETTY_WARP_CONNECTOR_TOKEN` | (none) | WARP Connector token for Zero Trust networking. |
| `JETTY_PUBLIC_IP` | (auto) | Override public IP detection (useful in containers). |
| `JETTY_DATA_DIR` | `/data` | Directory for state and compose files. |
| `JETTY_API_PORT` | `6880` | REST API port. |
| `JETTY_MESH_CIDR` | `10.100.0.0/16` | Mesh network IP range. |
| `JETTY_JOIN` | (none) | URL of existing node to join (e.g., `http://node1:6880`). |

---

## Cluster Operations

### Bootstrap a New Cluster

```bash
# Start first node
JETTY_SECRET=mypassword JETTY_CF_TOKEN=eyJ... ./jetty
```

### Join an Existing Cluster

```bash
# On new node - just needs the secret and join URL
JETTY_SECRET=mypassword \
JETTY_JOIN=http://first-node:6880 \
./jetty
```

### Check Cluster Status

```bash
curl http://localhost:6880/api/status
```

```json
{
  "node": {
    "id": "abc123...",
    "name": "node1",
    "mesh_ip": "10.100.42.1",
    "warp_ip": "100.96.0.5"
  },
  "peers": [
    {"id": "def456...", "name": "node2", "mesh_ip": "10.100.87.3", "healthy": true}
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
# Get health from all nodes
curl http://localhost:6880/api/cluster/health

# Filter by specific node
curl http://localhost:6880/api/cluster/health?node=node1
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
      "mesh_ip": "10.100.0.1",
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
    "mesh_ip": "10.100.50.1",
    "revive": true,
    "autostart": true,
    "allowed_nodes": ["node1", "node2"],
    "compose": "services:\n  web:\n    image: nginx:alpine\n    ports:\n      - \"80:80\""
  }'
```

If `mesh_ip` is omitted, one will be automatically assigned.

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
  "mesh_ip": "10.100.50.1",
  "owner": {
    "id": "abc123...",
    "name": "node1",
    "mesh_ip": "10.100.0.1"
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

# Update compose or mesh_ip (triggers redeploy)
curl -X PATCH http://localhost:6880/api/workloads/nginx \
  -H "Content-Type: application/json" \
  -d '{
    "compose": "services:\n  web:\n    image: nginx:latest",
    "mesh_ip": "10.100.50.2"
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

### Cluster

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/status` | GET | Full cluster status |
| `GET /api/health` | GET | Health check (for peers) |
| `GET /api/cluster/health` | GET | Aggregate health from all nodes |
| `POST /api/join` | POST | Join cluster with secret |
| `GET /api/sync` | GET | Get local workloads (for gossip) |

### Workloads

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/workloads` | GET | List all workloads |
| `POST /api/workloads` | POST | Deploy new workload |
| `GET /api/workloads/{name}` | GET | Get workload details |
| `PATCH /api/workloads/{name}` | PATCH | Update workload |
| `DELETE /api/workloads/{name}` | DELETE | Remove workload |
| `POST /api/workloads/{name}/move` | POST | Move to another node |
| `POST /api/workloads/{name}/start` | POST | Start workload |
| `POST /api/workloads/{name}/stop` | POST | Stop workload |
| `GET /api/workloads/{name}/logs` | GET | Get container logs |

### Tunnel

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/tunnel` | GET | Tunnel status |
| `POST /api/tunnel` | POST | Set tunnel token |
| `DELETE /api/tunnel` | DELETE | Remove tunnel |

### Proxy

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/proxy/{mesh_ip}/{path}` | ANY | Proxy request to workload |

### Internal (Peer-to-Peer)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `POST /api/peer-announce` | POST | Announce new peer |
| `POST /api/tunnel/sync` | POST | Sync tunnel token |

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
New Node                                    Existing Node
   │                                             │
   │  POST /api/join                             │
   │  {secret, id, name, mesh_ip, warp_ip}       │
   ├────────────────────────────────────────────►│
   │                                             │
   │                            1. Validate secret
   │                            2. Check mesh IP collision
   │                            3. Add peer to state
   │                                             │
   │  {peers, workloads, cf_token, mesh_cidr}    │
   │◄────────────────────────────────────────────┤
   │                                             │
   │                            4. Announce peer to others
   │                                             │
 5. Store state                                  │
 6. Start cloudflared/WARP                       │
   │                                             │
   ▼                                             ▼
 JOINED                                    CLUSTER UPDATED
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
│    └── {ID, Name, MeshIP, TunnelHost, WarpIP, Healthy, ...}     │
│                                                                 │
│  Workloads: map[MeshIP]*Workload                                │
│    └── {Name, MeshIP, Compose, Revive, AllowedNodes, Owner, ...}│
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

## Security

### Authentication

**Cluster Secret** (`JETTY_SECRET`)
- Permanent shared password
- Required for all join and peer-announce requests
- Set via environment variable
- All nodes must use the same secret

### Encryption

- **WARP**: All inter-node traffic encrypted via Cloudflare
- **API**: HTTP only (use Cloudflare tunnel for HTTPS)

### Input Validation

- Workload names: `^[a-zA-Z0-9_-]+$` (prevents path traversal)
- Mesh IPs: Must be valid IP addresses within mesh CIDR
- Collision detection: Prevents duplicate mesh IPs

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

# Check peer health
curl http://localhost:6880/api/cluster/health
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
- **No secrets management**: Compose files stored in plaintext
- **Single failure domain**: If all nodes die, state is lost

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

### 2. Start Second Node

```bash
docker run -d --name jetty-node2 \
  --network host \
  --privileged \
  -v jetty2:/data \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e JETTY_SECRET=supersecret \
  -e JETTY_JOIN=http://node1-ip:6880 \
  jetty
```

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
