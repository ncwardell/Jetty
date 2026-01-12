# Jetty - P2P Docker Compose Orchestration

## Overview

Jetty is a decentralized container orchestration system that enables multiple nodes to coordinate Docker Compose workloads without a central master. Every node is equal - any node can accept API requests, deploy workloads, and handle failovers.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              JETTY CLUSTER                                  │
│                                                                             │
│   ┌─────────────┐      WireGuard Mesh       ┌─────────────┐                │
│   │   Node A    │◄─────────────────────────►│   Node B    │                │
│   │ 10.100.x.x  │        (encrypted)        │ 10.100.y.y  │                │
│   └──────┬──────┘                           └──────┬──────┘                │
│          │                                         │                        │
│   ┌──────┴──────┐                           ┌──────┴──────┐                │
│   │  Workloads  │                           │  Workloads  │                │
│   │  (Docker)   │                           │  (Docker)   │                │
│   └─────────────┘                           └─────────────┘                │
│                                                                             │
│                    ┌─────────────────────┐                                 │
│                    │   Cloudflare Edge   │                                 │
│                    │  cluster.example.com │                                 │
│                    └──────────┬──────────┘                                 │
│                               │                                            │
│              cloudflared ─────┴───── cloudflared                           │
│              (Node A)                (Node B)                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Concepts

### 1. Mesh Networking (WireGuard)

Each node gets a unique mesh IP (e.g., `10.100.x.x`) derived from its hardware ID. Nodes communicate over encrypted WireGuard tunnels.

- **Interface**: `jetty0` - the WireGuard interface
- **Default CIDR**: `10.100.0.0/16` (65k+ possible IPs)
- **Peer Discovery**: Automatic via gossip protocol

### 2. Workloads

A workload is a Docker Compose application with a dedicated mesh IP:

```json
{
  "name": "nginx",
  "mesh_ip": "10.100.50.1",
  "compose": "version: '3'\nservices:\n  web:\n    image: nginx",
  "revive": true,
  "owner": "abc123...",
  "version": 1704067200
}
```

- **name**: DNS hostname (accessible as `nginx` from any node)
- **mesh_ip**: Unique IP on the mesh network
- **compose**: Docker Compose YAML content
- **revive**: If true, auto-restart on another node if owner dies
- **owner**: HWID of the node running this workload
- **version**: Unix timestamp for conflict resolution

### 3. Gossip Protocol

Every 10 seconds, each node:
1. **Health checks** all known peers (`GET /api/health`)
2. **Syncs workloads** from healthy peers (`GET /api/sync`)
3. **Cleans up** expired tokens

State propagates eventually - no central database needed.

### 4. Failover

Every 15 seconds, each node checks for orphaned workloads:
1. Is the workload's owner dead (unhealthy for 30+ seconds)?
2. Does the workload have `revive: true`?
3. Am I the lowest HWID among healthy nodes? (deterministic election)

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
  --cap-add NET_ADMIN \
  --cap-add NET_RAW \
  -p 8080:8080 \
  -p 51820:51820/udp \
  -v jetty-data:/data \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e JETTY_SECRET=my-cluster-password \
  -e JETTY_CF_TOKEN=eyJhIjoiNjA2... \
  jetty
```

### Option 2: Binary

```bash
# Build
go build -o jetty .

# Run (requires root for WireGuard)
sudo JETTY_SECRET=my-cluster-password ./jetty
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `JETTY_SECRET` | (none) | **Required for secure clusters.** Shared password all nodes must have. |
| `JETTY_CF_TOKEN` | (none) | Cloudflare tunnel token (bootstrap node only). |
| `JETTY_TUNNEL_DOMAIN` | (none) | **Tunnel-only mode.** Your Cloudflare tunnel domain (e.g., `cluster.example.com`). Eliminates need for UDP port forwarding. |
| `JETTY_TUNNEL_HOST` | (none) | **Direct WG routing.** This node's specific subdomain (e.g., `node1.cluster.example.com`) for direct WireGuard packet routing. |
| `JETTY_PUBLIC_IP` | (auto) | Override public IP detection (useful in containers). |
| `JETTY_DATA_DIR` | `/data` | Directory for state and compose files. |
| `JETTY_API_PORT` | `8080` | REST API port. |
| `JETTY_WG_PORT` | `51820` | WireGuard UDP port (not needed in tunnel-only mode). |
| `JETTY_MESH_CIDR` | `10.100.0.0/16` | Mesh network IP range. |
| `JETTY_JOIN` | (none) | URL of existing node to join (e.g., `http://node1:8080` or `https://cluster.example.com`). |
| `JETTY_TOKEN` | (none) | Join token from existing cluster. |

---

## Cluster Operations

### Bootstrap a New Cluster

```bash
# Start first node
JETTY_SECRET=mypassword JETTY_CF_TOKEN=eyJ... ./jetty

# Get a join token
curl -X POST http://localhost:8080/api/token
# Returns: {"token":"abc123...","expires_at":"2024-01-02T..."}
```

### Join an Existing Cluster

```bash
# On new node
JETTY_SECRET=mypassword \
JETTY_JOIN=http://first-node:8080 \
JETTY_TOKEN=abc123... \
./jetty
```

### Check Cluster Status

```bash
curl http://localhost:8080/api/status
```

```json
{
  "node": {
    "id": "abc123...",
    "name": "node1",
    "mesh_ip": "10.100.42.1",
    "public_key": "..."
  },
  "peers": [
    {"id": "def456...", "name": "node2", "mesh_ip": "10.100.87.3", "healthy": true}
  ],
  "workloads": [...],
  "tunnel": {
    "configured": true,
    "running": true
  }
}
```

---

## Workload Management

### Deploy a Workload

```bash
curl -X POST http://localhost:8080/api/workloads \
  -H "Content-Type: application/json" \
  -d '{
    "name": "nginx",
    "mesh_ip": "10.100.50.1",
    "revive": true,
    "compose": "version: '\''3'\''\nservices:\n  web:\n    image: nginx:alpine\n    ports:\n      - \"80:80\""
  }'
```

### List Workloads

```bash
curl http://localhost:8080/api/workloads
```

### Get Workload Details

```bash
curl http://localhost:8080/api/workloads/nginx
```

### View Workload Logs

```bash
curl http://localhost:8080/api/workloads/nginx/logs
```

### Move Workload to Another Node

```bash
curl -X POST http://localhost:8080/api/workloads/nginx/move \
  -H "Content-Type: application/json" \
  -d '{"to": "node2"}'
```

### Delete Workload

```bash
curl -X DELETE http://localhost:8080/api/workloads/nginx
```

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
curl -X POST http://localhost:8080/api/tunnel \
  -H "Content-Type: application/json" \
  -d '{"token": "eyJhIjoiNjA2..."}'
```

The token automatically propagates to all nodes. Each node connects to the same tunnel, providing redundancy.

### 4. Configure Tunnel Route

In Cloudflare Dashboard, set the tunnel to route to:
- **Service**: `http://localhost:8080`
- **Domain**: `cluster.example.com`

Now `cluster.example.com` hits any healthy node in your cluster.

---

## Tunnel-Only Mode (No Port Forwarding)

Tunnel-only mode eliminates the need for UDP port 51820 to be forwarded. All inter-node communication goes through the Cloudflare tunnel.

### When to Use

- **Nodes behind strict NAT/firewall** with no ability to forward UDP ports
- **Cloud environments** where UDP traffic is blocked
- **Simplified networking** - only HTTPS outbound required

### How It Works

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        TUNNEL-ONLY MODE                                      │
│                                                                              │
│   ┌─────────────┐                             ┌─────────────┐               │
│   │   Node A    │                             │   Node B    │               │
│   │ 10.100.x.x  │                             │ 10.100.y.y  │               │
│   └──────┬──────┘                             └──────┬──────┘               │
│          │                                           │                       │
│          │ cloudflared                     cloudflared │                      │
│          │    │                                   │    │                      │
│          │    └─────────────┬─────────────────────┘    │                      │
│          │                  │                          │                      │
│          │         ┌────────┴────────┐                 │                      │
│          │         │   Cloudflare    │                 │                      │
│          │         │     Tunnel      │                 │                      │
│          │         │ cluster.example │                 │                      │
│          │         └─────────────────┘                 │                      │
│          │                                             │                      │
│   ┌──────┴──────┐                             ┌───────┴─────┐               │
│   │  Workloads  │     HTTP Proxy API          │  Workloads  │               │
│   │  (local)    │◄──────────────────────────►│  (local)    │               │
│   └─────────────┘                             └─────────────┘               │
│                                                                              │
│   • Mesh IPs work locally (same node)                                        │
│   • Cross-node traffic goes through /api/proxy/                              │
│   • All gossip/sync via tunnel domain                                        │
│   • Health checks via heartbeat mechanism                                    │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Enable Tunnel-Only Mode

```bash
# On ALL nodes, set the tunnel domain
docker run -d --name jetty \
  --cap-add NET_ADMIN --cap-add NET_RAW \
  -p 8080:8080 \
  -v jetty-data:/data \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e JETTY_SECRET=my-cluster-password \
  -e JETTY_CF_TOKEN=eyJhIjoiNjA2... \
  -e JETTY_TUNNEL_DOMAIN=cluster.example.com \
  jetty
```

Notice: **No UDP port 51820 exposed!**

### Joining in Tunnel-Only Mode

```bash
# Join via the tunnel domain
docker run -d --name jetty \
  --cap-add NET_ADMIN --cap-add NET_RAW \
  -p 8080:8080 \
  -v jetty-data:/data \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e JETTY_SECRET=my-cluster-password \
  -e JETTY_TUNNEL_DOMAIN=cluster.example.com \
  -e JETTY_JOIN=https://cluster.example.com \
  -e JETTY_TOKEN=abc123... \
  jetty
```

### Cross-Node Workload Communication

In tunnel-only mode:
- **Local workloads**: Accessible via mesh IP directly (DNAT routing)
- **Remote workloads**: Accessible via HTTP proxy API

```bash
# Access remote workload through proxy
curl http://localhost:8080/api/proxy/10.100.50.1/path/to/resource

# Or via the tunnel
curl https://cluster.example.com/api/proxy/10.100.50.1/path/to/resource
```

The `/etc/hosts` file shows which workloads are local vs remote:
```
# JETTY START - managed by jetty, do not edit
# Mode: tunnel-only (cluster.example.com)
# Note: Remote workloads accessible via /api/proxy/{mesh_ip}/
10.100.42.1    node1    # this node
10.100.87.3    node2    # peer (healthy)
10.100.50.1    nginx    # workload (local)
10.100.50.2    redis    # workload (remote)
# JETTY END
```

### WebSocket UDP Tunnel (Full Mesh Connectivity)

Tunnel-only mode now includes **WebSocket UDP tunneling** that provides full WireGuard mesh connectivity without port forwarding:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    WEBSOCKET UDP TUNNEL ARCHITECTURE                        │
│                                                                             │
│   Node A                          Cloudflare                        Node B  │
│                                    Tunnel                                   │
│   ┌─────────┐                                                  ┌─────────┐ │
│   │WireGuard│──UDP──►┌──────────┐                ┌──────────┐──►│WireGuard│ │
│   │ jetty0  │        │UDP Relay │──HTTP/WS──────►│/api/wg/  │   │ jetty0  │ │
│   └─────────┘        │:51821    │   POST         │packet    │   └─────────┘ │
│                      └──────────┘                └──────────┘               │
│                                                                             │
│   • WireGuard sends to local relay (127.0.0.1:51821)                        │
│   • Relay encapsulates packet in JSON, sends via HTTP POST                  │
│   • Cloudflare routes to a node                                             │
│   • Receiving node checks target ID                                         │
│   • If match: injects into local WireGuard                                  │
│   • Full mesh IP connectivity - containers can reach remote workloads!      │
└─────────────────────────────────────────────────────────────────────────────┘
```

**How it works:**
1. For each peer, Jetty creates a local UDP relay (127.0.0.1:51821, 51822, etc.)
2. WireGuard is configured to send packets to these local relays
3. Relays capture packets and send via HTTP POST to `/api/wg/packet`
4. The packet includes `{from: nodeID, to: peerID, data: encryptedPayload}`
5. Receiving node injects the packet into its local WireGuard interface
6. WireGuard decrypts and routes - full mesh connectivity achieved!

**Benefits:**
- **Full mesh IP routing** - Containers can directly reach remote mesh IPs
- **No port forwarding** - Only outbound HTTPS required
- **WireGuard encryption** - All traffic remains encrypted end-to-end
- **Transparent** - Applications don't know they're going through a tunnel

### Subdomain-Based Direct Routing

By default, Cloudflare load balances requests randomly across nodes. This works for control plane operations (gossip, sync) but is inefficient for WireGuard packets that need to reach a specific peer.

**Solution:** Each node gets its own subdomain in Cloudflare, allowing direct routing:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     SUBDOMAIN-BASED DIRECT ROUTING                          │
│                                                                             │
│   Node A                          Cloudflare                        Node B  │
│   node1.cluster.example.com                                node2.cluster.   │
│                                                            example.com      │
│   ┌─────────────┐                                        ┌─────────────┐   │
│   │   Agent A   │──WG packet for B ───────────────────►  │   Agent B   │   │
│   │             │   POST node2.cluster.example.com       │  (direct!)  │   │
│   └─────────────┘   /api/wg/packet                       └─────────────┘   │
│                                                                             │
│   • Each node has unique subdomain (JETTY_TUNNEL_HOST)                     │
│   • WG packets sent directly to target peer's subdomain                    │
│   • No random routing - packets go straight to destination                 │
│   • Control plane can still use shared domain for load balancing           │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Cloudflare Setup:**
1. In Cloudflare Tunnel dashboard, add multiple public hostnames
2. Each hostname points to `http://localhost:8080` on that node
3. Example hostnames:
   - `cluster.example.com` → Load balanced to any node (general access)
   - `node1.cluster.example.com` → Routes only to Node A
   - `node2.cluster.example.com` → Routes only to Node B

**Node Configuration:**

```bash
# Node A
docker run -d --name jetty \
  --cap-add NET_ADMIN --cap-add NET_RAW \
  -p 8080:8080 \
  -v jetty-data:/data \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e JETTY_SECRET=my-cluster-password \
  -e JETTY_CF_TOKEN=eyJhIjoiNjA2... \
  -e JETTY_TUNNEL_DOMAIN=cluster.example.com \
  -e JETTY_TUNNEL_HOST=node1.cluster.example.com \
  jetty

# Node B
docker run -d --name jetty \
  --cap-add NET_ADMIN --cap-add NET_RAW \
  -p 8080:8080 \
  -v jetty-data:/data \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e JETTY_SECRET=my-cluster-password \
  -e JETTY_TUNNEL_DOMAIN=cluster.example.com \
  -e JETTY_TUNNEL_HOST=node2.cluster.example.com \
  -e JETTY_JOIN=https://cluster.example.com \
  -e JETTY_TOKEN=abc123... \
  jetty
```

**How it works:**
1. Each node announces its `TunnelHost` during join/announce
2. When sending WG packets, the sender looks up the target peer's `TunnelHost`
3. If available, packets go directly to `https://{peer.TunnelHost}/api/wg/packet`
4. If not available, falls back to shared tunnel domain (random routing)

**Benefits:**
- **No random hops** - Packets reach the correct node immediately
- **Lower latency** - Direct path instead of multi-hop through random nodes
- **Better reliability** - No dependency on gossip for packet routing

### Limitations

1. **Higher latency** - Packets route through Cloudflare
2. **Tunnel dependency** - If Cloudflare is down, cross-node mesh fails
3. **Bandwidth** - HTTP overhead compared to raw UDP

---

## API Reference

### Cluster

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/status` | GET | Full cluster status |
| `GET /api/health` | GET | Health check (for peers) |
| `POST /api/token` | POST | Generate join token (24h expiry, single-use) |
| `POST /api/join` | POST | Join cluster with token |
| `GET /api/sync` | GET | Get local workloads (for gossip) |

### Workloads

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/workloads` | GET | List all workloads |
| `POST /api/workloads` | POST | Deploy new workload |
| `GET /api/workloads/{name}` | GET | Get workload details |
| `DELETE /api/workloads/{name}` | DELETE | Remove workload |
| `POST /api/workloads/{name}/move` | POST | Move to another node |
| `GET /api/workloads/{name}/logs` | GET | Get container logs |

### Tunnel

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/tunnel` | GET | Tunnel status |
| `POST /api/tunnel` | POST | Set tunnel token |
| `DELETE /api/tunnel` | DELETE | Remove tunnel |

### Proxy (Cross-Node Communication)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/proxy/{mesh_ip}/{path}` | ANY | Proxy request to workload (local or remote) |

### WireGuard Tunnel (Tunnel-Only Mode)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/ws/wg` | WebSocket | WebSocket endpoint for WG packet relay |
| `POST /api/wg/packet` | POST | HTTP endpoint for WG packet forwarding |

### Internal (Peer-to-Peer)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `POST /api/peer-announce` | POST | Announce new peer |
| `POST /api/heartbeat` | POST | Health heartbeat (tunnel-only mode) |
| `POST /api/tunnel/sync` | POST | Sync tunnel token |
| `POST /api/token/sync` | POST | Sync join tokens |

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
                        ├── initWireGuard()     → Create jetty0 interface
                        ├── loadState()          → Load from state.json
                        ├── joinCluster()        → If JETTY_JOIN set
                        ├── updateHosts()        → Update /etc/hosts
                        ├── startCloudflared()   → If CF token configured
                        │
                        └── Goroutines:
                            ├── runAPI()         → HTTP server on :8080
                            ├── gossipLoop()     → Every 10s
                            └── failoverLoop()   → Every 15s
```

### Join Flow

```
New Node                                    Existing Node
   │                                             │
   │  POST /api/join                             │
   │  {token, secret, id, mesh_ip, pubkey}       │
   ├────────────────────────────────────────────►│
   │                                             │
   │                            1. Validate secret
   │                            2. Validate & consume token
   │                            3. Check mesh IP collision
   │                            4. Add peer to state
   │                            5. Configure WireGuard
   │                                             │
   │  {peers, workloads, cf_token}               │
   │◄────────────────────────────────────────────┤
   │                                             │
   │                            6. Broadcast token deletion
   │                            7. Announce peer to others
   │                                             │
 8. Store state                                  │
 9. Configure WireGuard peers                    │
10. Start cloudflared                            │
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
              │                         │                      │
              ╳ (dies)                  │                      │
                                        │                      │
                         ┌──────────────┴──────────────┐
                         │      Every 15 seconds:      │
                         │  1. W1 owner (A) healthy?   │
                         │     → No (30s+ since seen)  │
                         │  2. W1.revive == true?      │
                         │     → Yes                   │
                         │  3. Am I lowest HWID?       │
                         │     → Compare B vs C        │
                         └──────────────┬──────────────┘
                                        │
                    B has lower HWID ───┤
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
│    └── {ID, Name, MeshIP, Endpoint, PublicKey, TunnelHost, ...} │
│                                                                 │
│  Workloads: map[MeshIP]*Workload                                │
│    └── {Name, MeshIP, Compose, Revive, Owner, Version}          │
│                                                                 │
│  Tokens: map[string]*Token                                      │
│    └── {Token, ExpiresAt}                                       │
│                                                                 │
│  CFToken: string                                                │
│                                                                 │
└───────────────────────────┬─────────────────────────────────────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
              ▼             ▼             ▼
         Persisted      Gossiped      WireGuard
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

### Authentication Layers

1. **Cluster Secret** (`JETTY_SECRET`)
   - Permanent shared password
   - Required for all join and peer-announce requests
   - Set via environment variable

2. **Join Tokens**
   - Temporary (24h expiry)
   - Single-use (deleted after successful join)
   - Generated via `POST /api/token`
   - Synced cluster-wide for Cloudflare compatibility

### Encryption

- **WireGuard**: All inter-node traffic encrypted
- **API**: HTTP only (use Cloudflare tunnel for HTTPS)

### Input Validation

- Workload names: `^[a-zA-Z0-9_-]+$` (prevents path traversal)
- Mesh IPs: Must be valid IP addresses
- Collision detection: Prevents duplicate mesh IPs

---

## Troubleshooting

### Node Can't Join

```bash
# Check if target is reachable
curl http://target-node:8080/api/health

# Verify secret matches
echo $JETTY_SECRET

# Check token hasn't expired
curl http://target-node:8080/api/status | jq .tokens

# Check for mesh IP collision
# (will return 409 if collision)
```

### Workload Not Accessible

```bash
# Check if workload is running
curl http://localhost:8080/api/workloads/myapp

# Check Docker containers
docker ps | grep jetty_myapp

# Check iptables rules
iptables -t nat -L -n | grep 10.100.50.1

# Check WireGuard peers
wg show jetty0
```

### Peers Showing Unhealthy

```bash
# Check WireGuard connection
wg show jetty0

# Verify UDP port is open
nc -zu peer-ip 51820

# Check peer's endpoint
curl http://localhost:8080/api/status | jq '.peers[] | {name, endpoint, healthy}'
```

### Cloudflare Tunnel Not Working

```bash
# Check tunnel status
curl http://localhost:8080/api/tunnel

# Check cloudflared process
ps aux | grep cloudflared

# View cloudflared logs
docker logs jetty 2>&1 | grep cloudflared
```

---

## Files and Directories

```
/data/
├── state.json       # Cluster state (peers, workloads, tokens)
├── hwid             # Hardware ID (persistent node identity)
├── wg_private_key   # WireGuard private key
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
- **No rolling updates**: Workloads are replaced, not updated
- **No secrets management**: Compose files stored in plaintext
- **Single failure domain**: If all nodes die, state is lost

---

## Example: Full Cluster Setup

### 1. Start Bootstrap Node

```bash
docker run -d --name jetty-node1 \
  --cap-add NET_ADMIN --cap-add NET_RAW \
  -p 8080:8080 -p 51820:51820/udp \
  -v jetty1:/data \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e JETTY_SECRET=supersecret \
  -e JETTY_CF_TOKEN=eyJhIjoiNjA2... \
  -e JETTY_PUBLIC_IP=203.0.113.1 \
  jetty
```

### 2. Get Join Token

```bash
TOKEN=$(curl -s -X POST http://203.0.113.1:8080/api/token | jq -r .token)
echo $TOKEN
```

### 3. Start Second Node

```bash
docker run -d --name jetty-node2 \
  --cap-add NET_ADMIN --cap-add NET_RAW \
  -p 8080:8080 -p 51820:51820/udp \
  -v jetty2:/data \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e JETTY_SECRET=supersecret \
  -e JETTY_JOIN=http://203.0.113.1:8080 \
  -e JETTY_TOKEN=$TOKEN \
  -e JETTY_PUBLIC_IP=203.0.113.2 \
  jetty
```

### 4. Deploy a Workload

```bash
curl -X POST http://203.0.113.1:8080/api/workloads \
  -H "Content-Type: application/json" \
  -d '{
    "name": "whoami",
    "mesh_ip": "10.100.100.1",
    "revive": true,
    "compose": "version: '\''3'\''\nservices:\n  web:\n    image: traefik/whoami\n    ports:\n      - \"80:80\""
  }'
```

### 5. Access from Any Node

```bash
# From node1
curl http://10.100.100.1

# From node2
curl http://whoami

# From internet (via Cloudflare)
curl https://cluster.example.com
```
