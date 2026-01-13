# Jetty

```
 ╦ ╔═╗ ╔╦╗ ╔╦╗ ╦ ╦
 ║ ║╣   ║   ║  ╚╦╝
╚╝ ╚═╝  ╩   ╩   ╩
```

Peer-to-peer Docker Compose orchestration. Every node is equal.

## Features

- **Mesh Network**: Cloudflare WARP-based private network (10.100.0.0/16)
- **Internal DNS**: Use hostnames instead of IPs in compose files
- **Auto-Failover**: Workloads with `revive: true` restart on healthy nodes
- **No Master**: Any node can be the entry point via Cloudflare tunnel
- **Node Allowlist**: Restrict workloads to specific nodes with `allowed_nodes`
- **Zero-Downtime Moves**: Blue-green deployment when moving workloads

## Architecture

```
Node 1 (node1)            Node 2 (node2)            Node 3 (node3)
┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐
│  Jetty Agent      │    │  Jetty Agent      │    │  Jetty Agent      │
│  10.100.0.1       │◄──►│  10.100.0.2       │◄──►│  10.100.0.3       │
│  WARP: 100.96.x.x │    │  WARP: 100.96.x.x │    │  WARP: 100.96.x.x │
│                   │    │                   │    │                   │
│  Workloads:       │    │  Workloads:       │    │  Workloads:       │
│  └─ nginx         │    │  └─ app           │    │  └─ nfs-server    │
│     10.100.0.101  │    │     10.100.0.102  │    │     10.100.0.50   │
└───────────────────┘    └───────────────────┘    └───────────────────┘
         │                        │                        │
         └────────────────────────┼────────────────────────┘
                                  │
                        Cloudflare WARP Mesh
```

## Quick Start

### Prerequisites

Before starting, you need to set up Cloudflare WARP and Tunnel:

1. **WARP Connector Token**: Create a WARP Connector in your Cloudflare Zero Trust dashboard
2. **Tunnel Token**: Create a Cloudflare Tunnel for external API access

### First Node (Bootstrap)

```bash
docker run -d \
  --name jetty \
  --privileged \
  --net host \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v jetty-data:/data \
  -e JETTY_SECRET=my-cluster-password \
  -e JETTY_WARP_CONNECTOR_TOKEN=your-warp-connector-token \
  -e JETTY_CF_TOKEN=your-cloudflare-tunnel-token \
  jetty:latest
```

### Additional Nodes

```bash
docker run -d \
  --name jetty \
  --privileged \
  --net host \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v jetty-data:/data \
  -e JETTY_SECRET=my-cluster-password \
  -e JETTY_JOIN=https://your-tunnel-domain.com \
  jetty:latest
```

> **Note**: Additional nodes only need the cluster secret and join URL. The WARP connector token and tunnel token are automatically received from the cluster during join and configured at runtime.

## API

### Status
```bash
# Get this node + all peers + all workloads cluster-wide
curl http://node:8080/api/status

# Get aggregate cluster health from all nodes
curl http://node:8080/api/cluster/health

# Filter by specific node
curl http://node:8080/api/cluster/health?node=node1
```

### Workloads
```bash
# List all workloads (cluster-wide)
curl http://node:8080/api/workloads

# Filter by node
curl http://node:8080/api/workloads?node=node1

# Deploy workload (mesh_ip auto-assigned if omitted)
curl -X POST http://node:8080/api/workloads -d '{
  "name": "nfs-server",
  "mesh_ip": "10.100.0.50",
  "revive": true,
  "autostart": true,
  "allowed_nodes": ["node1", "node2"],
  "compose": "services:\n  nfs:\n    image: itsthenetwork/nfs-server-alpine"
}'

# Update workload
curl -X PATCH http://node:8080/api/workloads/nfs-server -d '{
  "revive": false,
  "allowed_nodes": ["node1", "node2", "node3"]
}'

# Delete workload
curl -X DELETE http://node:8080/api/workloads/nfs-server

# Move workload (zero-downtime blue-green deployment)
curl -X POST http://node:8080/api/workloads/nfs-server/move -d '{"to": "node2"}'

# Start/Stop workload
curl -X POST http://node:8080/api/workloads/nfs-server/start
curl -X POST http://node:8080/api/workloads/nfs-server/stop

# View logs
curl http://node:8080/api/workloads/nfs-server/logs
```

### Cluster Management
```bash
# Join (called automatically by JETTY_JOIN)
curl -X POST http://node:8080/api/join -d '{...}'
```

## Workload Schema

```json
{
  "name": "nfs-server",
  "mesh_ip": "10.100.0.50",
  "compose": "services:\n  nfs:\n    image: ...",
  "revive": true,
  "autostart": true,
  "allowed_nodes": ["node1", "node2"],
  "owner": {
    "id": "abc123def456",
    "name": "node1",
    "mesh_ip": "10.100.0.1"
  },
  "version": 1705312200
}
```

| Field | Description |
|-------|-------------|
| `name` | Workload name, becomes DNS hostname |
| `mesh_ip` | Unique IP on mesh network (auto-assigned if omitted) |
| `compose` | Docker Compose YAML |
| `revive` | If true, another node will revive if owner dies |
| `autostart` | If true, auto-start when Jetty starts |
| `allowed_nodes` | Whitelist of nodes that can run this workload |
| `owner` | Node info (id, name, mesh_ip) currently running this |
| `version` | Unix timestamp for conflict resolution |

## Using Hostnames in Compose

Since workload names become DNS entries, you can reference them by name:

```yaml
# This workload is deployed as "nfs-server" with mesh_ip 10.100.0.50
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
# Another workload can reference it by hostname
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
      o: addr=nfs-server,rw,nfsvers=4
      device: ":/data"
```

## Failover

When a node goes down (no response for 45s):

1. All nodes detect via gossip health checks
2. Workloads with `revive: true` become orphaned
3. Deterministic election: **lowest healthy node HWID wins** (respecting `allowed_nodes`)
4. Winner claims the `mesh_ip` and deploys workload
5. Other nodes update their cache

No coordination needed - all nodes reach same conclusion independently.

## State Storage

```
/data/
├── state.json        # All state (peers, workloads)
├── hwid              # This node's hardware ID
└── compose/          # Local compose files
    └── {name}/docker-compose.yml
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `JETTY_SECRET` | Shared cluster password (required for all nodes) | - |
| `JETTY_WARP_CONNECTOR_TOKEN` | WARP Connector token (bootstrap only, synced to joining nodes) | - |
| `JETTY_CF_TOKEN` | Cloudflare tunnel token (bootstrap only, synced to joining nodes) | - |
| `JETTY_JOIN` | URL to join existing cluster | - |
| `JETTY_DATA_DIR` | Data directory | `/data` |
| `JETTY_API_PORT` | API port | `8080` |
| `JETTY_MESH_CIDR` | Mesh network | `10.100.0.0/16` |
