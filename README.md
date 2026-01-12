╦╔═╗╔╦╗╔╦╗╦ ╦
  ║║╣  ║  ║ ╚╦╝
 ╚╝╚═╝ ╩  ╩  ╩

Peer-to-peer Docker Compose orchestration. Every node is equal.

## Features

- **Mesh Network**: WireGuard-based private network (10.100.0.0/16)
- **Internal DNS**: Use hostnames instead of IPs in compose files
- **Auto-Failover**: Workloads with `revive: true` restart on healthy nodes
- **No Master**: Any node can be the entry point via Cloudflare tunnel

## Architecture

```
Node 1 (node1)            Node 2 (node2)            Node 3 (node3)
┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐
│  Jetty Agent      │    │  Jetty Agent      │    │  Jetty Agent      │
│  10.100.0.1       │◄──►│  10.100.0.2       │◄──►│  10.100.0.3       │
│                   │    │                   │    │                   │
│  Workloads:       │    │  Workloads:       │    │  Workloads:       │
│  └─ nginx         │    │  └─ app           │    │  └─ nfs-server    │
│     10.100.0.101  │    │     10.100.0.102  │    │     10.100.0.50   │
└───────────────────┘    └───────────────────┘    └───────────────────┘

/etc/hosts on every node:
10.100.0.1    node1
10.100.0.2    node2
10.100.0.3    node3
10.100.0.101  nginx
10.100.0.102  app
10.100.0.50   nfs-server
```

## Quick Start

### First Node

```bash
docker run -d \
  --name jetty \
  --privileged \
  --net host \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v jetty-data:/data \
  jetty:latest
```

Get join token:
```bash
curl -X POST http://localhost:8080/api/token
```

### Additional Nodes

```bash
docker run -d \
  --name jetty \
  --privileged \
  --net host \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v jetty-data:/data \
  -e JETTY_JOIN=http://<first-node>:8080 \
  -e JETTY_TOKEN=<token> \
  jetty:latest
```

## API

### Status
```bash
# Get this node + all peers + all workloads cluster-wide
curl http://node:8080/api/status
```

### Workloads
```bash
# List all workloads (cluster-wide, cached)
curl http://node:8080/api/workloads

# Deploy workload
curl -X POST http://node:8080/api/workloads -d '{
  "name": "nfs-server",
  "mesh_ip": "10.100.0.50",
  "revive": true,
  "compose": "version: '\''3'\''\nservices:\n  nfs:\n    image: itsthenetwork/nfs-server-alpine"
}'

# Delete workload
curl -X DELETE http://node:8080/api/workloads/nfs-server

# Move workload
curl -X POST http://node:8080/api/workloads/nfs-server/move -d '{"to": "node2"}'
```

### Cluster Management
```bash
# Generate join token
curl -X POST http://node:8080/api/token

# Join (called automatically by JETTY_JOIN)
curl -X POST http://node:8080/api/join -d '{...}'
```

## Workload Schema

```json
{
  "name": "nfs-server",
  "mesh_ip": "10.100.0.50",
  "compose": "version: '3'\nservices:\n  ...",
  "revive": true,
  "owner": "abc123def456",
  "version": 1705312200
}
```

| Field | Description |
|-------|-------------|
| `name` | Workload name, becomes DNS hostname |
| `mesh_ip` | Unique IP on mesh network (the "lock") |
| `compose` | Docker Compose YAML |
| `revive` | If true, another node will revive if owner dies |
| `owner` | Node HWID currently running this |
| `version` | Unix timestamp, for conflict resolution |

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

When a node goes down (no response for 30s):

1. All nodes detect via gossip health checks
2. Workloads with `revive: true` become orphaned
3. Deterministic election: **lowest healthy node HWID wins**
4. Winner claims the `mesh_ip` and deploys workload
5. Other nodes update their cache

No coordination needed - all nodes reach same conclusion independently.

## State Storage

```
/data/
├── state.json        # All state (peers, workloads, tokens)
├── hwid              # This node's hardware ID
├── wg_private_key    # WireGuard private key
└── compose/          # Local compose files
    └── {name}/docker-compose.yml
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `JETTY_DATA_DIR` | Data directory | `/data` |
| `JETTY_API_PORT` | API port | `8080` |
| `JETTY_WG_PORT` | WireGuard port | `51820` |
| `JETTY_MESH_CIDR` | Mesh network | `10.100.0.0/16` |
| `JETTY_JOIN` | URL to join existing cluster | - |
| `JETTY_TOKEN` | Join token | - |
