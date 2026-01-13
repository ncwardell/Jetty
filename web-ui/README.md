# Jetty Web UI

A static web interface for orchestrating Docker/Docker Compose workloads across your Jetty cluster.

## Quick Start

1. Open `index.html` in your browser
2. Click "Connect" and enter your Jetty cluster URL and secret key
3. Start managing your cluster

## Features

### Cluster Overview
- View total nodes and their health status
- See all deployed workloads and their status
- Monitor tunnel and WARP connectivity

### Node Management
- View all nodes in the cluster
- See node details (Mesh IP, WARP IP, Tunnel host)
- View workloads running on each node

### Workload Management
- **Deploy**: Create new workloads with Docker Compose YAML
- **Start/Stop**: Control workload lifecycle
- **Delete**: Remove workloads and their containers
- **Logs**: View container logs
- **Move**: Migrate workloads between nodes

### Orchestration (Drag & Drop)
- Drag workloads between nodes to migrate them
- Visual representation of workload distribution
- Real-time status updates

## Connection

The UI requires:
- **API URL**: Your Jetty cluster URL (Cloudflare tunnel URL or direct IP)
  - Example: `https://cluster.example.com` or `http://10.100.0.1:6880`
- **Secret Key**: The `JETTY_SECRET` configured on your cluster

Connection settings are stored in localStorage for convenience.

## API Endpoints Used

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/status` | GET | Get full cluster status |
| `/api/workloads` | GET | List all workloads |
| `/api/workloads` | POST | Deploy new workload |
| `/api/workloads/{name}` | GET | Get workload details |
| `/api/workloads/{name}` | DELETE | Delete workload |
| `/api/workloads/{name}/start` | POST | Start workload |
| `/api/workloads/{name}/stop` | POST | Stop workload |
| `/api/workloads/{name}/move` | POST | Move workload to another node |
| `/api/workloads/{name}/logs` | GET | Get container logs |

## Browser Compatibility

Works in all modern browsers (Chrome, Firefox, Safari, Edge).

Note: Due to CORS, you may need to configure your browser or Jetty to allow cross-origin requests if accessing from a different domain.

## Development

This is a single-file static HTML application with:
- Vanilla JavaScript (no frameworks)
- CSS custom properties for theming
- Modern ES6+ features

To modify, simply edit `index.html`.
