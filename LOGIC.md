# Jetty Internal Logic Documentation

This document explains the reasoning behind Jetty's architecture, why certain design decisions were made, and identifies areas that could potentially be simplified.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [State Synchronization](#state-synchronization)
3. [Network Architecture](#network-architecture)
4. [Failover System](#failover-system)
5. [Cloudflare Integration](#cloudflare-integration)
6. [Simplification Opportunities](#simplification-opportunities)

---

## Architecture Overview

### Why No Master Node?

Jetty uses a **fully decentralized peer-to-peer architecture** with no master/controller node.

**Reasoning:**
- **Single point of failure elimination**: A master node going down doesn't break the cluster
- **Simpler deployment**: No need to designate or protect a "special" node
- **Horizontal scaling**: Any node can join/leave without coordination
- **Network partition resilience**: Nodes can operate independently if split

**Trade-off accepted:**
- Eventual consistency instead of strong consistency
- Requires tombstones for deletion propagation (see [Tombstones](#tombstones))

### Why WARP for Networking?

Cloudflare WARP provides the mesh networking backbone.

**Reasoning:**
- **Zero-configuration networking**: Nodes automatically get routable IPs (100.96.x.x)
- **End-to-end encryption**: All inter-node traffic is encrypted by default
- **NAT traversal**: Works across firewalls without port forwarding
- **Global anycast**: Connections route through nearest Cloudflare datacenter

**Why not alternatives:**
- WireGuard: Requires manual key exchange and endpoint configuration
- Tailscale: Similar to WARP but with additional vendor lock-in
- Plain TCP: No encryption, requires open ports, NAT issues

**Important WARP limitation: No ICMP support**

WARP does not forward ICMP traffic (ping), only TCP/UDP. This has significant design implications:

1. **Health checks must use HTTP, not ping** - The gossip loop uses `GET /api/health` instead of ICMP echo requests. This is why peer health checking hits the API endpoint rather than using a simple ping.

2. **ICMP proxy in userspace tunnel** - When the userspace tunnel receives ICMP packets destined for a remote workload, it can't just forward them over WARP. Instead, it must:
   - Receive the ICMP echo request via WebSocket
   - Send its own ICMP request locally to the workload container
   - Package the response and send it back via WebSocket

   This is why `proxyICMP()` exists in agent.go - it's working around WARP's ICMP limitation.

3. **Workload-to-workload ping works** - Because IPIP/GRE tunnels encapsulate the entire IP packet (including ICMP) inside a TCP/UDP packet that WARP *can* forward. The outer packet is TCP/UDP, the inner packet can be anything.

---

## State Synchronization

### The Gossip Protocol

Located in: `agent/agent.go:5140` (`gossipLoop`)

Every 10 seconds, each node:
1. Sends health checks to all known peers
2. Pulls state from healthy peers via `/api/sync`
3. Merges incoming state with local state

**Why gossip instead of consensus (Raft/Paxos)?**

- **Simplicity**: No leader election, no log replication
- **Availability over consistency**: System stays available during partitions
- **Scale**: O(n) messages per round vs O(n²) for consensus
- **Failure tolerance**: Continues working with any single node alive

**Trade-off accepted:**
- Updates may take multiple gossip rounds to propagate
- Conflicting updates resolved by "highest version wins"

### The Merge Algorithm

Located in: `agent/sync.go:24` (`mergeWorkloadState`)

```
For each incoming workload:
  1. Check if there's a tombstone with higher version → skip
  2. Check if local version is higher → skip
  3. Otherwise, accept the incoming workload
```

**Why version-based conflict resolution?**

- **Deterministic**: All nodes reach same state given same inputs
- **Simple**: No vector clocks or causal ordering needed
- **Timestamp-based**: Uses Unix timestamps, naturally ordered

**Limitation:**
- Clock skew between nodes can cause unexpected behavior
- Mitigation: Versions only compared, not used for timing

### Tombstones

Located in: `agent/types.go:100` (`DeletedWorkload`)

When a workload is deleted, a "tombstone" is created with a version higher than the workload's.

**Why tombstones?**

Without tombstones, this happens:
1. Node A deletes workload X
2. Node B still has workload X in its state
3. Node A syncs from Node B
4. Workload X reappears on Node A (ghost resurrection)

With tombstones:
1. Node A creates tombstone for X with version = now()
2. Tombstone propagates to all nodes via gossip
3. Tombstone.Version > Workload.Version, so X stays deleted everywhere

**Why 1-hour tombstone expiry?**

Located in: `agent/agent.go:5168` (`gcTombstones`)

- **Prevents unbounded state growth**: Tombstones are garbage collected after 1 hour
- **Sufficient propagation time**: 1 hour >> gossip interval (10s) × expected cluster size
- **Trade-off**: Nodes offline >1 hour may resurrect deleted workloads on rejoin

---

## Network Architecture

### Three-Tier Tunnel Fallback

Located in: `agent/agent.go:569` (`detectTunnelMode`)

Jetty tries tunneling methods in order of efficiency:

```
1. IPIP (most efficient) → encapsulates IP in IP, kernel-level
2. GRE (fallback) → uses different kernel module
3. Userspace tunnel → WebSocket-based, works anywhere
```

**Why three tiers?**

- **IPIP**: Fastest, lowest overhead, but requires `ipip` kernel module
- **GRE**: Alternative when IPIP unavailable (different module dependencies)
- **Userspace**: Works on restricted environments (ChromeOS, some containers)

**Why not just use userspace?**

- Userspace tunnel adds significant latency (user↔kernel context switches)
- Requires TCP state tracking for every connection (complex, memory-intensive)
- Much higher CPU usage for high-throughput workloads

### Workload IP Routing

Located in: `agent/agent.go:1400` (`ensurePeerTunnel`)

Workloads get IPs from the service CIDR (default: 10.100.0.0/16). Traffic routing:

```
Client → 10.100.x.x (workload IP)
      ↓
Local routing table → IPIP tunnel to peer's WARP IP
      ↓
IPIP encapsulation: 10.100.x.x packet wrapped in 100.96.x.x packet
      ↓
WARP routes 100.96.x.x to correct peer
      ↓
Peer decapsulates → delivers to local container via DNAT
```

**Why separate service CIDR from WARP CIDR?**

- WARP IPs (100.96.x.x) are assigned by Cloudflare, can change
- Service IPs (10.100.x.x) are stable, chosen by Jetty
- Services maintain consistent IPs even when nodes restart

### The Userspace Tunnel Implementation

Located in: `agent/agent.go:606-1350`

When kernel tunnels aren't available, Jetty implements a full TCP/UDP proxy:

1. **TUN device** captures packets destined for remote workloads
2. **WebSocket connection** to peer's `/api/tunnel/ws` endpoint
3. **Protocol handlers** for ICMP, TCP, UDP

**Why is this so complex?**

The userspace tunnel must:
- Parse IP headers to determine destination
- Track TCP connection state (SYN, ACK, FIN, RST, sequence numbers)
- Calculate TCP checksums with pseudo-headers
- Handle connection cleanup on timeout/reset

**This complexity is necessary because:**
- We can't use raw sockets on many restricted platforms
- We need bidirectional communication (requests AND responses)
- TCP requires stateful handling for reliability

**Could this be simplified?**

Possibly, by using an existing userspace TCP/IP stack library (e.g., gVisor's netstack). However:
- Would add a large dependency
- Current implementation is ~750 lines, well-contained
- Works correctly for the common use cases

---

## Failover System

### Deterministic Leader Election

Located in: `agent/agent.go:5439` (`shouldClaim`)

When a workload's owner becomes unhealthy:

```go
// Collect all healthy nodes allowed to run this workload
candidates := []string{...}

// Sort by node ID (deterministic ordering)
sort.Strings(candidates)

// Lowest ID wins
return candidates[0] == a.hwid
```

**Why deterministic election?**

- **No coordination needed**: All nodes independently reach same decision
- **No split-brain**: Can't have two nodes both think they won
- **No voting protocol**: Faster, simpler, no quorum requirements

**Why "lowest ID wins"?**

Any deterministic rule works. Lowest ID is simple and stable:
- Node IDs don't change during runtime
- Alphabetically first is unambiguous
- Easy to understand and debug

**Alternative considered: Load-based selection**

Could pick the node with lowest CPU usage. Rejected because:
- CPU usage is dynamic, could cause flip-flopping
- Requires more state synchronization
- Determinism is more valuable than optimal placement

### Failover Timing

```
Health timeout: 45 seconds (3 missed heartbeats)
Failover check: every 15 seconds
```

**Why these values?**

- **45s health timeout**: Allows for transient network issues (3 × 15s check)
- **15s failover check**: Balance between responsiveness and avoiding flapping
- **10s gossip interval**: Fast enough for state propagation, slow enough to not flood

**Trade-off:**
- Faster detection = more risk of false positives (flapping)
- Slower detection = longer downtime during real failures

---

## Cloudflare Integration

### Two Cloudflare Components

1. **WARP Connector** (`JETTY_WARP_CONNECTOR_TOKEN`)
   - Provides mesh networking (100.96.x.x IPs)
   - Encrypted L3 connectivity between nodes
   - Managed via `warp-cli`

2. **Cloudflared Tunnel** (`JETTY_CF_TOKEN`)
   - Provides external API access
   - Routes `cluster.example.com` → node's API port
   - Cloudflare load-balances across all nodes

**Why two separate components?**

- WARP: Always needed for inter-node communication
- Tunnel: Optional, only needed for external API access
- Different tokens allow different Cloudflare accounts/teams

### Cloudflared Process Monitoring

Located in: `agent/cloudflared.go:117` (`monitorCloudflared`)

```
Initial backoff: 5 seconds
Max backoff: 2 minutes
Max failures: 10 consecutive
Success reset: 30 seconds running = reset counters
```

**Why exponential backoff?**

- Prevents thundering herd on Cloudflare if token is invalid
- Gives time for transient issues to resolve
- Eventually gives up (10 failures) to avoid infinite retries

**Why 30-second success reset?**

If the process runs for 30+ seconds, it's probably working correctly. Reset the failure counter so transient crashes don't accumulate toward the 10-failure limit.

---

## Simplification Opportunities

### 1. Duplicate Sync Logic in sync.go

**Current state:**
- `mergeWorkloadState()` - used during normal gossip (lines 24-95)
- `mergeStartupSyncData()` - used at startup (lines 97-151)

These functions are ~90% identical, differing only in logging messages.

**Potential simplification:**

```go
type MergeContext string
const (
    MergeContextGossip  MergeContext = "Sync"
    MergeContextStartup MergeContext = "Startup sync"
)

func (a *Agent) mergeWorkloadState(syncResp *SyncResponse, ctx MergeContext) *MergeResult {
    // Single implementation with context-aware logging
}
```

**Why it exists this way:**
The functions were likely created separately during different development phases. The startup version has different logging semantics ("while we were down" vs ownership transfer).

**Recommendation:** Could be unified with a logging context parameter. Low priority since both work correctly.

### 2. HTTP Client Configuration

**Current state:**

```go
// In types.go
var (
    httpClient          = &http.Client{Timeout: 30s}
    peerClient          = &http.Client{Timeout: 5s}
    unhealthyPeerClient = &http.Client{Timeout: 1s}
)

// Also in types.go
type HTTPClients struct { ... }
func NewHTTPClients() *HTTPClients { ... }
```

The `HTTPClients` struct and `NewHTTPClients()` factory exist but aren't used. Comment says "for backwards compatibility during refactor".

**Potential simplification:**
- Either complete the refactor to use `HTTPClients` struct
- Or remove the unused struct/factory

**Why it exists this way:**
Partial refactoring - the global variables work fine, the struct was intended to encapsulate them but was never fully integrated.

**Recommendation:** Low priority. The global variables work correctly. Could clean up unused code.

### 3. Tombstone Garbage Collection Duplication

**Current state:**
- `cleanupTombstones()` in `sync.go:334`
- `gcTombstones()` in `agent.go:5168`

Both do the same thing: remove tombstones older than 1 hour.

**Potential simplification:**
Remove one and use the other everywhere.

**Why both exist:**
- `cleanupTombstones` in sync.go: Part of the sync module
- `gcTombstones` in agent.go: Called from gossip loop

**Recommendation:** Delete `cleanupTombstones()` from sync.go since it's not called. Only `gcTombstones()` is used.

### 4. Route Tracking Could Be Computed On-Demand

**Current state:**

```go
workloadRoutes map[string]string // workload IP -> owner WARP IP
```

Routes are manually tracked and updated during sync.

**Potential simplification:**

```go
func (a *Agent) getWorkloadRoute(workloadIP string) string {
    a.stateMu.RLock()
    defer a.stateMu.RUnlock()
    if wl := a.state.Workloads[workloadIP]; wl != nil {
        if peer := a.state.Peers[wl.Owner]; peer != nil {
            return peer.IP
        }
    }
    return ""
}
```

**Why the current approach:**
- Caching avoids map lookups on every packet
- The userspace tunnel is hot path - every packet queries this

**Trade-off:**
- Manual tracking = risk of stale data if sync misses an update
- On-demand = slower but always consistent

**Recommendation:** Keep current approach for performance, but ensure routes are always updated when state changes.

### 5. TCP/UDP Proxy Could Use Existing Library

**Current state:**
~750 lines of manual TCP state machine, checksum calculation, packet construction.

**Potential simplification:**
Use gVisor's netstack or similar userspace TCP/IP library.

**Why manual implementation:**
- Avoids large dependency
- Jetty only needs basic proxy functionality
- Full TCP stack is overkill (don't need congestion control, etc.)

**Trade-off:**
- Manual = more code to maintain, potential bugs
- Library = large dependency, but well-tested

**Recommendation:** Current implementation works. Only consider library if bugs are found in edge cases.

---

## Summary of Recommendations

| Area | Priority | Status |
|------|----------|--------|
| Duplicate sync functions | Low | Keep (different logging contexts) |
| Unused HTTPClients struct | Low | **DONE** - Removed |
| Duplicate tombstone GC | Low | **DONE** - Removed sync.go version |
| Route tracking | Keep | Current approach is correct for performance |
| Userspace tunnel | Keep | Manual implementation is adequate |

### Applied Simplifications

The following cleanup changes have been applied:

1. **Removed unused `cleanupTombstones()` from sync.go** - This function was defined but never called. The `gcTombstones()` function in agent.go handles tombstone garbage collection.

2. **Removed unused `HTTPClients` struct and `NewHTTPClients()` factory** - These were marked as "for backwards compatibility during refactor" but were never used. The global HTTP client variables remain and work correctly.

The codebase is generally well-architected for its purpose. Most "complexity" exists for good reasons:
- Tombstones: Necessary for eventual consistency
- Three-tier tunneling: Necessary for platform compatibility
- Deterministic failover: Necessary for consensus-free coordination
- TCP proxy: Necessary for restricted environments

The identified simplifications are mostly code cleanup opportunities rather than architectural issues.
