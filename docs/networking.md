# Jetty Networking

This is the explainer for how Jetty wires up packets across nodes and
workloads. The code is in `agent/agent.go` (host setup), `agent/tunnel.go`
(userspace fallback), and `agent/deploy.go` (per-workload DNAT). Read this
top-to-bottom; the layers build on each other.

If you only have a minute: every workload gets a virtual `10.100.x.x` IP that
is the same on every node. Local workloads are reached by host DNAT. Remote
workloads are reached by encapsulating the packet in IPIP (kernel) or by
shipping the raw IP packet over a WebSocket (userspace fallback). Cloudflare
WARP is the underlay that connects the nodes.

---

## The three layers

```
┌──────────────────────────────────────────────────────────────────────┐
│  Layer 3:  Workload mesh   10.100.0.0/16     (the service CIDR)      │
│            Each workload gets a stable IP. Same on every node.       │
│            Local: DNAT on the host to the container's bridge IP.     │
│            Remote: encapsulated and shipped to the owning node.      │
├──────────────────────────────────────────────────────────────────────┤
│  Layer 2:  WARP underlay   100.96.0.0/12     (the node mesh)         │
│            Each node gets one WARP IP. All nodes are reachable from  │
│            all nodes via Cloudflare's overlay. This is the underlay  │
│            our IPIP/GRE/WebSocket tunnels run over.                  │
├──────────────────────────────────────────────────────────────────────┤
│  Layer 1:  Whatever the node has - LAN, WAN, behind NAT, doesn't     │
│            matter. WARP papers over it.                              │
└──────────────────────────────────────────────────────────────────────┘
```

Three address ranges to keep straight:

| Range            | What lives there                              |
| ---------------- | --------------------------------------------- |
| `100.96.0.0/12`  | Node WARP IPs. One per Jetty node.            |
| `10.100.0.0/16`  | Workload mesh IPs. Configurable per cluster.  |
| `172.x.x.x`      | Per-workload Docker bridge IPs (the actual container IP). |

---

## Per-node host setup

When the agent boots, `initNetwork` (in `agent/agent.go`) lays down the
following:

### `jetty0` — dummy interface for binding workload IPs

```
ip link add dev jetty0 type dummy
ip link set up dev jetty0
```

A "dummy" interface is a black hole that lets you bind IPs without
forwarding them anywhere. We use it as a parking spot for every local
workload's mesh IP, so the kernel says "yes, this host owns 10.100.0.50"
and processes incoming packets for that IP.

### `/proc/sys/net/ipv4/ip_forward = 1`

Required so the kernel forwards packets between Docker bridges, jetty0,
and the IPIP/GRE tunnels.

### Inter-workload FORWARD ACCEPT (the rules from the open branch)

```
iptables -I FORWARD 1 -d 10.100.0.0/16 -j ACCEPT
iptables -I FORWARD 2 -s 10.100.0.0/16 -j ACCEPT
```

Each workload runs in its own docker-compose project, which gives it its
own bridge network with its own iptables FORWARD rules from Docker. Docker's
defaults block traffic that crosses bridges (network isolation). So when a
container in workload A on bridge `br-A` sends to workload B's mesh IP and
the packet has to leave `br-A` to reach the kernel routing table, Docker's
DROP rules can intercept it.

We insert ACCEPT at the top of FORWARD for the service CIDR in both
directions. `-I FORWARD 1` matters: insert before Docker's rules, not after.

### MASQUERADE for the service CIDR

```
iptables -t nat -A POSTROUTING -d 10.100.0.0/16 -j MASQUERADE
```

When a container sends a packet to a mesh IP and the packet egresses
through a tunnel (or even just gets DNATed locally), the kernel rewrites
the source from the container's bridge IP (`172.x.x.x`) to the host's
outbound IP. Without this, the remote side has no route back to
`172.x.x.x` and the response is dropped.

Trade-off: the receiving workload sees `host_IP` as the source, not the
original container. If you need the real client IP, you need a different
solution (PROXY protocol, X-Forwarded-For at L7, etc.).

### `nft` table `ip jetty` — WARP-side masquerade

```
nft add table ip jetty
nft add chain ip jetty postrouting { type nat hook postrouting priority srcnat; }
nft add rule ip jetty postrouting oifname "CloudflareWARP" masquerade
```

For traffic egressing through `CloudflareWARP` directly (like a node-to-
node mesh handshake), source-NAT to the WARP IP. This is in our own table
so we can drop it cleanly on shutdown without touching Docker's nat rules.

Jetty also deletes the `cloudflare-warp` nft table that the WARP daemon
creates (a default-DROP firewall that breaks SSH, git, and cloudflared).
We only want WARP for routing, not for firewalling — see `initWarpRules`.

---

## Per-workload setup

When a workload deploys (`deployWorkload` → `setupWorkloadIP` in
`agent/deploy.go`):

```
ip addr add 10.100.0.50/32 dev jetty0
iptables -t nat -A PREROUTING -d 10.100.0.50 -j DNAT --to <containerIP>
iptables -t nat -A OUTPUT     -d 10.100.0.50 -j DNAT --to <containerIP>
```

Three things happen:

1. The mesh IP is parked on `jetty0` so the kernel claims it.
2. PREROUTING DNAT rewrites incoming packets for that mesh IP to the actual
   container's bridge IP. This is what makes "the mesh IP behaves like the
   container" work for traffic coming from anywhere on the host (including
   from other containers).
3. OUTPUT DNAT does the same for traffic originating on the host itself
   (Jetty's own API requests to a workload, debug curl from the host shell,
   etc.). PREROUTING doesn't fire for locally-generated packets, so OUTPUT
   is needed too.

Container IP discovery has a retry loop in `setupWorkloadIP`: `docker
compose up -d` returns before the container has its bridge IP assigned, so
we poll `docker inspect` up to 10 times with 500ms backoff steps.

---

## Inter-workload routing — local case

```
Container A (workload "npm" on Node 1, bridge IP 172.20.0.5)
  ↓ sends to wordpress (resolved to 10.100.0.50 via /etc/hosts)
Bridge br-jetty_npm
  ↓ packet enters host
PREROUTING nat:
  -d 10.100.0.50 -j DNAT --to 172.21.0.5     ← rewrites dst to wordpress's container IP
  ↓
FORWARD:
  -d 10.100.0.0/16 ACCEPT                    ← our rule (rule 1)
  ↓
Bridge br-jetty_wordpress
  ↓
Container B (workload "wordpress", 172.21.0.5)
```

POSTROUTING MASQUERADE rewrites src `172.20.0.5` → host IP, so the response
from wordpress can route back through the host's bridge to npm.

---

## Inter-workload routing — cross-node case

```
Container A on Node 1 (172.20.0.5)
  ↓ sends to 10.100.0.50 (wordpress, owned by Node 2)
Bridge br-jetty_npm
  ↓
PREROUTING nat: no DNAT match (wordpress isn't local)
  ↓
Routing decision:
  10.100.0.50/32 dev tun_<peerB>             ← installed by updateWorkloadRoutes
  ↓
FORWARD:
  -s 10.100.0.0/16 ACCEPT                    ← our rule (rule 2)
  ↓
POSTROUTING:
  -d 10.100.0.0/16 -j MASQUERADE             ← src rewritten to Node 1's host IP
  ↓
tun_<peerB>: IPIP (or GRE) encapsulation
  ↓
Outer packet: src=Node 1 WARP IP, dst=Node 2 WARP IP, proto=4 (IPIP)
  ↓
CloudflareWARP interface → Cloudflare's overlay → Node 2

Node 2 receives:
  ↓
Kernel decapsulates IPIP
  ↓
Inner packet: src=Node 1 host IP, dst=10.100.0.50
  ↓
PREROUTING nat: -d 10.100.0.50 -j DNAT --to 172.21.0.7
  ↓
Bridge br-jetty_wordpress → wordpress container
```

The reply traces the same route in reverse, terminated by the IPIP
de-encapsulation on Node 1.

---

## The three transport modes

`detectTunnelMode` picks the best available at startup. `updateWorkloadRoutes`
installs routes accordingly.

### 1. IPIP (preferred)

Kernel module `ipip`. Adds a 20-byte IP header, total overhead 20 bytes.
One tunnel per peer named `tun_<first-8-of-peerID>`. Packets in the service
CIDR get routed to `tun_<peer>`, kernel encapsulates them, sends them to
the peer's WARP IP.

Requires the `ipip` kernel module loaded on both ends. `modprobe ipip` is
attempted at startup and fails silently if the host doesn't allow loading
modules (sandboxed envs, ChromeOS, some hardened distros).

### 2. GRE (fallback)

Same idea, kernel module `ip_gre`, slightly more overhead. Used when IPIP
isn't available but GRE is.

### 3. Userspace tunnel (last resort)

When neither IPIP nor GRE is available, Jetty creates a TUN device named
`jetty_tun` (MTU 1280) and routes mesh IPs to it:

```
ip route add 10.100.0.50/32 dev jetty_tun
```

`tunReadLoop` (in `agent/tunnel.go`) reads raw IP packets from the TUN,
looks up the owning peer, opens a WebSocket to
`ws://<peerWARPip>:6880/api/tunnel/ws`, and sends the IPv4 packet as a
binary message. The peer receives it in `apiTunnelWs` →
`handleTunnelProxy`, which:

  - Parses the IP header.
  - Verifies the destination is one of our local workloads (security guard).
  - Dispatches by protocol:
    - **ICMP echo**: sends our own ping to the workload, repackages reply.
    - **TCP**: maintains a per-flow connection table, opens a real TCP
      connection to the workload on SYN, proxies data, synthesizes
      SYN+ACK / ACK / FIN+ACK / RST responses with hand-built TCP
      headers + checksums + sequence numbers.
    - **UDP**: opens a one-shot UDP socket, writes the payload, reads up
      to one response, ships it back.
    - Anything else: dropped with a log line.

This path also requires the `/api/tunnel/ws` endpoint to be available on
the peer, which means the peer's API must be reachable on the WARP IP at
port 6880. The userspace tunnel is a *receive*-side concern as well: Jetty
always starts the listener, even if it has IPIP available, because peers
without IPIP need somewhere to send to.

The userspace tunnel is for ChromeOS / containers / sandboxes where the
operator can't load kernel modules. **It is not a real TCP stack** — see
"Issues" below.

---

## Routes installed for remote workloads

`updateWorkloadRoutes` (in `agent/agent.go`) is called any time the
workload table or peer table changes. For each remote workload, it
installs a `/32` route to the workload IP pointing at the appropriate
tunnel:

```go
if a.tunnelMode != "" {                        // IPIP/GRE
    ip route add 10.100.0.50/32 dev tun_<peerID>
} else if a.tunDevice != nil {                 // Userspace
    ip route add 10.100.0.50/32 dev jetty_tun
} else {                                       // Last resort
    ip route add 10.100.0.50/32 via <peerWARPip> dev CloudflareWARP
}
```

The "last resort" branch (direct WARP route) requires WARP itself to know
how to forward `10.100.x.x` traffic to the right peer. By default WARP only
routes `100.96.0.0/12` (the WARP CIDR), so this branch typically does not
work end-to-end without additional Cloudflare-side configuration. It's
effectively a dead path; the userspace tunnel is the practical fallback.

---

## Names and discovery — two layers

There are two separate things Jetty does for name resolution. They serve
different consumers.

### Layer A: host-side `/etc/hosts` (for the agent and `cloudflared`)

`updateHosts` rewrites `/etc/hosts` between `# JETTY START` and `# JETTY
END` markers, putting in:

  - This node's own hostname → its WARP IP
  - Every peer's name → its WARP IP
  - Every workload's name → its mesh IP (with a `# workload (local|remote)`
    comment)

This file is read by anything running in the host's network namespace:
the Jetty agent itself, the `cloudflared` subprocess it spawns, and any
operator running `curl postgres` from a host shell. Jetty runs `--net
host` so it shares this file directly.

Workload containers do *not* see this file. Each container has its own
`/etc/hosts` that Docker generates at start, populated only with localhost
and same-compose-project entries. That's where layer B comes in.

The block is hash-skipped (we only rewrite when peers, workloads, or status
actually changes — no per-tick churn).

### Layer B: per-workload `docker-compose.override.yml` (for cross-workload DNS)

When a workload deploys, Jetty parses the user's `docker-compose.yml`,
discovers its service names, and writes a sibling
`docker-compose.override.yml` containing:

```yaml
services:
  <each service from the user's compose>:
    extra_hosts:
      - "nginx:10.100.0.20"
      - "postgres:10.100.0.51"
      - "wordpress:10.100.0.50"
      - ...
```

The compose CLI is invoked with `-f docker-compose.yml -f
docker-compose.override.yml` and merges them. Result: every container in
the workload starts with `/etc/hosts` entries for every workload in the
cluster.

That's how a container in `nginx` can do `curl http://wordpress` and it
resolves to a mesh IP, even when wordpress is on a different node. The
mesh IP routes via the host's tunnel table (Layer 3) and lands at the
right container.

#### What happens on failover

Mesh IPs are stable. When `wordpress` fails over from Node A to Node B,
the mesh IP `10.100.0.50` doesn't change. The entry already in nginx's
`/etc/hosts` keeps pointing at the right address — it's just that the
host's `/32` route for `10.100.0.50` now points at `tun_<NodeB>` instead
of `tun_<NodeA>`. Nginx never knows the difference. No restart needed.

#### What happens when a new workload is added

The agent regenerates the override files for all owned workloads on every
gossip tick when the host map has changed (hash-checked, cheap when
nothing changed). But it does *not* restart the running containers. So:

  - Existing workloads keep working with the entries they had at start.
  - The new workload is in the override file on disk.
  - Existing containers can't see the new entry until they restart.
  - Any restart picks it up: failover, image update, manual `docker
    compose restart`, or `POST /api/workloads/{name}/start` after a stop.

This is a deliberate trade-off: avoid surprise container restarts of
unrelated workloads when you add a new service. If you want every
existing service to discover a newly-added one immediately, restart it.

---

## Tunnel topology

For IPIP/GRE mode, every node creates a tunnel to every healthy peer
(`updateWorkloadRoutes` ensures this on each cycle). With N nodes, you have
N×(N-1) tunnel endpoints across the cluster. For small clusters (< 20
nodes) this is fine; it doesn't scale to hundreds.

The tunnels are bidirectional: IPIP encapsulation requires the receiving
side to have a tunnel from us back to it, otherwise the kernel rejects the
encapsulated packets. That's why we create tunnels to all healthy peers,
not just to peers that own workloads we route to.

---

## Issues / known sharp edges

These are real and worth knowing about before you stake anything important
on Jetty.

### 1. Userspace TCP stack is not a real TCP stack

`agent/tunnel.go` hand-rolls TCP responses for the userspace tunnel:

  - **No retransmission.** If a WebSocket frame is lost, the connection
    hangs. Browsers retry HTTP, so it's not always visible, but a long-
    running stream just dies.
  - **No congestion control.** Window is hardcoded to `0xFFFF`. Sender
    will happily flood.
  - **No SACK, no PAWS, no timestamps.**
  - **Initial sequence number** is `time.Now().UnixNano() & 0xFFFFFFFF` —
    not RFC-compliant random ISN, predictable, technically a TCP injection
    risk, though the WS endpoint requires authentication so it's mitigated.
  - **MSS** is hardcoded to 1240 to fit MTU 1280. No PMTUD.
  - **The chunking loop in `tcpProxyReadLoop`** sends segments without
    waiting for ACKs. The receiver's TCP stack will reorder/buffer based
    on sequence numbers, so it works, but if the sender (us) is faster
    than the receiver can consume, segments stack up in the receive
    buffer.
  - **No keepalive.** Idle connections are pruned only when the upstream
    socket EOFs.

In practice it is good enough to serve HTTP, ping a database, etc. It is
not good enough to soak production traffic. The right long-term move is
either (a) require IPIP/GRE and drop the userspace path, or (b) replace
the hand-rolled TCP with `gvisor.dev/gvisor/pkg/tcpip`'s netstack.

### 2. Three sync paths can race

State updates can flow through:

  - Memberlist broadcasts (`broadcastWorkloadUpdate` etc.)
  - Memberlist periodic full sync (every 30s, picks a random peer and pulls)
  - HTTP gossip fallback (every 10s, `gossipLoop` → `syncWorkloads`)

`mergeWorkloadState` enforces "highest version wins" so eventual
consistency holds, but you can see workloads briefly flip between owners
during failover or join. The version is `time.Now().UnixNano()`; if two
nodes' clocks are skewed by more than the interval between writes, the
"wrong" one wins. There is no NTP gating.

### 3. `cleanupOrphanedState` and `cleanupNetwork` are aggressive

`cleanupOrphanedState` runs at startup and previously deleted any `/32`
route in `10.0.0.0/8` (now scoped to `serviceCIDR`). It also deletes any
interface starting with `tun_`, which collides with anything else on the
host using that prefix.

`cleanupNetwork` (on shutdown) used to disconnect WARP and delete the
`CloudflareWARP` interface unconditionally — even if the operator had set
up WARP themselves before installing Jetty. We now record `warpPreexisting`
at boot and skip the WARP teardown if so, but the existence-check is naive
(just looks for the interface name).

### 4. Node-to-node packet visibility through Cloudflare

WARP traffic transits Cloudflare's edge. Cloudflare can see the outer IP
header of the IPIP/GRE packets. The inner workload payload is whatever your
app put on the wire — TLS-encrypted in most cases, but not always.

If end-to-end secrecy from Cloudflare matters, run TLS or a proper VPN
inside the mesh. Jetty's "encrypted by default" claim refers to WARP
encrypting the underlay against snoopers between nodes and Cloudflare, not
end-to-end against Cloudflare itself.

### 5. WARP IP reachability assumed but not verified

`getPeerAPIURL` and friends construct `http://<peerWARPip>:6880/...` URLs
without checking that the peer's API is reachable from this node. If WARP
routing is misconfigured (e.g., the operator forgot to set "Include IPs and
domains: 100.96.0.0/12" in the Zero Trust dashboard), nothing will work
and the only signal is timeouts in the logs.

### 6. FORWARD rule ordering is fragile

`iptables -I FORWARD 1 ...` puts our rule at position 1. Docker also
inserts FORWARD rules. If Docker restarts after Jetty, Docker's rules go
on top, ours get pushed down, and inter-workload mesh traffic breaks until
we re-insert. The agent doesn't currently re-apply FORWARD rules on a
periodic basis — it should.

### 7. `/etc/hosts` write amplification

`updateHosts` rewrites the file every gossip tick (10s), regardless of
whether anything changed. With 20 workloads and an SSD it's fine; on slow
storage or with file-watch tooling on the host it shows up.

### 8. Multi-arch failover is silent

If a workload only has `compose_arm64` and the only healthy peer is
`amd64`, `findAllowedNode` returns nil and the failover loop just skips it
on each tick. There is no warning, no "marooned" status — the workload
just sits there with a dead owner. A "no compatible node available" status
on the workload would help.

---

## Quick reference — what's where

| File                  | What it owns                                              |
| --------------------- | --------------------------------------------------------- |
| `agent/agent.go`      | Lifecycle, host network setup, peer tunnel mgmt, routing  |
| `agent/tunnel.go`     | TUN device + WS forwarder + handrolled TCP/UDP/ICMP proxy |
| `agent/deploy.go`     | `docker compose` invocation, per-workload IP/DNAT setup   |
| `agent/gossip.go`     | Peer health checks, periodic sync, peer announcements     |
| `agent/failover.go`   | Orphaned-workload claim election                          |
| `agent/memberlist.go` | hashicorp/memberlist delegate, broadcast handlers         |
| `agent/sync.go`       | State merge with tombstones                               |
| `agent/cloudflared.go`| Cloudflare Tunnel subprocess management                   |
| `agent/crypto.go`     | AES-256-GCM with Argon2id key derivation                  |
| `agent/state.go`      | `state.json` persistence                                  |
