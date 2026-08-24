# JettyOS — Outline

Jetty is the hardware. JettyOS is the desktop you drive it with.

This document maps desktop concepts onto Jetty mechanisms, lists what JettyOS is
made of, and lists what Jetty needs added.

---

## 1. Jetty as hardware

Treat the cluster as one machine. Jetty is its board.

| Machine part | Jetty |
|---|---|
| CPU / RAM / disk | a node — reports arch, and should report GPU, cores, memory, free disk |
| System bus | the WARP mesh (`10.100.0.0/16` service network) |
| Device tree — what hardware exists | cluster state, replicated to every node |
| Process | a workload (a compose project) |
| Process address | a mesh IP, stable across node moves |
| Device name | the workload's DNS name |
| Slot / socket assignment | node pinning (`allowed_nodes`) |
| Hot-swap | failover — a workload's node dies, another picks it up |
| Firmware console | the existing dashboard and API |

What this hardware already gives you, for free:

- Run a container anywhere in the cluster and reach it by name from anywhere else
- Mixed architectures (ARM and x86) in one machine
- A thing that keeps running when the machine it was on dies
- Persistent, replicated record of everything that exists
- Add a node and the machine gets bigger, with no rebuild

What it does not give you: a screen, a session, a launcher, or any concept of a
user sitting in front of it. That's JettyOS.

---

## 2. JettyOS

A workload that serves a web desktop. It holds the cluster's admin credential
and uses the Jetty API to run everything else.

- **Single user.** You're root. Extra users are a later feature, not a
  foundation.
- **Served over the tunnel**, same as anything else. Open it from any browser.
- **The existing dashboard stays**, separately and publicly, for hardware-level
  work: nodes, tunnels, join tokens, backups. JettyOS never becomes the only way
  in — when it won't boot, that dashboard is how you fix it.

The browser is the screen, keyboard, and mouse. The cluster is everything else.

---

## 3. Desktop → cluster mapping

The core of the design.

| Desktop thing | Jetty mechanism | Exists today? |
|---|---|---|
| Launch an app | create a workload | yes |
| Quit an app | delete the workload | yes |
| App window | a stream from the workload to the browser | **no** |
| Task manager | list workloads + nodes | yes |
| "Run this on the GPU box" | `allowed_nodes` | yes |
| "Run this anywhere" | pick a node at spawn | **no — no scheduler at launch** |
| Open session (what's running) | a tag grouping the session's workloads | yes |
| Close everything | bulk action on the tag | yes |
| Move a window to another machine | `POST /workloads/{name}/move` | yes |
| Autostart on login | `autostart: true` | yes |
| Background service (music daemon, sync) | ordinary durable workload | yes |
| Files | a storage workload (NFS/SMB/S3) mounted by others | yes, by convention |
| Clipboard, notifications | small always-on workloads with DNS names | yes |
| Install an app | an entry in a catalog → a compose template | **no — needs a catalog** |
| Short-lived window | needs a cheap workload class | **no — see §6** |

Most of the desktop is already expressible. Three gaps: **windows, launch-time
placement, and cheap short-lived workloads.**

---

## 4. How a window works

The one flow that has to be designed rather than assembled.

```
click app in launcher
  → JettyOS asks Jetty to create a workload (from a catalog template)
  → Jetty picks a node (or honours the pin)
  → container starts, app runs, exposes a display endpoint
  → browser connects to THAT NODE directly and renders
  → window closes → workload deleted
```

Two rules that decide whether this feels good:

**Stream direct, not through JettyOS.** If node C runs the app, the browser
connects to node C. Relaying pixels through whichever node serves JettyOS adds a
hop through the mesh for every frame. Control messages (launch, kill, focus) go
through JettyOS; pixels don't.

**Two kinds of window, not one.**

| | Semantic | Pixel |
|---|---|---|
| Carries | HTML/text/events | encoded video |
| Bandwidth | KB | Mbps |
| Needs GPU | no | yes |
| Use for | terminals, editors, web apps, dashboards, file manager | games, video, 3D |
| Share of use | most of it | a little |

Building one pixel-streaming path and using it for everything makes a terminal
feel like a game stream. Most windows should be a web app in an iframe or a
terminal over WebSocket — both nearly free.

For the pixel case, adopt existing work (WebRTC-based container streaming is a
solved problem with mature implementations) rather than writing an encoder.

---

## 5. Pinning does more work than it looks

Pin the heavy things — Steam to the GPU node, media server to the box with the
disks, build agent to the fast CPU.

That's not just placement. A pinned workload never moves, so **its data never
has to move either.** The apps with big state are exactly the apps you'd pin,
which means the hardest problem in the design (volumes don't follow failover)
doesn't apply to them.

Light, stateless things stay unpinned and land wherever there's room.

---

## 6. What Jetty needs added

Short list. Neither item changes how Jetty works for its current use.

**1. A cheap workload class.**
Everything today is durable infrastructure: written to cluster state, gossiped
to every node, tombstoned on delete. Correct for a database. Wrong for a
terminal window you open and close forty times an hour.

Needed: workloads that are node-local, not gossiped, don't fail over, and get
reaped automatically. The session's *manifest* (what should be open) stays in
cluster state; the individual containers don't.

This gates everything else.

**2. Placement at launch.**
Today a node is only chosen when one dies. Creating a workload puts it on
whichever node received the request.

Needed: "run this somewhere sensible" — by free memory, CPU, latency, or
required capability. The inputs are already sampled on every node and thrown
away.

**Worth doing alongside:** let nodes advertise capabilities (`gpu`, `nvme`,
`bigmem`) so a workload can say *"needs a GPU"* instead of *"runs on gpu-box"*.
Nodes already report architecture; this is the same idea. Cheap now, annoying
to retrofit once you have a catalog full of hostnames.

---

## 7. What JettyOS is made of

| Component | Job |
|---|---|
| Shell | window management, launcher, dock. Reads cluster state and draws it. Holds no state of its own — kill it and it redraws identically. |
| Catalog | app definitions → compose templates. What "install an app" means. |
| Session manager | tracks what's open, restores it, handles login |
| Display bridge | per-window transport to the browser, in both modes |
| Clipboard / notifications | small workloads, no special privilege |
| Ops dispatcher | runs commands where the data is and returns the result — not a container full of `grep`. Sending a command to the data beats pulling the data to the command. |

The shell holding no state is what lets you open JettyOS on your laptop, phone,
and TV at once and see the same desktop, with no sync protocol — they're all
just reading cluster state.

---

## 8. The thin node

If the machine with the screen is also a Jetty node, two things follow:

- Latency-sensitive apps schedule onto it and run locally — no streaming at all
- With the network down, it degrades to a normal single-machine computer instead
  of a dead terminal

So the terminal should be a cluster member, not a dumb client. "Thin" means its
specs stop being the ceiling, not that it does nothing.

---

## 9. Known limits

- **A process can't be bigger than one node.** Four 16 GB machines aren't a
  64 GB machine.
- **Launch is seconds, not instant** — image pull and container start. Fine for
  apps, useless for anything finer-grained.
- **No `ls | grep` across the cluster.** The unit is the application; anything
  smaller runs inside one.
- **Volumes don't follow failover.** Mitigated by pinning stateful things (§5).
- **Peripherals attach to the browser** (WebUSB/WebHID/gamepad), so their
  latency stacks on top of display latency.

---

## 10. Answers

The four questions, resolved against the code.

### Q1 + Q3: the cheap workload class, and the mesh IP

These turned out to be the same question, and the answer inverts the intuition.

`state.Workloads` is `map[string]*Workload` **keyed by mesh IP** — the IP is the
primary key, not an attribute. Everything downstream follows from that: sync
dedup (`sync.go`), failover (`claimStillHeld(ip)`), route installation
(`workloadRoutes[ip]`), `/etc/hosts`, compose `extra_hosts`, and
`/api/proxy/{meshIP}/`.

So a short-lived window that doesn't need a cluster-unique IP **cannot live in
that map at all.** Which resolves the flag-vs-concept question:

- **As a flag on `Workload`:** ~11 subsystems iterate `state.Workloads`
  unconditionally — gossip broadcast, both merge paths, tombstones, failover,
  `updateHosts`, `updateWorkloadRoutes`, compose overrides, `saveState`,
  autostart, reconcile, list. Each needs an `if !wl.Ephemeral` guard, and every
  future feature has to remember it. Brittle in proportion to the codebase's
  future size.
- **As a separate map** (`a.ephemeral`, keyed by session/window ID, no mesh IP,
  never persisted, never gossiped): **zero changes to those 11 subsystems.**
  They keep iterating `state.Workloads`, which simply doesn't contain them.

**Verdict: a new concept, and that is the cheap option.** The "smaller" change
is the expensive one. This is a smaller project than §6 implies — it is
additive rather than invasive, because the existing durable path is left
completely untouched.

What an ephemeral workload gives up by not having a mesh IP: cross-node DNS,
`/32` routing, failover, and `/api/proxy` reachability. For a window the browser
connects to directly on a known node, none of those are needed. If one *is*
needed later, the escape hatch is promoting it to a real workload.

### Q2: placement

Three paths choose a node today:

| Path | Behaviour |
|---|---|
| Create | `wl.Owner = a.hwid` — whichever node received the POST |
| Create, when that node isn't allowed | `findAllowedNode()` — **first match in map iteration order** |
| Failover | `shouldClaim()` — ranked election |

So placement exists but is arbitrary, and the fallback is non-deterministic
(Go map iteration order). Only failover ranks anything.

`shouldClaim`'s ranking **is separable** — it's ~15 lines that build a load
count and sort. Extracting `rankCandidates()` makes `shouldClaim` become
`ranked[0] == a.hwid` and gives launch-time placement `ranked[0]` for free.
**Launch-time placement is a generalization, not a new path**, and it's a
strict improvement to Jetty on its own — it replaces map-iteration-order with a
deliberate choice.

### Q2b: capabilities — the constraint that matters

`Peer` carries only `Arch`. Memory, CPU, load, and disk are all sampled
(`cpuSampleLoop` every 2s, `getSystemStats` on demand) and **never gossiped** —
they're reachable only by an explicit `/api/health?node=local` call per node.
There is no capability/label field anywhere.

**Do not put resource metrics in `NodeMeta`.** memberlist caps node metadata at
512 bytes, and the current payload (ID, Name, IP, Version, Arch, APIPort,
APIKey) already spends a few hundred of them. Worse, the overflow path today is
`return d.meta[:limit]` — it truncates JSON mid-string and ships a payload every
peer fails to parse, with only a log line. Adding fields there is a live
hazard, not just a tight fit.

So the split is:
- **`NodeMeta`** — small, slow-changing, high-value: capability labels (`gpu`,
  `nvme`, `bigmem`). Compact enough to fit, and exactly what "needs a GPU"
  requires.
- **A separate channel** — resource metrics, which are large and change
  constantly. Either periodic `/api/health` polling into an in-memory view, or
  a dedicated gossip message, not node metadata.

Capability labels are the cheap-now/expensive-later item §6 flagged, and that's
correct — but they belong in `NodeMeta` and the metrics do not.

### Q4: the streaming path

Three findings, in increasing order of usefulness:

1. **`bridgeWebSockets()` is already generic.** It forwards message type and
   payload verbatim in both directions, unbuffered. Reusable as-is for any
   stream; it just lives in `handlers_terminal.go` and should be lifted out.
2. **The terminal wire protocol is not generic** — `0x00` = data, `0x01` =
   resize(cols,rows). PTY-specific, as is shell selection and the `docker exec`
   invocation. A different payload needs its own protocol; the transport
   underneath is fine.
3. **`/api/proxy/` cannot upgrade WebSockets.** It's `httpClient.Do()`, with no
   hijack and no `websocket.Dialer`, so a browser upgrade request through it
   fails. This is the single concrete blocker for "browser opens a stream to a
   container", and it's a contained fix.

And the useful surprise: **`JETTY_TUNNEL_HOST` — a per-node subdomain — is
already read from the environment and stored, and then never used anywhere.**
That is precisely the primitive §4 needs for "the browser connects to THAT NODE
directly", sitting half-built. Wiring it up is much cheaper than designing
per-node addressing from scratch.

### What this means for sequencing

Build order implied by the above:

1. Per-node addressing (`JETTY_TUNNEL_HOST` + entry-node role) — §4's direct
   streaming has no other foundation
2. WebSocket upgrade in `/api/proxy/` — unblocks any browser↔container stream
3. `rankCandidates()` extraction + launch-time placement
4. Capability labels in `NodeMeta`; metrics on a separate channel
5. The ephemeral map — gated on 1–3, but genuinely additive once they exist

The Jetty-side prerequisites are tracked in ROADMAP.md under "JettyOS
enablers".
