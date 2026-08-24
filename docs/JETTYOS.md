# JettyOS — A Conceptual Architecture

**Status: speculative design.** This document argues a direction. It is not a
commitment, a plan, or a specification. It deliberately stays above the code —
a detailed audit of what Jetty actually does, function by function, is the
*next* document, and it should be written against a direction rather than
inventing one as it goes.

The question: can a Jetty cluster be a *computer* — one you sit down at, with a
terminal, a shell, and an optional desktop — rather than a place you deploy
services to?

The answer this document argues: **yes, but only for a specific and
non-obvious partition of the work.** Getting that partition right is the entire
design. Everything else is implementation.

---

## Part I — What a computer actually is

Before claiming a cluster can be one, it's worth being precise about what the
traditional model provides, because most of it is invisible until you try to
distribute it.

### The hierarchy, and the new layer

Every computer is a latency hierarchy. The numbers, to order of magnitude:

| Layer | Latency | Ratio to L1 |
|---|---|---|
| L1 cache | ~1 ns | 1× |
| L3 cache | ~15 ns | 15× |
| Main memory (DRAM) | ~100 ns | 100× |
| NVMe SSD | ~50 µs | 50,000× |
| **LAN round trip** | **~0.5–1 ms** | **~1,000,000×** |
| WiFi round trip | ~2–10 ms, jittery | ~10,000,000× |
| WAN / anycast round trip | ~10–50 ms | ~50,000,000× |

An operating system is, structurally, a machine for hiding this hierarchy. Virtual
memory hides the DRAM/disk boundary. The page cache hides the disk. Schedulers
hide the fact that one CPU is pretending to be many.

Distributing a computer means **adding a layer at the bottom that is 10,000×
slower than the one above it** — and unlike every other layer, this one can
fail partially.

### The two thresholds — the central insight

Traditional OS design treats the network as unusable for anything a program
depends on, and it is right to. Compared to a memory bus, a network is a
catastrophe.

But there is a second threshold that has nothing to do with buses:

| Human threshold | Budget |
|---|---|
| One frame at 60 Hz | 16.7 ms |
| Input-to-photon "feels instant" | ~50 ms |
| "That was noticeable" | ~100 ms |
| "This is laggy" | ~200 ms |

So:

- **DRAM (100 ns) → LAN (1 ms) is a 10,000× regression.** Catastrophic.
- **LAN (1 ms) → "feels instant" (50 ms) is 50× of headroom.** Comfortable.

**The network is far too slow to be a bus and far more than fast enough to be
a human interface.** That gap is where a cluster computer lives, and it is the
only reason this idea is coherent.

### The historical argument

A 7200 RPM hard disk seek was ~10 ms. Every desktop operating system whose
metaphors we still use — the ones designed between 1984 and 1995 — was built
assuming 10 ms was an acceptable latency for a user-facing operation. Opening a
file, launching an app, paging in a window: all of it budgeted against a disk
that slow.

**A modern LAN round trip is roughly twenty times faster than the storage that
all traditional desktop design assumed.**

The network today is better than the disk was when the desktop was invented.
That is why this is possible now and was not in 1995 — not browsers, not
containers, not WASM. Those are conveniences. The latency budget is the
substance.

### Three scarcities, and how they invert

A traditional OS exists to ration three scarce things:

1. **CPU time** — hence schedulers, preemption, priorities.
2. **RAM** — hence virtual memory, paging, the OOM killer.
3. **I/O bandwidth** — hence caches, queues, elevators.

On a cluster, the profile inverts:

- **CPU is abundant.** Add a node.
- **RAM is abundant but not poolable.** This is the one people always get
  wrong. Four machines with 16 GB each do not make a 64 GB machine. No process
  can exceed one node's RAM. Aggregate capacity grows; *maximum* capacity does
  not.
- **The new scarcity is locality.** Not cycles — distance.

A cluster OS's scheduler is therefore not rationing time. It is rationing
distance. That is a different job with a different shape.

### Time-sharing versus space-sharing

This follows directly:

| | Traditional OS | Cluster OS |
|---|---|---|
| Resource | scarce hardware | abundant hardware |
| Strategy | **time-share** it | **space-share** it |
| Decision | who runs next, for 5 ms | where this lives, for its lifetime |
| Frequency | thousands per second | a few per minute |
| Requires | preemption | placement |
| Can afford to be | fast and dumb | slow and smart |

A traditional scheduler makes a microsecond decision ten thousand times a
second, so it must be nearly free. A placement engine makes one decision per
process lifetime, so it can consult load, latency, capability, trust, and
affinity, and still cost nothing.

**This is a genuine advantage, not a consolation.** Placement quality is
something traditional operating systems cannot buy at any price, because they
have nowhere to place anything.

### Fate sharing versus partial failure

A traditional computer has a simplifying property that is never named because
it is never absent: **when it breaks, all of it breaks.** The CPU cannot lose
contact with the RAM. Half the machine cannot keep running. Every abstraction —
every syscall, every pointer, every open file — is built on the assumption that
the substrate is either entirely present or entirely gone.

Distributed systems have no such property. Partial failure is the defining
characteristic, and it is why distributed programming is hard.

The trade is exact:

- **You lose:** the assumption that a call returns. Every boundary becomes a
  place where the answer can be "I don't know," which is worse than "no."
- **You gain:** survivability. Your computer can keep working when part of it
  breaks. No desktop OS has ever offered this, and it cannot, because fate
  sharing is the assumption it is built on.

Jetty is already a partial-failure machine — gossip, health detection,
ownership, failover, tombstones. That is the single most valuable thing it
brings to this idea, and it is the part that would be hardest to build from
scratch.

### The display is memory

One more traditional property, and it is the awkward one.

On a real computer the framebuffer *is* memory. The GPU writes pixels into RAM
and the display controller scans them out. Zero copy, zero network, zero
serialization. The path from "program decided what to draw" to "photons" never
leaves the box.

Distributing a computer means the display is at the human and the computation
is not. Pixels must cross the network. There is no clever architecture that
avoids this — it is the one place where the cluster model is unavoidably worse
than the traditional one, and it is why VDI and thin clients have remained
niche for forty years despite being repeatedly reinvented.

---

## Part II — The inversion

### The partition rule

From the two thresholds, one rule follows, and everything else in this document
is downstream of it:

> **Anything a human waits on may cross the network.
> Anything a computation waits on may not.**

Applied:

| May be distributed | Must stay local |
|---|---|
| Launching an application | Executing instructions |
| Opening or saving a file | Memory access |
| Window management, focus, layout | The frame pipeline |
| Session state — what's running | Any tight loop |
| Service discovery, naming | Anything a program *blocks* on |
| Notifications, clipboard, search | Audio buffer refill |
| Anything human-initiated *and* human-observed | Anything program-initiated and program-observed |

The line falls almost exactly on the **process boundary** — and that is not a
coincidence. Processes were designed to be independently schedulable, isolated
in memory, and to communicate by message. That is already the shape of a thing
you can put on the far side of a network.

### Why "distribute inside a program" always fails

Every attempt at a single-system-image cluster — distributed shared memory,
Mosix, Plan 9's process migration, and a long tail of research systems — has
foundered on the same rock. If a program's memory accesses can cross a network,
then a pointer dereference can take a millisecond and can fail. Programs are
not written to survive that, and rewriting them to is called "distributed
programming," which is a hard problem that no operating system can solve on the
program's behalf.

**Distribute between programs. Never inside one.** The unit is the application.

### What Unix already got right, and what blocks it

Unix processes are nearly the right shape already: isolated address space,
message-based IPC, independently schedulable, killable and restartable. Three
things stop them being distributable as-is:

1. **`fork()`** — inheritance of an address space. Cannot cross a network.
2. **Shared memory and `mmap`** — the fast paths are all shared-memory paths.
3. **Syscalls that cannot fail** — the API has no vocabulary for "unreachable."

Plan 9 solved the third-and-a-half of this by making everything a named
resource in a per-process namespace, removing the need for shared memory to
find things. It was right and it lost, partly because the network of 1992 could
not carry it.

**The container model removes `fork` by construction.** Every process starts
from an image, never from a parent's memory. What looks like a limitation is
precisely what makes the model distributable — and it is why the design below
can be stateless in the places that matter.

---

## Part III — Where Jetty already sits

Jetty was not built to be an operating system. But because it solved the
partial-failure problem honestly, it accidentally has most of a kernel's
bookkeeping.

### What it already is, in OS terms

| OS concept | What Jetty already provides |
|---|---|
| Process table | cluster state, gossiped — replicated, survives node death |
| Process | a workload (a compose project) |
| Process identity | a stable mesh IP that survives migration |
| Name service | workload name → address |
| Isolation | containers, plus physical machine separation |
| CPU affinity | node pinning |
| Process group / session | tags, with group operations |
| Migration | blue-green move between nodes |
| Reaping | tombstones with a bounded lifetime |
| Placement engine | deterministic failover election |
| Heterogeneity | per-architecture images, mixed ARM and x86 in one cluster |

That is a substantial amount of an operating system arrived at sideways.

The process table in particular is the valuable part. **A traditional kernel's
process table is in RAM and dies with the machine.** Jetty's is replicated to
every node and converges. That single difference is what makes everything in
Part IV possible.

### The one conceptual mismatch

Jetty's mental model is **"services that should always be running."**
An operating system's mental model is **"things a person starts and stops all
day."**

Every default reflects the first: workloads are durable, gossiped cluster-wide,
revivable, individually addressable, and leave tombstones when deleted. That is
correct and well-tuned for a dozen long-lived services. It is wrong for
something a user opens and closes forty times an hour, where each open would
mean a cluster-wide state mutation and each close a tombstone.

**This is the single most important conceptual change, and everything else
follows from it:** an OS needs a second, cheaper class of thing.

---

## Part IV — The concept

### The thin node is a node

The obvious design is a dumb terminal: a browser in kiosk mode, all compute
elsewhere. This is the wrong design, for two reasons.

Make the terminal a **full cluster member that happens to have a display
attached**. Then:

1. **It is the nearest node, so locality-aware placement makes local things
   local for free.** A text editor schedules onto the machine you're sitting at
   — zero network, zero streaming, indistinguishable from native. Steam
   schedules onto the GPU box. A build schedules onto whatever is idle. The
   same mechanism handles all three, and the fast case requires no special
   casing.
2. **It degrades to a normal computer when the network dies.** A true thin
   client with no network is a brick. A thin *node* with no network is a
   single-machine computer running whatever it can host locally. This is not a
   detail — it is the difference between a machine you can depend on and one
   you cannot.

The "thin" in thin node is about *hardware expectations*, not about
capability. It can be a laptop, a NUC, or a Pi. What makes it thin is that its
own specifications stop being the ceiling on what you can run.

### Session is state; the shell is a projection

The desktop — which windows exist, where, on what — belongs in **cluster
state**, not in the shell's memory.

The shell (window manager, launcher, dock) becomes a **stateless renderer**: it
reads the session from the cluster and draws it. It holds nothing.

Three consequences, and they are the payoff of the whole design:

1. **The OS can crash and the applications survive.** They are separate
   workloads on separate machines. Your desktop dies, is revived, re-reads
   state, redraws — and your game never noticed. No conventional window manager
   can be killed without at minimum disturbing everything it manages.

2. **Multi-head across physical machines falls out for free.** Laptop, phone,
   TV — each running its own local shell instance, all rendering the same live
   session. No sync protocol, no handoff, no "continuity" feature, because they
   are all just readers of the same replicated state.

3. **Each head runs its own shell, pinned locally.** The shell is not one
   cluster-wide workload; it is one per display, always local, always
   stateless. Which is what makes (2) work without any coordination.

**The discipline this requires:** the shell must never cache. The first time it
keeps its own window list "for performance," multi-head breaks, failover
breaks, and it will still work perfectly on your desk — so you won't find out
until a node dies.

### Two workload classes

The conceptual change from Part III, stated concretely:

| | **Services** (what Jetty has) | **Instances** (what it needs) |
|---|---|---|
| Lifetime | months | minutes |
| State | durable, gossiped cluster-wide | node-local, not gossiped |
| Addressable | yes — stable mesh IP, DNS name | no — reachable only via its session |
| Failover | yes | no; if the node dies, the window dies |
| On delete | tombstone | just gone |
| Examples | database, file server, clipboard broker | a terminal, an editor window, a browser tab |

The addressing point matters more than it looks. If every window needed a
cluster-unique address, then rapid open/close would make address allocation a
hot, contended decision — exactly the class of decision that a
converge-don't-agree state layer handles badly. Instances don't need cluster
addresses. **Services get well-known names; instances don't** — which is the
same distinction Unix makes between well-known ports and ordinary processes.

The session ties them together with a **desired-state split**: cluster state
holds one small record per session saying *what should be open*; each node
reconciles its own containers against it. Declarative session, imperative local
execution. One gossiped record per session instead of one per window.

### Two planes

On a real computer, control and data share memory. Distributed, they cannot.

- **Control plane** — spawn, kill, list, focus, move, policy. Crosses the
  cluster mesh. Tolerates tens of milliseconds. This is where all the
  interesting architecture lives.
- **Data plane** — pixels, audio, input events. Goes **directly** from the node
  running the application to the display, never relayed through a third node.

Routing pixels through the control plane is the single easiest way to build a
beautiful architecture that feels terrible. This is a fork in the road at the
beginning, not a limitation discovered later.

### Two rendering modes

The other thing everyone gets wrong: building one pixel-streaming path and
using it for everything, which makes a terminal feel like a game stream instead
of like a terminal.

| | **Semantic remoting** | **Pixel streaming** |
|---|---|---|
| Carries | events, text, DOM, escape sequences | encoded video frames |
| Bandwidth | kilobytes | megabits |
| Latency tolerance | high — it's already event-driven | brutal — every millisecond visible |
| Needs GPU | no | yes, hardware encode |
| Covers | terminals, editors, chat, file managers, web apps, dashboards | games, video, 3D, CAD |
| Share of a normal day | ~90% | ~10% |

Most of what a person does all day is event-driven and remotes almost for free.
The expensive path is needed by a small set of applications — and those are
precisely the ones you would pin to a specific machine anyway, which means they
never migrate, which means their state never has to move.

**The pinning that makes the latency work also dissolves the state-migration
problem.** The two hardest constraints cancel each other, and this is the most
load-bearing structural fact in the design.

---

## Part V — What has to change in Jetty

Conceptually, not as a work plan. Roughly in dependency order.

1. **A second workload class.** Ephemeral, node-local, not gossiped, reaped
   automatically. Everything else depends on this existing.

2. **A placement engine that chooses.** Today placement happens on failover;
   a node is chosen only when one dies. An OS needs a choice made at *spawn* —
   by load, by latency, by capability, by trust. The inputs are already being
   measured; they are just discarded.

3. **Capability-based scheduling, not hostname pinning.** "This needs a GPU" is
   portable; "this runs on `gpu-box`" breaks when you rename or replace the
   machine. Nodes already report architecture — the concept generalizes.

4. **A session abstraction.** A named, per-user grouping distinct from both a
   single workload and the whole cluster. Groups already exist as a mechanism;
   they need to mean something.

5. **An inter-process call layer with a policy table.** This is the real gap.
   Today, communication between workloads is whatever you configure by hand.
   An OS needs named services (`fs.read`, `clipboard.get`, `gpu.claim`) that
   resolve to a node and are authorized or refused by a *table* — data, not
   code, and replicated like everything else. This is the layer that turns
   "workloads on a cluster" into an operating system rather than a deployment
   tool.

   It also corrects the tempting mistake of a "system utilities" workload
   holding `grep`, `find`, and friends. Those need *bytes*. If the bytes are on
   another node you have moved data to code, which at cluster latency is
   ruinous. **Ship the operation to the data.** A system-utilities service is a
   dispatcher, not a toolbox.

6. **A user.** There is currently one credential for the whole cluster. This is
   conceptually a single-user, root-only machine. Multi-user needs per-user
   identity and per-session scoping before any of it means anything.

7. **A display path.** Both modes, as separate mechanisms. This is the largest
   piece of genuinely new work, and it is the one where existing open-source
   projects should be adopted rather than reinvented — browser-delivered
   application streaming is a solved problem with several mature
   implementations.

8. **Ownership that is actually decided.** A converge-don't-agree state layer
   is correct for observations and wrong for facts with exactly one right
   answer. At OS spawn rates this matters more than it does for a dozen
   services. Note the mitigation, though: **instances don't fail over, so they
   have nothing to contend about.** Only the small set of long-lived services
   needs this solved — which makes it a bounded problem rather than a
   pervasive one.

---

## Part VI — What this buys you

- **The computer survives its own operating system crashing.** Structurally
  novel. Nothing traditional offers it.
- **Hardware failure stops being data loss.** A dead machine is a capacity
  event, not an emergency.
- **Incremental growth instead of replacement.** Adding a node makes your
  computer faster. There is no upgrade cycle and no migration day.
- **Genuine heterogeneity.** An ARM SBC and an x86 GPU box are one computer.
  Traditional operating systems cannot span instruction sets.
- **Isolation stronger than any single machine can offer.** Two applications on
  different physical nodes share no cache, no TLB, no DRAM. The entire class of
  cross-tenant side-channel attacks — Spectre, Meltdown, rowhammer — that has
  consumed a decade of hypervisor engineering simply does not exist across
  machines.
- **Placement becomes a security primitive.** "Never schedule this alongside
  anything untrusted" is expressible here and is not expressible on one
  machine, at any price.
- **Multiple simultaneous heads,** with no synchronization protocol.
- **The machine works while you are not at it.** The distinction between "my
  computer" and "my server" disappears.
- **The whole system is already reproducible.** Applications are declarative
  images; the state is small, versioned, and backed up. Rebuilding is not a
  weekend.

---

## Part VII — What it costs

Honestly, including what cannot be fixed.

**Structural — no amount of engineering removes these:**

- **No trusted display path.** The strongest compartmentalized OS designs rest
  on a small, air-gapped, local component that owns the screen and draws
  unforgeable trust indicators, so a hostile application cannot paint a
  convincing fake password prompt. When the display is on the far side of a
  network and rendered by a general-purpose client, that component cannot
  exist — the renderer is the thing you would have to trust. **This is a
  compartmentalized OS, not a verifiably compartmentalized one.** Fine for a
  personal cluster; not a substitute for a design where that property is the
  point.

- **Single-thread latency has a floor.** Nothing distributed will ever match a
  local memory bus. Applications that need one must run entirely on one node —
  which the architecture supports, but it means the cluster is not helping
  them.

- **RAM does not pool.** Aggregate capacity grows; maximum process size does
  not. The largest thing you can run is bounded by your largest single machine,
  forever.

- **Network dependency is total for anything remote.** Mitigated but not
  removed by making the terminal a real node — you degrade to one machine
  rather than to nothing, which is the difference between inconvenient and
  useless.

- **Partition splits your desktop.** Two halves that both keep accepting work
  and reconcile by last-write-wins. On a single machine this cannot happen.

**Practical — expensive but tractable:**

- **Every node is fully privileged today.** Each holds the credentials for the
  entire cluster, so compromising any one is compromising all of them. This is
  precisely backwards from the compartmentalization model being invoked, and it
  is the largest security gap in the concept.
- **Cold start is seconds, not microseconds.** No fine-grained processes. There
  is no `ls | grep` across the cluster; the unit of distribution is the
  application, and anything finer happens inside one.
- **Peripherals live at the edge, with the human,** and are mediated by client
  APIs you don't control and that vary by vendor. For latency-sensitive input —
  a gamepad — input latency stacks on top of frame latency.
- **Power and cost.** N machines idling versus one laptop sleeping. This is a
  desk computer, not a travel computer.
- **You are depending on a browser** as the hardware abstraction layer, and its
  capabilities are set by vendors with their own priorities.

---

## Part VIII — Direction

What makes sense:

| Decision | Verdict |
|---|---|
| Cluster hosts applications; browser is the head | **Yes.** The latency budget works. |
| Terminal is a full node, not a dumb client | **Yes.** Gives the local fast path and offline degradation for free. |
| Session in cluster state; shell as stateless projection | **Yes.** Multi-head and crash-survival fall out of it. |
| Two workload classes — services and instances | **Yes.** The prerequisite for everything else. |
| Control plane meshed; data plane direct | **Yes.** Decide this first; retrofitting is a rewrite. |
| Semantic remoting for most apps, pixel streaming for few | **Yes.** One path for both is the classic mistake. |
| Pin heavy, stateful applications | **Yes.** Solves latency and state migration with one decision. |
| Capability-based placement, not hostname pinning | **Yes.** Cheap now, expensive later. |
| A policy table for inter-process calls | **Yes.** This is what makes it an OS. |

What does not:

| Temptation | Verdict |
|---|---|
| Distributing a single application across nodes | **No.** Not an OS problem. Programs are not written to survive it. |
| Pooling RAM into one large virtual machine | **No.** Physically doesn't work; the model lies. |
| Making every window a gossiped cluster workload | **No.** Wrong lifetime class; the state layer will fight you. |
| One pixel-streaming path for everything | **No.** Makes a terminal feel like a game stream. |
| Aiming for verifiable compartmentalization | **No.** The trusted display path is not available. Don't sell a property you can't deliver. |
| Replacing the laptop | **No.** This is a desk computer. Accept it and the design gets simpler. |

The one-sentence version:

> **The cluster is the computer at the granularity of applications, the browser
> is the display, the session lives in replicated state, and the shell is a
> disposable projection of it.**

Every hard decision in this document is a consequence of that sentence and of
the partition rule in Part II.

---

## Part IX — What to analyze next in the code

This document deliberately avoided implementation. The audit that should follow
it needs answers to these, in roughly this order:

1. **Lifetime.** How deeply does "workloads are durable infrastructure" reach
   into the state layer? Is a second, non-gossiped class a new concept or a
   flag? This determines whether the whole idea is weeks or quarters.
2. **Placement.** Where is a node actually chosen, and is spawn-time placement
   a new code path or a generalization of the failover election?
3. **Addressing.** What allocates identity to a workload, and how much of the
   system assumes every workload has a cluster-unique address?
4. **Attach.** How general is the existing cross-node interactive-session
   plumbing, and does it carry arbitrary payloads or only terminal traffic?
5. **Identity.** How far does the single-credential assumption reach, and what
   would per-user scoping touch?
6. **Reconciliation.** Does a desired-state-versus-actual-state loop already
   exist in a reusable form, or is lifecycle imperative throughout?

Question 1 gates everything. Answer it first.
