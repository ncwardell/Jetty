# Jetty Roadmap

A living document. The ordering is deliberate: each phase makes the next one
safe to attempt. Phase 0 exists because without a build gate, every later
change is unverifiable.

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done

---

## Guiding principles

**Separate observations from decisions.** Gossip is anti-entropy replication,
not consensus. It converges; it does not agree. Facts each node observes
independently (health, RTT, load, capability) are correct to gossip at any
scale. Decisions with exactly one right answer (who *owns* a workload, which IP
is allocated) need agreement — last-write-wins on those produces split-brain.

**Prefer delegation to agreement.** DNS scales to the entire internet with no
consensus protocol: it partitions authority so there is nothing to agree about.
Before making nodes agree on something, ask whether authority can be
partitioned instead.

**Third-party services are providers, not assumptions.** Cloudflare should stay
fully supported — it is genuinely excellent, and WARP solves NAT traversal for
free. The goal is that it is *selectable*, not *load-bearing*. The test:
can the agent boot and serve internal traffic with `JETTY_CF_TOKEN` unset?

**Don't reimplement transport protocols.** Every hand-rolled TCP feature is a
bug with a long tail.

---

## Phase 0 — Unblock and gate

Nothing else is safe until CI exists.

- [x] Fix `agent/tunnel.go` build break — declare `defaultPeerWindow` /
      `windowProbeInterval`, wire the four missing `closeFlow()` teardown calls
- [x] Fix `go vet` IPv6 findings — `net.JoinHostPort` instead of `"%s:%d"`
      before `net.Dial`
- [x] `gofmt` the tree (17 of 39 files were unformatted)
- [x] `.github/workflows/ci.yml` — gofmt, vet, build, `test -race -cover`,
      govulncheck, dashboard-embed drift check
- [x] `Makefile` so local `make check` is the same gate CI runs
- [x] `LICENSE` (MIT was claimed in `main.go` with no file present)
- [x] `.gitignore` scratch dirs
- [ ] Delete superseded branch `claude/debug-cloudflared-workload-5NRBz` — its
      only surviving idea already landed in `network.go` (idempotent, with a
      re-apply ticker), done better
- [ ] **Rotate leaked credentials** found in local session transcripts:
      Anthropic OAuth token, Cloudflare tunnel JWT, a plaintext password

---

## Phase 1 — Rock-solid primitives

### 1a. Routing (tactical, then strategic)

The userspace tunnel advertised a hardcoded `0xFFFF` window it could not
honour, ignored the peer's advertised window, and discarded incoming ACKs.
Bulk flows overran the receiver, the kernel dropped over-window segments,
nothing retransmitted, and the flow deadlocked permanently. Observed as CIFS
mount hangs and 10–15s stalls on ~21% of public requests.

- [x] Flow control (`initFlow` / `noteAck` / `awaitWindow` / `closeFlow`)
- [x] Unit tests for the flow-control state machine (window accounting,
      sequence wraparound, zero-window block/unblock, probe timer, teardown
      releases blocked senders). These four functions are at 100%.
- [ ] **Verify under load** — the unit tests prove the state machine, not the
      fix. Still needs: a sustained CIFS transfer, and a public request
      latency distribution with both nodes as Cloudflare tunnel connectors.
      This is the gate before production.
- [x] **Tunnel receive path now runs on gVisor netstack.** Deletes the
      hand-rolled TCP and its whole bug class — retransmission, RTO, congestion
      control, window probes — replacing it with a real, tested stack.

      Correcting an earlier overstatement: this does **not** remove the TUN
      device or the `NET_ADMIN` requirement. Those belong to the *send* path,
      where `tunReadLoop` captures kernel-routed packets off `jetty_tun`; that
      side is a pure packet forwarder with no TCP logic and is fine as-is. The
      hand-rolled TCP only ever existed on the receive side, which is exactly
      what netstack replaces.

      Feasibility confirmed: `gvisor.dev/gvisor@v0.0.0-20250428193742-2d800c3129d5`
      builds and runs on amd64 and cross-compiles to arm64 (~6MB). Pin that
      revision — tip has an upstream package conflict in `pkg/tcpip/stack`.
      The API surface needed (channel endpoint, `tcp.NewForwarder`,
      `udp.NewForwarder`, `gonet` adapters, `InjectInbound`/`ReadContext`)
      compiles against it.

      Preserve `getWorkloadContainerTargetForPort` — the mesh-IP:port →
      container-IP:port translation is real domain logic (asymmetric
      publications like `8222:80`, per-port container matching in multi-
      container stacks), not protocol code. Keep the local-workload-only
      destination check before injecting.

      Shipped behind `JETTY_TUNNEL_STACK`, defaulting to `netstack`; set it to
      `legacy` to roll back without a different binary. Tested by joining two
      netstacks over an in-memory wire, so a real handshake and an 8MB
      transfer run through the actual receive path (~0.12s). Binary 29M -> 33M.

      NOT yet verified against real hardware, real workloads, or a real
      WebSocket - see "Verify under load" above, which is still the gate.

### 1b. Correctness

- [x] **`stateMu` deadlock: fork/exec under the lock froze the whole control
      plane.** Almost certainly the cause of the "everything is up but nothing
      answers" incidents.

      `updateWorkloadRoutes` was documented *"caller must hold a.stateMu"* and
      shelled out — 3 `ip` calls plus 5 more per peer inside
      `ensurePeerTunnel`, so 3+5N unbounded fork/execs — from six call sites,
      five holding the **write** lock. `apiKeyMiddleware` takes
      `stateMu.RLock()` on **every** API request, and Go's RWMutex excludes
      new readers as soon as a writer is queued. One slow `ip` child therefore
      stopped every endpoint at once, including `/api/health` — so a wedged
      node could not report being wedged. The listener stays open because the
      kernel owns it, which is why it looks exactly like a broken tunnel from
      outside. There were **zero** `exec.CommandContext` calls in the package
      (107 `exec.Command`).

      The invariant: *never hold a lock across an operation of unbounded
      duration — I/O, network, or process execution.*

      Fixed by splitting reconciliation in three:
      1. Call sites now call `triggerRouteReconcile()`, a non-blocking
         capacity-1 send that is safe to make while holding `stateMu`.
      2. A single reconciler goroutine snapshots state under a brief read
         lock, releases it, and only then execs. Nothing that forks runs under
         `stateMu`.
      3. Reconciles **coalesce** rather than queue. Route reconciliation is
         idempotent and level-triggered, so a burst should produce one
         reconcile against final state — not N against N stale snapshots,
         which is what a plain `routesMu` would have given.

      All execs on this path are now bounded (`runBoundedCommand` /
      `runBoundedOutput`, 10s). Regression test injects a *deliberately slow*
      command and asserts an API-path read lock is still served — a fast-
      failing command cannot demonstrate the property, and an earlier version
      of the test passed against the reintroduced bug for exactly that reason.

- [ ] **`a.ip` is written without holding `stateMu`** (`warp.go:45,64,139`)
      while being read under it elsewhere — a genuine data race, pre-existing
      and unrelated to the deadlock above. `ensurePeerTunnel` reads it from
      five call sites. The clean fix is to pass the local WARP IP and tunnel
      mode in as parameters rather than re-reading agent fields, which also
      makes the dependency explicit. Deliberately not bundled into the
      deadlock fix.

- [x] **A wedged control plane can now say so.** Two subtractions rather than
      new machinery, which is why there is no watchdog goroutine:

      1. `apiKeyMiddleware` took `stateMu.RLock()` and scanned the whole peer
         table *before* checking whether the path needed auth at all - so
         `/api/health` depended on a lock it had no reason to touch, and
         discarded the result. The public-path check now comes first. Strictly
         less work per request.
      2. `GET /api/livez` reports liveness, uptime, goroutine count and
         whether `stateMu` is acquirable (`TryRLock`, non-blocking) - so it
         cannot get stuck on the lock it is reporting on. No background
         goroutine, no polling, no timer: it costs nothing until called.

      `/api/health` is not a liveness check - it takes `stateMu` twice. Use
      `/api/livez` when a node is unresponsive; `state_lock_acquirable=false`
      alongside a high goroutine count means wedged rather than busy, and it
      says so in a `diagnosis` field.

- [x] **`updateHosts` no longer holds a lock across file I/O.** Same shape as
      the route deadlock, one hop further round: it held `stateMu.RLock()`
      across `os.ReadFile`/`os.WriteFile` from 16 call sites. A read lock does
      not block readers directly, but a writer queuing behind it does, and
      Go's RWMutex then excludes every subsequent reader. Now snapshots, then
      does the I/O with nothing held.

      Fixing it exposed a live bug: the block was built by ranging over maps,
      and Go randomises map iteration order — so the rendered block differed
      on every call, its hash never matched, and the "skip the write if
      nothing changed" optimisation **never fired**. `/etc/hosts` was being
      rewritten on every gossip tick. Entries are now emitted in a stable
      order and the optimisation actually works.

- [ ] **Audit the remaining ~100 unbounded `exec.Command` sites.** The route
      path is bounded now, but `deploy.go`, `handlers_nodes.go` and others
      still fork with no deadline. Lower severity now that the worst of them
      are off the lock, but a hung `docker` child still stalls whatever loop
      it is on.

- [x] **Node removal now sticks, and no longer double-runs workloads.**
      (Caused a production outage.) `DELETE /api/nodes/{id}` removes a peer from *everyone else's*
      view and never tells the node itself. The removed node keeps running,
      stays on WARP, and stays an active memberlist member — so
      `NotifyJoin` (`memberlist.go:34`) and `apiPeerAnnounce`
      (`handlers_cluster.go:341`) both re-add it within one gossip round.
      There is no peer tombstone anywhere to stop them.

      Meanwhile the removal has already done destructive work on the way out:
      `removePeerTunnel` tears down the IPIP tunnel, `updateWorkloadRoutes`
      rewrites the route table, `updateHosts` rewrites `/etc/hosts` — then the
      node returns and it is all rebuilt. That flap is what bricked the
      cluster.

      Worse: in the window where the peer is absent from state, its workloads
      have an owner that is not in `Peers`, so they look orphaned and
      `checkFailover` claims them — while the removed node is still running
      them. **One click double-runs a workload.** The claim-settle does not
      help; the removed node never finds out it is contesting anything.

      Fixed with two things that have to happen together:
      1. **Peer tombstones** — `RemovedPeers` in cluster state, gossiped and
         version-merged like workload tombstones, carried on both the
         memberlist `LocalState` path and `/api/sync`. `NotifyJoin` and
         `peer-announce` consult them, so no peer can resurrect a removed
         node. They expire after 30 days rather than the 1 hour used for
         workload tombstones — a node can legitimately be powered off for
         weeks, but must not be stranded forever either.
      2. **Cooperative leave** — `POST /api/leave` tells the target it is out.
         It stops the workloads it owns *before* the cluster fails them over,
         leaves the gossip pool, and tombstones itself. Memberlist has no
         forcible eviction, so removal is a conversation, not an announcement.

      Teardown now happens after the leave is requested, and the response
      reports `leave_acknowledged` so an operator knows whether the node
      actually heard.

      **Leaving stops workloads, it never removes them.** The first version
      called `removeWorkload`, which runs `docker compose down -v` and would
      have destroyed every named volume on the node being removed. Being
      removed from a cluster must not mean losing your data - the operator may
      be decommissioning the node, or may have clicked the wrong row. Guarded
      by `TestLeaveNeverDestroysVolumes`.

      Two deliberate asymmetries: the tombstone does **not** gate `/api/join`
      (a fresh one-time token is an explicit decision to re-admit, and clears
      it), and a tombstone naming the local node is stored and propagated but
      never acted on (one stale gossip message must not evict a healthy node
      from its own view).

- [ ] **Revoke a removed node's API key.** Not done. A removed node keeps its
      `APIKey`, so the tombstone is the only thing keeping it out — good
      enough against gossip, but it should not still hold a valid cluster
      credential.

- [ ] **Removal is silent to a node that is off at the time.** It boots later,
      is refused by gossip, and needs a fresh join token with no indication
      why. Intended behaviour, but it will surprise someone — surface it in
      the dashboard's node list rather than leaving it to a log line.

- [x] **Join no longer silently provisions a blackhole node.** The Cloudflare
      Mesh migration broke the generated join command and nothing noticed:
      `buildJoinDockerRun` emitted `JETTY_JOIN` and `JETTY_JOIN_TOKEN` only,
      and `WARP_CONNECTOR` appeared zero times in the whole dashboard. Every
      node created that way took the shared-token fallback, and Mesh registers
      shared-token nodes as active-passive replicas of one identity — the
      passive ones drop all traffic while joining cleanly, gossiping, running
      workloads and showing green.

      Fixed: a per-node Mesh token field in the create-token modal wired into
      the generated command, a warning on the token-result screen when it is
      left blank, the shared-token fallback raised from `logInfof` to
      `logWarnf`, and the README's "That's it" claim removed — the WARP token
      handed out at join is the shared one, which was the problem.

- [ ] **Restore true one-paste join (optional).** The above makes the failure
      loud, not absent: provisioning a node is still two steps, because Jetty
      cannot mint a Mesh token — it identifies a specific machine and comes
      from the Cloudflare dashboard. Calling the Cloudflare API at
      token-generation time would restore the original property, at the cost
      of a stored Cloudflare API credential with Mesh scope. That cuts against
      "Cloudflare is a provider, not an assumption", so it is a deliberate
      decision rather than an obvious win.

- [x] **Per-node tunnel control.** `?scope=node` (default) vs `?scope=cluster`,
      plus `?node=<id|name>` targeting. `CFTunnelDisabled` is node-local and
      never broadcast; the guard lives in `startCloudflared` so the monitor
      loop and token syncs can't resurrect a disabled connector.
- [x] **Concurrent-claim safety.** `shouldClaim` runs a deterministic election
      (least-loaded, HWID tiebreak) that is safe from *identical* state, but it
      ranks by counts read from local state, so nodes whose maps have not
      converged can both claim. The claim is now announced before deploying,
      settles for 2s, and is re-checked — the loser never starts a container.
- [ ] **Ownership consensus (proper).** The above shrinks the window; it does
      not close it. Ownership is still last-write-wins, so a partition longer
      than the settle can still produce two live containers. The real fix is
      a small Raft group over placement decisions only (see the guiding
      principle on observations vs. decisions). Gated behind wanting a
      dependency; the settle buys time.
- [x] **Panic recovery.** `goSafe` (drop the unit of work) and `goSupervised`
      (restart with backoff, bounded) applied to every goroutine spawn, plus
      inline barriers on all eight memberlist delegate callbacks.
- [x] **Multi-node convergence test.** `convergence_test.go` runs 4-node
      clusters through 50 randomized operation/gossip interleavings plus a
      directional tombstone test. Verified by mutation: switching the merge to
      first-write-wins fails 37 of 50 seeds.
- [ ] Collapse the three overlapping sync paths (memberlist broadcast + 30s
      full sync + 10s HTTP pull) to one. Three paths that can disagree is not
      redundancy.
- [ ] **`NodeMeta` truncation corrupts gossip.** memberlist caps node metadata
      at 512 bytes; on overflow `jettyDelegate.NodeMeta` does
      `return d.meta[:limit]`, slicing JSON mid-string and publishing a payload
      every peer fails to parse — with only a log line to say so. It should
      drop optional fields to fit, or refuse to publish, rather than ship
      corrupt data. Latent today, and a live hazard the moment anything is
      added to `NodeMeta` (see JettyOS enablers).

### 1c. Test coverage

**21.0%** as of the panic-barrier work, up from 18.8%. The gaps are still the
dangerous subsystems:

| Area | Coverage | Note |
|---|---|---|
| `tunnel.go` | flow control 100%, rest ~0% | packet parsing still untested |
| `handlers_workloads.go` | 0% | 1,749 lines, the largest source file |
| `apiUpdateNode` | 0% | 350 lines — self-update; a bug here bricks a node |
| `memberlist.go` | 5.7% | 41 functions |
| `sync.go` | 26.2% | merge logic is the best-tested part of the repo |

---

## Phase 2 — Security

The existing posture is genuinely strong and should not regress: 108
`exec.Command` sites with **zero shell injection**, systematic input validation
applied on the gossip *ingest* path and not just the API, Argon2id secret
envelopes, constant-time key comparison, no secrets in logs with tests
enforcing it. Remaining gaps:

- [ ] **Fail-open auth.** With no keys configured, every route serves
      unauthenticated — an open, root-capable Docker orchestrator. Require an
      explicit `JETTY_ALLOW_INSECURE=true`.
- [ ] **No rate limiting anywhere**, including the public unauthenticated
      `/api/join`.
- [ ] **`?api_key=` query fallback** leaks bearer credentials into proxy logs,
      and combined with CORS reflecting any `Origin` with
      `Allow-Credentials: true`, is cross-origin exploitable. Restrict to the
      WebSocket endpoints that genuinely cannot set headers.
- [ ] **No TLS.** Plaintext admin keys on `:6880` with `--net host`. Either
      terminate TLS or document loudly that the port must never be reachable.
- [ ] Dependabot, Trivy image scan, pin base images by digest, drop `root` in
      the Dockerfile where possible
- [ ] Plan migration off `songgao/water` — unmaintained since March 2020 and
      load-bearing for the entire TUN path

---

## Phase 3 — Standardization

- [x] **JSON error envelope.** All 185 error sites now emit
      `{"error": "..."}` with `application/json`, standardized on the
      already-present `writeError`/`ErrorResponse` rather than a new shape.
      Status codes normalized to `net/http` constants. Guarded by
      `TestNoBareHTTPErrorCalls`. Dashboard gained `apiErrorMessage()` so
      envelopes don't surface as raw JSON in toasts.
- [ ] **Machine-readable error codes.** Deliberately deferred — codes invented
      without a consumer are usually the wrong codes. Adding a `code` field to
      the envelope is non-breaking, so do it when JettyOS or a client actually
      needs to branch on error type.
- [x] **Structured logging.** All 340 `log.Printf` calls migrated to leveled
      helpers behind `slog`. `JETTY_LOG_LEVEL` and `JETTY_LOG_FORMAT`
      (text/json); debug adds source positions. 215 info / 57 warn / 71 error.
      Guarded by `TestNoBareLogPrintf`.
- [ ] **Promote log messages to key/value attributes.** The migration kept
      messages printf-style deliberately — converting 340 of them is a per-site
      judgement call and would have made the level plumbing unreviewable. Do it
      incrementally, starting with the messages you actually grep for (workload
      name, peer, IP). Request IDs belong here too.
- [ ] **Error wrapping.** `fmt.Errorf` uses `%w` only ~45% of the time; zero
      `errors.Is` / `errors.As`; no sentinel or typed errors.
- [ ] **Collapse the duplicated dashboard.** `web-ui/index.html` and
      `agent/dashboard.html` are byte-identical 253KB files kept in sync by a
      `go:generate cp` that only runs inside `docker build` — a local `go build`
      embeds whatever happens to be on disk. `go:embed` cannot traverse `..`,
      which is why the copy exists. Fix: move the source to `agent/web/`, embed
      it directly, delete `web-ui/`. CI drift check is the stopgap until then.
- [x] **Swagger drift.** 32 → 38 documented paths; every registered route is
      now either in the spec or on an explicit not-API list. The internal
      node-to-node endpoints are documented under an `internal` tag rather than
      hidden — they're part of the surface an operator can hit and a peer must
      implement. `@host` dropped (was pinned to `localhost:6880`). Regeneration
      wired into `go:generate`; CI fails on any diff, and
      `TestEveryRegisteredRouteHasASwaggerAnnotation` catches the case
      regeneration can't — a handler with no annotation at all.
- [ ] **Docs gaps:** no `ARCHITECTURE.md`, no `CONTRIBUTING.md`, no development
      setup, no release process, no troubleshooting guide. `LOGIC.md` is stale.
      `docs/networking.md` is accurate and thorough — use it as the model.
- [ ] **Decompose the big files:** `handlers_workloads.go` (1,749 lines, 14
      handlers), `apiUpdateNode` (350 lines inline in one handler). Move
      `newClusterTransport` (164 lines) and `tunnelConn.WriteMessage` (191
      lines) out of `types.go` — ~355 of its 460 lines are behaviour, not types.
- [ ] Config struct with startup validation; document all ~20 `JETTY_*` vars in
      one place (they are currently split between Go and `docker-entrypoint.sh`)

---

## Phase 4 — Release discipline

The repo has **zero git tags**. The release workflow's only automatic trigger
is `push: tags: v*`, so it has never fired — every published image came from a
manual dispatch defaulting to version `dev`. Meanwhile `agent.go` hardcodes
`2.0.0` and `main.go` declares `2.0`.

- [ ] Tag a real release; single-source the version
- [ ] `release-please` or `git-cliff` — commits already follow Conventional
      Commits, so a changelog and GitHub Releases are nearly free
- [ ] Re-enable SBOM and provenance (currently `provenance: false`)
- [ ] Post-build job that pulls each arch image and asserts the binary
      architecture matches the manifest — this is the exact regression an
      earlier commit fixed, still untested

---

## Phase 5 — Product & UX

- [ ] Vendor xterm.js — it loads from a CDN, so offline/airgapped clusters get
      a broken terminal
- [ ] Real UI error states (needs the Phase 3 error envelope first)
- [ ] Frontend tests — there are currently none
- [ ] `Workload.Hostname` in cluster state. Route config currently lives in the
      Cloudflare dashboard: data you don't own, can't back up, can't gossip,
      can't test. Cheap now, expensive once hostnames accumulate.
- [ ] Entry-node role: any publicly reachable node terminates TLS, reads
      SNI/Host, looks up the gossiped route table, and dials the workload over
      the mesh — the workload need not be local. Reachability must be *probed*
      by a peer, not inferred from having a public IP (CGNAT lies).
- [ ] DNS resolver per node, served from gossiped state, replacing
      `/etc/hosts` + `extra_hosts`. Today adding a workload anywhere requires
      container recreation elsewhere to make it resolvable. Split-horizon also
      fixes hairpinning on public names.
- [ ] RTT on `Peer` as an EWMA — `checkPeers` already samples it every 5s and
      throws it away. Unlocks replica selection, relay selection, and geo
      policy. Latency beats GeoIP; geography is a lossy proxy for the thing you
      actually care about.
- [ ] Workload replicas (`Owner string` → `Owners []string`). Note this gates
      geo-routing entirely: routing policy needs a choice to make, and today
      every mesh IP has exactly one correct destination.

---

## JettyOS enablers

Prerequisites in Jetty for the desktop project (`docs/JETTYOS.md`). Listed
separately because the sequencing is driven by that project — but note how much
of it was already on this roadmap for unrelated reasons. That overlap is the
main finding: JettyOS mostly needs Jetty to be *finished*, not extended.

**Already listed above, and load-bearing for JettyOS:**

| Item | Phase | Why JettyOS needs it |
|---|---|---|
| Entry-node role + `Workload.Hostname` | 5 | §4 requires the browser to reach a *specific* node directly, not whichever one the tunnel picks |
| DNS resolver replacing `extra_hosts` | 5 | Today adding a workload forces container recreation elsewhere — fatal for windows opened 40× an hour |
| RTT on `Peer` | 5 | Placement input; §8's thin node wants latency-aware scheduling |
| JSON error envelope | 3 | JettyOS is an API client; `text/plain` errors with no code contract are unworkable to program against |

**New, and specific to JettyOS:**

- [x] **Wire up `JETTY_TUNNEL_HOST`.** ~~The per-node subdomain is already read
      from the environment and stored on the agent, then never used anywhere.~~
      Done. `Peer.TunnelHost` now rides `NodeMeta` and `peerWire`, is ingested in
      `NotifyJoin`/`NotifyUpdate`, and `getPeerAPIURL` prefers it over the
      cluster-wide domain. The bug this closed: before WARP attaches, the
      fallback used the *shared* domain, which Cloudflare resolves to an
      arbitrary node — so `rotate-key`, `leave` and `move`, all of which mutate
      peer-specific state, could land on the wrong node. Validated harder than
      `Version`/`Arch` because it becomes the authority of an authenticated URL.

      **Dormant until an operator sets it.** Jetty provisions no DNS — there is
      no Cloudflare API call in the agent — so a per-node hostname means
      manually creating a CNAME per node pointing at that node's
      `<uuid>.cfargotunnel.com`. And unlike `TunnelDomain`, there is no
      join-time adoption of `TunnelHost`, so it is empty on every node until
      set per node. The code path is correct; it does not fire yet.
- [ ] **Adopt or provision per-node tunnel hostnames.** Follows directly from
      the above: a config value that must be set by hand on every node, with no
      error when unset, is a feature that will silently not exist. Either
      provision the DNS record via the Cloudflare API at bootstrap/join, or at
      minimum surface "no per-node hostname configured" somewhere an operator
      will see it.
- [ ] **WebSocket upgrade in `/api/proxy/`.** Currently `httpClient.Do()` with
      no hijack and no `websocket.Dialer`, so browser upgrade requests fail.
      The single concrete blocker for browser↔container streams.
- [x] **Lift `bridgeWebSockets()` out of `handlers_terminal.go`.** Done — now
      `agent/wsbridge.go`. Pure code motion; it forwards message type and data
      verbatim and inspects neither, so it was never terminal-specific. Picked
      up its first tests on the way, including the goroutine-leak property
      (both conns must close before the second wait, or one pump leaks per
      proxied session).
- [ ] **Extract `rankCandidates()` from `shouldClaim`, add launch-time
      placement.** Worth doing regardless of JettyOS: today a workload lands on
      whichever node received the POST, and the not-allowed fallback
      (`findAllowedNode`) returns the first match in *Go map iteration order*.
      Non-deterministic placement is a bug on its own.
- [ ] **Node capability labels** (`gpu`, `nvme`, `bigmem`) in `NodeMeta`, so a
      workload can require a capability instead of naming a hostname. Cheap
      now, annoying once a catalog is full of hostnames.
- [ ] **Gossip node resource metrics on a channel that is not `NodeMeta`.**
      Memory/CPU/disk are already sampled and thrown away, but memberlist caps
      node metadata at 512 bytes and the current payload already spends a few
      hundred. See the `NodeMeta` truncation bug below — that must be fixed
      first either way.
- [ ] **Ephemeral workload class as a separate map**, not a flag on `Workload`.
      `state.Workloads` is keyed by mesh IP, and ~11 subsystems iterate it
      unconditionally; a flag needs a guard in every one of them and in every
      future feature. A separate map keyed by window/session ID, never
      persisted or gossiped, needs zero changes to any of them.

## Worker nodes (ephemeral, untrusted capacity)

Rent a box, have it run workloads, let it vanish whenever it likes. It never
holds the admin key, never joins the WARP mesh, and everything it touched is
gone when it goes.

This is the largest single item on the roadmap and it is worth being precise
about why: **Jetty currently has exactly one trust tier.** `/api/join` hands the
joiner the complete state — every workload, the whole env store, the admin key,
and every peer's `APIKey`. Being on the mesh *is* the authentication, and
`MergeRemoteState` merges whatever a peer sends. There is no smaller thing a
node can be. So this is not a flag; it is a second kind of participant.

Naming: **worker**, not "unprivileged". The distinction is role, not permission
level — a worker is a node that executes but does not decide, which is the same
delegation-over-agreement principle at the top of this file.

**The six decisions that shape everything else:**

1. **A worker is not a gossip peer.** It does not run memberlist. Filtering an
   untrusted peer's merges is the hard, easy-to-get-wrong version of this
   problem: today a peer can gossip "I own vaultwarden now" or "these workloads
   are deleted" and every node takes it. A worker is pushed work by its sponsor
   and reports status back — a much smaller surface, and it matches what the
   feature actually is.

2. **Control plane and data plane are separate. Only the control plane is
   hub-and-spoke, and even that hub is incidental.**

   *Control:* the worker holds one WebSocket to one node, dialled outbound.
   Placements down, status up. That node is its sponsor.

   It can attach via the **cluster-wide** domain and simply take whichever node
   answers — ambiguous routing is a feature here, because sponsorship is
   incidental. That matters practically: per-node hostnames are not something
   Jetty provisions. There is no Cloudflare API call anywhere in the agent;
   both `JETTY_TUNNEL_DOMAIN` and `JETTY_TUNNEL_HOST` are operator-supplied
   env vars, and pointing a hostname at one specific node means manually
   creating a CNAME per node to that node's `<uuid>.cfargotunnel.com`. Making
   attach depend on that would put a manual DNS step in front of "rent a box".

   *Data:* the worker opens **direct** tunnels to whichever nodes it actually
   needs to exchange traffic with, dialled outbound, on demand. Workload
   traffic never transits the sponsor. An earlier draft of this section had
   the sponsor relaying, which made it both a bottleneck and a SPOF; that was
   a mistake born of assuming "dials out" meant "dials one".

   Because every connection is outbound, **nothing ever has to reach in** — no
   DNS record for the worker, no inbound port, no NAT traversal, no mesh
   membership. That is what makes "doesn't touch the public WARP" true by
   construction rather than by policy.

   **Sponsorship is incidental and movable.** A worker's placements live in
   ordinary cluster state (owner = worker ID, gossiped among the real nodes),
   and every node has the env store, so any node can mint that worker's
   envelopes. The sponsor therefore holds no unique state — it is just
   whichever node currently has the socket. If it dies, the worker re-attaches
   elsewhere with the same token and the new sponsor reconciles from state it
   already has. A worker costs its sponsor one socket, so capacity spreads by
   construction: rent twenty boxes and they attach wherever they land.

   This also beats giving each worker its own Cloudflare connector token, and
   for a reason worth recording: **a node terminating its own tunnel can
   filter.** It sees every packet, so it can enforce that a worker only
   sources from the mesh IPs of workloads actually placed on it and only
   reaches IPs those placements are permitted to reach. On the WARP mesh the
   worker can address everything and enforcement is Cloudflare's, at
   Cloudflare's granularity. Owning the tunnel is what makes least-privilege
   possible at all.

3. **The socket is the lease. Expiry is optional.** A worker usually knows
   better than the cluster when it is going away, so an open-ended attachment
   is the default: it stays until it disconnects. A deadline is an *optional*
   upper bound for when a hard stop is genuinely wanted, not the mechanism.

   Disconnect therefore has to be the primary lifecycle signal, which needs a
   **grace window** before teardown — otherwise a three-second blip tears down
   and re-places everything. Same shape as `FailoverClaimSettle`. And the
   credential must survive reconnect inside that window, so it is not strictly
   single-use; the tombstone lands only once the window closes.

4. **The sponsor mints per-placement permission; the worker never sees state.**
   This replaces filtering the join response, because there *is* no join
   response. A worker receives a placement envelope per workload — the compose
   file plus exactly the env keys that placement references — and its total
   knowledge is the union of what it has been handed. A field added to `State`
   later cannot leak, because `State` is never sent. That is a much stronger
   property than an allowlist somebody has to remember to maintain.

5. **Workers live in `state.Workers`, a separate gossiped map — not in
   `state.Peers` with a role flag.** Same argument as the ephemeral-workload
   map above: `state.Peers` is iterated unconditionally by failover, route
   reconciliation, `/etc/hosts`, health checks, tunnel-mesh building, the
   dashboard and memberlist sync. A flag means auditing every one of those and
   guarding it, and makes every *future* iteration site a latent bug where an
   untrusted rented box is silently treated as a full peer. A separate map is
   opt-in by construction. The lifecycles differ anyway: shorter tombstone TTL,
   different health semantics, no `APIKey`.

   The worker's *workloads* need nothing new — they are ordinary
   `state.Workloads` entries with `Owner` set to the worker ID, and they gossip
   as they always have.

   **What the worker record is for is setup, not routing.** Routing does not
   need it: each node has its own direct tunnel and sends the worker's workload
   IPs down that. But because the worker only ever dials *out*, a node with no
   tunnel yet cannot initiate one — it has to ask whoever holds the control
   socket to push a "dial me" down it. So the record carries `SponsorID`: not a
   path, but who to ask. The sponsor is a bottleneck for connection setup only,
   which is rare, small, and off the data path.

   **The worker is authoritative about its own attachment.** It picks a
   sponsor and the receiving node publishes the fact; the cluster never assigns
   one, because only the worker knows whether a socket is actually up. During a
   partition two nodes can both truthfully believe they sponsor the same
   worker — that is an *observation*, not a decision, so do not make the nodes
   agree. The worker increments an **attach epoch** on each attach, the record
   carries it, highest epoch wins.

   Falls out of this: **re-attachment never disturbs traffic.** Existing direct
   tunnels stay up while the control socket moves, so the control plane can
   flap without touching the data plane.

6. **Worker state is memory-only.** Cluster state never gets written to a rented
   disk, so there is nothing to wipe and nothing left behind if the box is
   imaged after you release it.

**The honest limitation, stated up front:** a worker running a workload needs
that workload's env vars. You cannot run vaultwarden without its secrets. Per-
placement minting bounds the blast radius to exactly what was placed there —
no admin key, no env store at large, nothing enumerable, no gossip authority —
but it is not zero. **Do not place a secret-bearing workload on a box you don't
trust.** The workloads this is for are stateless compute: batch jobs, builds,
transcode, scrapes. The UI should say so at placement time, not bury it in docs.

**Stages.** Each stands alone and lands in order:

- [ ] **A. `Peer.Role` + receive-side enforcement.** The load-bearing one, and
      valuable on its own regardless of the rest: "this peer may not mutate
      cluster state" is an invariant worth having. Enforcement belongs on the
      *receiving* side of every path a peer can write through —
      `MergeRemoteState`, `apiPeerAnnounce`, broadcast handling, the workload
      and key-rotation endpoints. Default-deny for unknown roles, so a future
      role cannot silently inherit full rights.
- [ ] **B. Attach: worker tokens and the control socket.** A second token type
      granting worker role only, and `/api/worker/attach` — a long-lived
      WebSocket carrying placements down and status up. The worker is handed
      one or more node hostnames and attaches to whichever answers; that node
      becomes its sponsor. Reconnect with backoff, and **re-attach to a
      different node if the current one goes away** — the token is not bound to
      a node, because nothing about sponsorship is. No join response, no state
      transfer.
- [ ] **C. Placement envelopes.** The sponsor mints, per placement, a bundle of
      compose file + exactly the env keys that compose file references. Nothing
      else crosses. Build the reference-extraction carefully: the failure mode
      is silently shipping the whole env store because a `${VAR}` form was not
      recognised, and that failure is invisible until someone audits it. A test
      that asserts the envelope contains *only* the expected keys, not merely
      that it contains them, is the one that matters here.
- [ ] **D. Detach lifecycle.** Disconnect starts a grace window; reconnect
      inside it resumes; expiry of the window drains placements, tombstones the
      worker, and invalidates the token. An *optional* deadline layers on top
      as a hard stop. Reuses the `RemovedPeer` machinery, but
      `RemovedPeerMaxAge` (30 days) is wrong here — a worker ID never recurs,
      so its tombstone can be short. Keep planned release and disconnect as
      distinct paths: one drains cleanly, the other is failover.
- [ ] **E. Direct data tunnels + per-worker packet filtering.** The worker
      dials a tunnel to a node the first time it has traffic for it, using the
      reachability the sponsor supplied — which should cover only the nodes it
      genuinely needs, not the full topology. Nodes route the worker's workload
      IPs down that tunnel directly, so this stays inside the existing
      owner-is-next-hop model and no relay is involved.

      The filtering is the point, not a nicety: the terminating node drops any
      packet whose source is not a mesh IP placed on that worker, or whose
      destination that worker's placements are not permitted to reach. Do it
      here, while the tunnel is new — retrofitting enforcement onto an
      already-trusted path is how it ends up never happening.

      **Connection count — a worker costs more than a peer, not less.** An
      earlier draft claimed the opposite by miscounting what a peer's tunnels
      are. `ensurePeerTunnel` runs `ip tunnel add ... mode ipip`: a *stateless
      encapsulation interface*, no socket, no handshake, no keepalive. So a
      peer holds exactly **one** real connection — WARP WireGuard to the
      Cloudflare edge — plus N cheap encaps, and node-to-node packets go
      node → edge → node. The edge is the rendezvous, which is what makes that
      one connection independent of cluster size.

      A worker has no edge to rendezvous through, so every path is its own real
      socket: 1 control + N data. That is the price of being off-mesh, and it
      is the *same* property that makes filtering possible — we terminate the
      tunnel, so we can inspect it.

      Which makes the connector-token question a genuine trade, not a settled
      one. On WARP: one connection, but the worker can address the whole mesh
      and enforcement is Cloudflare's, at Cloudflare's granularity. Off WARP: N
      connections, but per-placement filtering is ours to enforce. For a small
      cluster N is trivial and off-WARP wins. At many workers × many nodes,
      N×W real sockets is a real cost and this should be revisited rather than
      assumed.

      **NAT is the open problem here, and it is not hypothetical.** The worker
      dials node B directly, which requires B to be reachable. The Radxa is
      behind home NAT with no public address and cannot be dialled at all. So:
      direct where the node is reachable, relay via a reachable node where it
      is not. Much narrower than relaying everything through the sponsor, but
      not nothing — and it means the reachability list the sponsor supplies has
      to say *how* to reach each node, not merely which ones exist.
- [ ] **F. Placement gating and worker-loss policy.** Two directions, and the
      inbound one is a trap verified in the current code, not a hypothetical.

      *Workloads must not land on a worker by accident.* Workers are ineligible
      as failover targets by default — a permanent node dying and its workload
      landing on a box that may leave in minutes is worse than the outage.
      Opt-in per workload; `isNodeAllowed` is the existing hook.

      *Workloads placed on a worker must not be stolen back.* `checkFailover`
      resolves owner health through `isPeerHealthyInMemberlist`, which walks
      `memberlist.Members()`. A worker is never in that set — it does not
      gossip, by design — so its ID reads as **dead permanently, from the
      moment of placement**. Any worker placement with `Revive: true` is
      claimed by a permanent node on the next scan. Not on worker loss:
      immediately, and repeatedly. **This has to land in the same change as
      placement itself**, or the first worker placement is stolen before it
      starts running.

      *On genuine worker loss, default to drop, not reschedule.* A workload is
      put on rented capacity deliberately; silently relocating it to permanent
      nodes when that capacity vanishes is the exact surprise renting was meant
      to avoid — "the rented box died and now the Radxa is pegged". Dropping
      fails visibly instead, which is what ephemeral capacity already implies.
      Make it explicit per placement (`on_worker_loss: drop | reschedule`),
      reschedule opt-in. The workload record stays in state marked *unplaced*
      rather than being deleted: losing the capacity and discarding the intent
      are different things.

      Note this is **derived, not decided** — any node can compute "worker past
      its grace window ⇒ its placements are unplaced" from state it already
      holds. No claim race, no agreement, and no dependency on the sponsor
      surviving.

      Also refuse (or loudly warn on) workloads with named volumes —
      `docker compose down -v` on detach is correct here and destructive
      everywhere else, and that hazard has already bitten once.
- [ ] **G. Rent flow in the UI.** Issue token → copy one command → see the
      worker attach, with its placements and whether a deadline is set →
      release on demand. Attachment state is the product; a worker you cannot
      see attached or gone is one you will be surprised by.

Start at A. It is the only stage the others cannot be built without, and it
closes a real gap today: any peer on the mesh can currently rewrite cluster
state by gossiping it.

## Known scaling limits

Not urgent, but these are the walls, in the order they arrive (~50–100 nodes):

1. **O(n²) tunnel mesh** — every node builds a tunnel to every healthy peer
2. **Full-state sync** — the 30s sync ships the entire workload set, no deltas
3. **`/etc/hosts`** — contains every workload in the cluster, on every node

Gossip itself is not the bottleneck; SWIM handles 10k-node pools comfortably.

---

## Deliberately not doing (yet)

- **Replacing WARP.** It solves NAT traversal, stable addressing, and transport
  encryption in one dependency. Replacing it means STUN + UDP hole punching
  (~85–90% of NAT pairs; symmetric-to-symmetric essentially never) plus a relay
  fallback for the rest, plus Noise for encryption. Real work with a real payoff
  — but Phase 1 correctness matters more.
- **Crypto-addressed IPv6** (`address = hash(pubkey)`). Removes the allocation
  authority, makes source addresses unspoofable, and is the prerequisite for
  cross-cluster peering. Requires IPv6 — 16 bits of host space in a `/16` hits
  birthday collisions around 256 workloads. Big change, do it last.
- **Replicated storage.** Failover moves ownership, not volumes, so "no
  downtime" currently holds only for stateless workloads. This is a second
  product of comparable difficulty and needs a deliberate decision, not drift.
