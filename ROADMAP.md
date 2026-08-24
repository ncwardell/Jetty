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
- [ ] **Migrate the tunnel *receive* path to gVisor netstack.** Deletes the
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

      Ship behind `JETTY_TUNNEL_STACK=netstack|legacy` so there is an instant
      rollback that does not need a different binary.

### 1b. Correctness

- [ ] **🔴 Node removal is broken and cannot stick. Caused a production
      outage.** `DELETE /api/nodes/{id}` removes a peer from *everyone else's*
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

      The fix has three parts:
      1. **Peer tombstones** — `RemovedPeers` in cluster state, gossiped and
         version-merged like workload tombstones, so `NotifyJoin`,
         `peer-announce` and `sync` all refuse to resurrect a removed node.
         GC them, but on a much longer horizon than workload tombstones — a
         node can legitimately be powered off for weeks.
      2. **Cooperative leave** — tell the target it has been removed so it
         calls `memberlist.Leave()`, stops gossiping, and stops the workloads
         it no longer owns. Memberlist has no forcible eviction: the node has
         to leave itself, so removal is a conversation, not a unilateral write.
      3. **Key revocation** — invalidate its `APIKey` so it cannot rejoin
         without a fresh join token.

      Also: do not tear down tunnels/routes until the leave is acknowledged or
      the node is confirmed unreachable. Today the teardown happens first,
      unconditionally.

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

- [ ] **Wire up `JETTY_TUNNEL_HOST`.** The per-node subdomain is already read
      from the environment and stored on the agent, then never used anywhere.
      It is exactly the per-node addressing §4 needs, half-built.
- [ ] **WebSocket upgrade in `/api/proxy/`.** Currently `httpClient.Do()` with
      no hijack and no `websocket.Dialer`, so browser upgrade requests fail.
      The single concrete blocker for browser↔container streams.
- [ ] **Lift `bridgeWebSockets()` out of `handlers_terminal.go`.** It is already
      payload-agnostic — forwards message type and data verbatim, unbuffered.
      It just needs to stop living in the terminal handler.
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
