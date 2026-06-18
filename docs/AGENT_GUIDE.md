# Jetty — Operating Guide for AI Agents (Claude)

> Audience: a Claude instance (or any LLM agent) that needs to **drive a Jetty
> cluster over its HTTP API** — deploy workloads, including private GitHub
> container images, manage nodes, secrets, and tunnels. This document is
> self-contained and captures the nuances that aren't obvious from the API
> shapes alone. When in doubt, the live OpenAPI spec is at `/swagger/` on any
> node and the source of truth is `agent/*.go`.

---

## 1. Mental model (read this first)

Jetty is a **captain-less, peer-to-peer Docker Compose orchestrator** with a
**Cloudflare WARP mesh** for private networking. Internalize these facts before
issuing any command:

- **Every node is equal and runs the same agent.** There is no leader. Any node
  can accept any API request. If the receiving node can't satisfy it locally, it
  **proxies** to the node that can. So you can point all your calls at a single
  node's API and it Just Works.
- **A "workload" is a Docker Compose app** plus metadata (mesh IP, allowed
  nodes, failover policy). You submit the compose YAML *as a string* in JSON.
- **Each workload gets a private mesh IP** from the service CIDR (default
  `10.100.0.0/16`, e.g. `10.100.0.50`) and a **DNS name** equal to its workload
  name. Workloads reach each other by name. These IPs are reachable **only over
  the WARP mesh** — never publicly. (See §9 for why.)
- **State is gossiped.** Writes fan out to all nodes via memberlist in
  sub-second time, with a 30s full-sync poll as backstop. Workloads, peers, and
  encrypted env vars all replicate cluster-wide automatically.
- **Ownership & failover.** Exactly one node "owns" (runs) a workload at a time
  (`owner` = node hardware ID). If the owner dies and the workload has
  `revive: true`, a surviving allowed node claims and redeploys it.

---

## 2. Connecting & authentication

**Base URL:** `http://<node-ip>:6880` (default port). Over a Cloudflare tunnel
it's `https://<your-tunnel-domain>`. The dashboard is at `/`, Swagger UI at
`/swagger/`.

**Auth:** send the API key as **either** an `X-API-Key` header **or** an
`?api_key=` query param. There are three kinds of key the server accepts:

| Key | What it is | Use it for |
|-----|-----------|------------|
| **AdminKey** | Cluster-wide operator/dashboard key. Bootstrapped from `JETTY_SECRET` on the first node, then replicated to every node. | Everything. This is the key you (the agent) should use. |
| **SelfAPIKey** | A node's own self-call key. | Internal; you won't normally use it. |
| **Peer APIKey** | Per-peer key generated at join. | Node-to-node calls; not for you. |

```bash
export JETTY=http://192.168.2.91:6880
export KEY=your-admin-key       # the JETTY_SECRET value
curl -s -H "X-API-Key: $KEY" $JETTY/api/status | jq
```

**Unauthenticated (public) endpoints** — no key needed:
`GET /api/health`, `POST /api/join` (token-gated in body), `/swagger/`, and `/`
(dashboard). Everything else requires a valid key.

**Admin-only endpoints** — require the **AdminKey specifically** (peer keys are
rejected, even though they pass normal auth):
- `POST /api/tokens`, `GET /api/tokens`, `DELETE /api/tokens/{id}` (mint/manage join tokens)
- `POST /api/nodes/{id}/update` (pull+restart a node — effectively RCE)
- `POST /api/host/exec`, `/api/host/shell` (run commands on the host)
- `POST /api/admin-key/rotate`, `POST /api/peers/{id}/rotate-key`

**Bootstrap quirk:** on a brand-new first node where no AdminKey and no peer
keys exist yet, *all* routes are temporarily unauthenticated (the agent logs a
warning). As soon as `JETTY_SECRET` is set / a key exists, auth is enforced.

---

## 3. Deploying a workload (the core task)

One `POST /api/workloads`. You can send it to **any** node — it self-routes to
an allowed node.

```bash
curl -X POST $JETTY/api/workloads \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{
    "name": "whoami",
    "revive": true,
    "autostart": true,
    "compose": "services:\n  whoami:\n    image: traefik/whoami\n    ports:\n      - \"80:80\""
  }'
```

What happens, in order (`agent/deploy.go` `deployWorkload`):
1. Picks the compose for the node's architecture (see §8).
2. Writes `docker-compose.yml` + generates `docker-compose.override.yml` with
   `extra_hosts` so this workload can resolve **every other** workload by name.
3. `docker compose config` validate → **pull** (3 tries: 0s/2s/4s backoff) →
   `up -d --remove-orphans`.
4. Binds the mesh IP and installs per-port DNAT rules (see §9).

### The Workload schema — every field

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `name` | string | ✅ | DNS hostname. Pattern `^[a-zA-Z0-9_-]+$`. Must be unique. |
| `compose` | string | ✅* | Default Compose YAML (as a string, `\n`-escaped in JSON). |
| `compose_amd64` | string | ✅* | Optional amd64-specific compose; wins on amd64 nodes. |
| `compose_arm64` | string | ✅* | Optional arm64-specific compose; wins on arm64 nodes. |
| `ip` | string | ❌ | Mesh IP. **Auto-allocated from the service CIDR if omitted** (recommended: omit it). Must be inside the CIDR and free. |
| `revive` | bool | ❌ | `true` → fail over to another allowed node if the owner dies. |
| `autostart` | bool | ❌ | `true` → (re)deploy on agent startup; reconciled continuously. |
| `allowed_nodes` | []string | ❌ | Whitelist of node IDs **or** names. Empty or `["*"]` = any node. Pin GPU/region work here. |
| `tags` | []string | ❌ | Lowercase `[a-z0-9][a-z0-9_:-]{0,62}`, e.g. `env:prod`. For bulk ops. Sorted/deduped on ingest. |
| `registry_auth` | object | ❌ | Private-registry credentials **by reference**. See §4. |
| `owner` | string | server-set | Don't send. The owning node's hardware ID. |
| `version` | int64 | server-set | Don't send. Unix-nano timestamp; higher wins in conflicts. |

`*` At least one of `compose` / `compose_amd64` / `compose_arm64` is required.

**Nuances:**
- Submit the compose **as a JSON string**. Newlines are `\n`. The whole compose
  file goes in one field.
- If you omit `ip`, Jetty allocates one and returns it. Prefer this.
- `allowed_nodes` accepts node **names or IDs** interchangeably.
- Reference other workloads by their `name` in compose (internal DNS). No DNS
  server needed — Jetty injects `extra_hosts`.

---

## 4. Private / authenticated registry images (e.g. private GHCR)

**Key fact: registry auth is per-registry-host, not per-repo.** One GitHub token
with `read:packages` authenticates pulls for **every** private repo in that
account/org. So "many private repos" usually means **one credential**.

Jetty handles this with **two decoupled pieces** — the secret value lives in the
encrypted cluster secret store; the workload only carries a *reference* to it.
The token is **never** stored in the workload record (workloads sync in the clear
and render in the dashboard; only env secrets are encrypted).

### Step 1 — store the token once, in the encrypted env store

```bash
curl -X POST $JETTY/api/env \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"env": {"GHCR_FOO": "ghp_xxxxxxxxxxxxxxxx"}}'
```

This is AES-256-GCM encrypted at rest and gossiped to **every** node (so pulls,
pre-pulls, and failovers on any allowed node can authenticate — no per-node
setup). Env keys must match `^[a-zA-Z_][a-zA-Z0-9_]*$`.

### Step 2 — reference it from the workload via `registry_auth`

```bash
curl -X POST $JETTY/api/workloads \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{
    "name": "app-a",
    "revive": true,
    "autostart": true,
    "registry_auth": { "registry": "ghcr.io", "username": "foo-bot", "token_ref": "GHCR_FOO" },
    "compose": "services:\n  app:\n    image: ghcr.io/org-foo/app:latest"
  }'
```

`registry_auth` fields:

| Field | Required | Notes |
|-------|----------|-------|
| `registry` | ✅ | Registry **host** only, e.g. `ghcr.io`, `registry.gitlab.com`. No slash/space. |
| `token_ref` | ✅ | Name of the env-store key holding the token (e.g. `GHCR_FOO`). Validated as an env key. |
| `username` | ❌ | Registry username. **Defaults to `x-access-token`** (GitHub's PAT convention) when omitted — so for GHCR you often only need `registry` + `token_ref`. |

### How it works under the hood (`agent/deploy.go`)
At pull time, Jetty resolves `token_ref` from the decrypted env store, writes a
**throwaway, per-workload `DOCKER_CONFIG`** dir (`config.json`, mode `0600`,
deleted right after the command), and points the `docker compose pull/up` at it.
Because each workload gets its **own** docker config, two workloads can pull from
the **same** registry host under **different** accounts without colliding — which
a single shared `~/.docker/config.json` fundamentally cannot do.

### Multiple GitHub accounts / orgs
Store one env key per account (`GHCR_FOO`, `GHCR_BAR`, …) and point each
workload's `token_ref` at the right one. That's the whole pattern.

### ⚠️ Gotcha — pull failures are quiet
`pullWithRetry` logs `"will rely on cached image"` and does **not** fail the
deploy on a pull error (transient registry blips are common). If your
`token_ref` is missing/expired, the auth failure won't surface there — the deploy
fails later at `up` with an image-not-found-style error. **If a private workload
won't start, check the owner node's logs for the pull line first**, and verify
the env key exists (`GET /api/env`) and the token is valid.

### Default behavior is unchanged
A workload with **no** `registry_auth` pulls exactly as before (public images, or
whatever the host's docker config already provides). `registry_auth` is purely
additive.

---

## 5. Encrypted secrets / env vars (`/api/env`)

Values are AES-256-GCM encrypted at rest and synced cluster-wide. Two distinct
uses:
1. **Image-pull auth** — referenced by `registry_auth.token_ref` (§4).
2. **Runtime env in containers** — compose variable substitution. A `${MY_VAR}`
   in your compose YAML is filled from the env store at deploy time.

| Method | Path | Body / Notes | Response |
|--------|------|--------------|----------|
| `GET` | `/api/env` | — | `{"keys":[...],"count":N}` — **keys only, never values**. |
| `POST` | `/api/env` | `{"env":{"K":"v",...}}` (batch) | `{"added":[...],"updated":[...]}` |
| `GET` | `/api/env/{key}` | — | `{"key":"...","value":"..."}` (decrypted) |
| `DELETE` | `/api/env/{key}` | — | `204` (writes a tombstone that propagates) |

---

## 6. Full endpoint reference

All responses are JSON unless noted. `{name}` is a workload name; `{id}` a node
ID or name.

### Workloads — CRUD
| Method | Path | Purpose / nuance |
|--------|------|------------------|
| `GET` | `/api/workloads` | List all workloads (cluster-wide). Includes `containers[]` for remote workloads. |
| `POST` | `/api/workloads` | Create + deploy (§3). `?move=true` allows IP overlap during blue-green. Self-routes to an allowed node. |
| `GET` | `/api/workloads/{name}` | Details (proxies to owner if remote). |
| `PATCH` | `/api/workloads/{name}` | Update fields. Send only what changes. **Compose/IP/`registry_auth` changes trigger an in-place `up -d` redeploy** (volumes preserved — it does *not* `down -v`). `registry_auth: null` clears it. |
| `DELETE` | `/api/workloads/{name}` | Stop + remove + cleanup. `?move=true` suppresses the tombstone (used by move). |

### Workloads — lifecycle
| Method | Path | Purpose / nuance |
|--------|------|------------------|
| `POST` | `/api/workloads/{name}/start` | `docker compose start` + re-setup mesh routing. Proxies to owner if remote. |
| `POST` | `/api/workloads/{name}/stop` | Cleanup DNAT first (needs container IP), then `stop`. |
| `POST` | `/api/workloads/{name}/restart` | DNAT cleanup → optional re-pull → `up -d --force-recreate` → re-route. **Use after a PATCH to pick up a new image.** |
| `POST` | `/api/workloads/{name}/move` | Blue-green move. Body `{"to":"<node id or name>"}`. Serialized per-workload. Validates arch + `allowed_nodes`. |
| `GET` | `/api/workloads/{name}/logs` | `text/plain`, `docker compose logs --tail 200`. Proxies to owner. |
| `POST` | `/api/workloads/{name}/prepull` | `202`. Background image pull to warm a failover target's cache. No-op on owner / disallowed nodes. |
| `WS` | `/api/workloads/{name}/exec` | WebSocket PTY into a container. Needs the key via `?api_key=`. |

### Workloads — bulk & portability
| Method | Path | Body / nuance |
|--------|------|---------------|
| `POST` | `/api/workloads/bulk` | `{"action":"start\|stop\|restart\|delete", + exactly one of: "tag":"x" \| "names":[...] \| "all":true}`. Returns `{"selected":[...],"results":{name:{ok,error}}}`. Best-effort, max 8 concurrent. |
| `GET` | `/api/workloads/export` | `?tag=` or `?names=a,b` (else all). Returns a portable bundle: workloads (minus owner/version) + `referenced_env_keys` (the `${VARS}` it found). Does **not** export secret values. |
| `POST` | `/api/workloads/import` | `{"mode":"skip\|replace\|fail","reassign_ips":true,"payload":{...export...}}`. `skip` default; `fail` is atomic (409 on any collision). Sets owner=this node. Does **not** auto-deploy; relies on next reconcile or a `/start`. |

### Cluster status & nodes
| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/status` | Full cluster snapshot: `node`, `peers[]`, `workloads[]` (with computed `status`: running/unhealthy/starting/restarting/stopped/remote/unknown), `service_cidr`, `tunnel`, `warp`. Peer/owner API keys are stripped. |
| `GET` | `/api/health` | Unauthenticated liveness probe. |
| `GET` | `/api/nodes` | List nodes (self + peers) with arch/health/version. |
| `DELETE` | `/api/nodes/{id}` | Remove a peer (can't remove self). Returns `orphaned_workloads[]`; those fail over if `revive`. |
| `POST` | `/api/nodes/{id}/update` | **Admin.** `{"image":"ghcr.io/...:tag","env":{...}}` — pull image + recreate that node's own container (self-replacing). Proxies to the target. |

### Secrets, tokens, join
| Method | Path | Purpose |
|--------|------|---------|
| `POST/GET/DELETE` | `/api/env[/{key}]` | §5. |
| `POST` | `/api/tokens` | **Admin.** `{"ttl_seconds":3600,"note":"..."}` → one-time join token. TTL 1min–7days, default 1h. |
| `GET` | `/api/tokens` | **Admin.** Lists tokens; unused IDs redacted to an 8-char prefix. |
| `DELETE` | `/api/tokens/{id}` | **Admin.** Revoke a pending token (or clear a burned one). |
| `POST` | `/api/join` | **Public** but token-gated. New-node bootstrap (see §7). Refuses plaintext http to non-loopback. |

### Tunnel, host, backup, proxy
| Method | Path | Purpose |
|--------|------|---------|
| `GET/POST/DELETE` | `/api/tunnel` | Manage the Cloudflare tunnel config. |
| `WS` | `/api/tunnel/ws` | Userspace packet tunnel between peers (internal). |
| `GET` | `/api/host/containers` | All Docker containers on this node, including ones Jetty didn't deploy. |
| `GET` | `/api/host/compose` | Host-level compose projects. |
| `POST` | `/api/host/exec` | **Admin + `JETTY_HOST_SHELL=true`.** `{"command":"...","timeout":30}` → `{exit_code,stdout,stderr,duration_ms,node,timed_out}`. `?node=` proxies. Runs via `nsenter` on the host if `--pid=host`, else inside the agent container. |
| `WS` | `/api/host/shell` | **Admin + `JETTY_HOST_SHELL=true`.** Interactive host PTY. |
| `GET` | `/api/backup` | tar.gz of state+compose+warp. Set `X-Backup-Passphrase` to get an encrypted `JETTY-ENC-V1` bundle (safe to store); without it, **secrets are in the clear**. |
| `POST` | `/api/restore` | **Admin.** Restore a backup (overwrites all credentials on this node). |
| `GET/POST/DELETE` | `/api/backup/schedule` | Scheduled backups. |
| `POST` | `/api/admin-key/rotate` | **Admin.** Rotate the cluster AdminKey. |
| `POST` | `/api/peers/{id}/rotate-key` | **Admin.** Force a peer to regenerate its outbound key. |
| `ANY` | `/api/proxy/{mesh_ip}/{path}` | HTTP-proxy a request to a workload by mesh IP. Routes locally (DNAT) or to the owner (tunnel/direct). **Strips `X-API-Key` + cookies** before forwarding to the workload container. |

---

## 7. Adding nodes (join flow)

You (the agent) usually only need to **mint a token**, then hand it to whoever
boots the new node:

```bash
curl -X POST $JETTY/api/tokens \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"ttl_seconds":3600,"note":"node3"}'
# → {"token":"...","expires_at":"...","note":"node3"}
```

The new node boots with `JETTY_JOIN=https://<cluster-domain>` and
`JETTY_JOIN_TOKEN=<token>`. On first start it `POST`s a `JoinRequest` to
`/api/join`; the cluster burns the token (one-time, persisted before continuing),
registers the peer, and returns the peer list, workloads, **AdminKey**,
**EncryptionKey**, encrypted `env_data`, and any tunnel/WARP tokens. Because the
response carries the AdminKey + EncryptionKey, **joins must go over TLS** (the
Cloudflare tunnel) — plaintext joins to a non-loopback host are refused.

---

## 8. Multi-architecture

Mixed amd64/arm64 clusters are supported. Provide arch-specific compose:
- `compose_amd64` / `compose_arm64` win on matching nodes; `compose` is the
  fallback.
- A workload with **only** `compose_arm64` (no fallback `compose`) can run
  **only** on arm64 nodes; failover skips incompatible architectures.
- `allowed_nodes` and architecture are both enforced when choosing a node.

---

## 9. Networking — why workloads are private

This is the heart of Jetty's privacy model:

- The workload's mesh IP (`10.100.x.x`) is bound to a **dummy interface
  `jetty0`** — the host owns the IP but it's attached to nothing public.
- Traffic to `meshIP:port` is caught by **per-port DNAT rules inserted at the
  top of PREROUTING/OUTPUT** (ahead of Docker's own chain) and sent to the
  container's bridge IP. One mesh IP can front multiple services on different
  ports.
- For a workload owned by **another** node, the route to its mesh IP goes
  through a kernel IPIP/GRE tunnel or a userspace WebSocket tunnel over WARP,
  with `src` pinned to the WARP IP so replies return through the mesh — never
  the public internet.
- Cross-workload **DNS** works via injected `extra_hosts` (workload name → mesh
  IP), regenerated when the cluster's workload set changes. Existing containers
  aren't restarted on change, so a newly added workload's name resolves in peers
  only after their next bounce/redeploy.

Net effect: a workload is reachable **only** from inside the WARP mesh. You never
publish a host port to the public internet. To reach a workload's HTTP service
from your tooling, use `GET /api/proxy/{mesh_ip}/{path}`.

---

## 10. Failover, ownership, persistence — operational nuances

- **Failover:** if an owner node goes unhealthy and the workload has
  `revive: true`, a surviving allowed node claims it (`owner`/`version` updated)
  and redeploys. Pre-pull keeps failover fast by warming image caches on allowed
  peers.
- **PATCH does not destroy volumes.** Updates run `up -d` (in-place reconcile),
  **not** `down -v`. Named volumes survive config tweaks. Only `DELETE` (or a
  full recreate) runs `down -v` and wipes volumes/data. Treat `DELETE` as
  destructive.
- **Move is blue-green:** the new owner comes up before the old is torn down;
  serialized per workload to survive double-clicks.
- **Remote ops auto-proxy:** start/stop/restart/logs/PATCH/DELETE on a workload
  you don't own are forwarded to the owner (if healthy). If the owner is dead,
  local cleanup is allowed.
- **Secrets reach all nodes automatically** (gossiped, encrypted), so private
  pulls work on whichever allowed node ends up running or failing over the
  workload — no per-node credential placement needed.

---

## 11. Startup environment variables (how a node is configured)

Set on the `docker run`/compose of the **agent** container itself (not workloads):

| Env var | Default | Meaning |
|---------|---------|---------|
| `JETTY_SECRET` | — | First node: becomes the cluster AdminKey. Joiners get it via `/api/join`. |
| `JETTY_DATA_DIR` | `/data` | state.json, compose projects, WARP state. Mount a volume here. |
| `JETTY_API_PORT` | `6880` | HTTP API + dashboard port. |
| `JETTY_SERVICE_CIDR` | `10.100.0.0/16` | Mesh IP pool for workloads. Must not overlap host networks. |
| `JETTY_JOIN` | — | URL of an existing cluster node (to join). |
| `JETTY_JOIN_TOKEN` | — | One-time join token (from `POST /api/tokens`). |
| `JETTY_TUNNEL_DOMAIN` | — | Cluster's Cloudflare tunnel domain. |
| `JETTY_TUNNEL_HOST` | — | This node's tunnel subdomain. |
| `JETTY_CF_TOKEN` | — | Cloudflare tunnel token (usually delivered via join). |
| `JETTY_WARP_CONNECTOR_TOKEN` | — | WARP connector token (usually delivered via join). |
| `JETTY_HOST_SHELL` | `false` | Enables `/api/host/exec` + `/api/host/shell`. Dangerous (host RCE for any AdminKey holder); requires `--pid=host`. |

The agent container must run `--privileged --net host` with
`-v /var/run/docker.sock:/var/run/docker.sock`, `-v /lib/modules:/lib/modules:ro`,
and a data volume. Gossip uses port `6881` (TCP+UDP).

---

## 12. Recipes

**Deploy a private GHCR app (single org):**
```bash
# once per cluster (or once per GitHub account)
curl -X POST $JETTY/api/env -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"env":{"GHCR_TOKEN":"ghp_..."}}'

# then any number of private workloads referencing it
curl -X POST $JETTY/api/workloads -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"name":"api","revive":true,"autostart":true,
       "registry_auth":{"registry":"ghcr.io","token_ref":"GHCR_TOKEN"},
       "compose":"services:\n  api:\n    image: ghcr.io/myorg/api:latest\n    ports:\n      - \"8080:8080\""}'
```

**Update a private image to a new tag:**
```bash
curl -X PATCH $JETTY/api/workloads/api -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"compose":"services:\n  api:\n    image: ghcr.io/myorg/api:v2\n    ports:\n      - \"8080:8080\""}'
curl -X POST $JETTY/api/workloads/api/restart -H "X-API-Key: $KEY"   # pulls + recreates
```

**Restart everything tagged `prod`:**
```bash
curl -X POST $JETTY/api/workloads/bulk -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"tag":"prod","action":"restart"}'
```

**Pin a workload to one node:**
```bash
# allowed_nodes accepts node names or IDs (see GET /api/nodes)
... '"allowed_nodes":["palmer"]' ...
```

**Reach a workload's HTTP endpoint through the mesh:**
```bash
curl -H "X-API-Key: $KEY" "$JETTY/api/proxy/10.100.0.50/healthz"
```

---

## 13. Footguns checklist

- ❌ **Don't put a raw token in the workload/compose.** Workloads sync in the
  clear. Use `registry_auth.token_ref` → encrypted env store.
- ⚠️ **Silent pull fallback:** a bad `token_ref`/expired PAT won't fail the pull
  step loudly; the deploy fails at `up`. Check owner-node logs + `GET /api/env`.
- ⚠️ **`DELETE` wipes volumes** (`down -v`). `PATCH` does not. Know which you want.
- ⚠️ **`registry` is a host, not a repo path** — `ghcr.io`, not `ghcr.io/org`.
- ⚠️ **Admin vs peer key:** token/host/node-update endpoints need the AdminKey.
- ⚠️ **`username` default** is `x-access-token` (fine for GHCR PATs); set it
  explicitly for registries that need a real username.
- ✅ **Omit `ip`** and let Jetty allocate a mesh IP.
- ✅ **Send writes to any node** — proxying handles the rest.
```
