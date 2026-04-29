package agent

import (
	"crypto/subtle"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/ncwardell/jetty/docs"
	httpSwagger "github.com/swaggo/http-swagger"
)

// =============================================================================
// HTTP API framing: middleware, server bootstrap, route table
// =============================================================================
//
// Two middleware layers wrap every route:
//
//   corsMiddleware     - permissive CORS (any origin, credentials allowed).
//                        The X-API-Key requirement on protected routes is
//                        what actually stops cross-origin abuse.
//
//   apiKeyMiddleware   - constant-time check of X-API-Key (or ?api_key=...)
//                        against JETTY_SECRET. A small allowlist of public
//                        paths (/api/health, /api/join, internal sync,
//                        /swagger, dashboard root) is exempt.
//
// runAPI registers all routes (delegating to handlers in handlers_*.go),
// applies the middleware stack, and starts ListenAndServe on apiPort.
// waitForAPI blocks until the server is reachable so cloudflared can
// safely connect to it.

// corsMiddleware reflects the request Origin (or "*") with credentials
// allowed. Combined with apiKeyMiddleware below, the only way to make a
// useful authenticated request is to supply X-API-Key in a header, which
// can't be sent automatically by a browser without the API key being
// known. Effectively this lets the embedded dashboard work over file://
// without weakening the security model.
func (a *Agent) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Allow requests from any origin (web UI can be opened from file:// or any domain)
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = "*"
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-API-Key, Authorization")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Max-Age", "86400")

		// Handle preflight requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// apiKeyMiddleware enforces JETTY_SECRET on protected endpoints.
//
// publicPaths is an allowlist of routes that don't require the API key:
//   - /api/health   - monitoring probes
//   - /api/join     - new-node bootstrap (validates the secret in the body)
//   - /api/sync, /api/peer-announce, /api/heartbeat, /api/tunnel/sync -
//     internal cluster machinery; these endpoints expect requests from
//     other cluster members and validate identity in the request body
//     where applicable
//   - /swagger/     - API docs UI
//   - /             - dashboard root
//
// All other routes require X-API-Key matching JETTY_SECRET. The comparison
// uses subtle.ConstantTimeCompare to avoid timing-leak attacks on the
// secret.
//
// If JETTY_SECRET is unset, every route is unauthenticated. Start() prints
// a loud warning when this is the case; production deployments must set it.
func (a *Agent) apiKeyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip authentication if no secret is configured
		if a.clusterSecret == "" {
			next.ServeHTTP(w, r)
			return
		}

		// Public endpoints that don't require API key
		publicPaths := []string{
			"/api/health",        // Monitoring
			"/api/join",          // Node joining (uses secret in body)
			"/api/sync",          // Internal cluster sync
			"/api/peer-announce", // Internal peer announcement
			"/api/heartbeat",     // Internal heartbeat
			"/api/tunnel/sync",   // Internal tunnel sync
			"/swagger/",          // API documentation
		}

		path := r.URL.Path

		// Dashboard root is public (exact match)
		if path == "/" {
			next.ServeHTTP(w, r)
			return
		}

		for _, p := range publicPaths {
			if strings.HasPrefix(path, p) {
				next.ServeHTTP(w, r)
				return
			}
		}

		// Check API key from header or query param
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			apiKey = r.URL.Query().Get("api_key")
		}

		if subtle.ConstantTimeCompare([]byte(apiKey), []byte(a.clusterSecret)) != 1 {
			http.Error(w, "unauthorized: invalid or missing API key", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// waitForAPI polls /api/health until the server starts accepting requests
// (up to 5s). Called from Start() before launching cloudflared so the
// tunnel doesn't try to forward to a dead listener.
//
// Treats both 200 (healthy) and 401 (auth required) as "up" - we just need
// to know the listener exists.
func (a *Agent) waitForAPI() {
	addr := fmt.Sprintf("http://127.0.0.1:%d/api/health", a.apiPort)
	maxRetries := 50 // 5 seconds total (50 * 100ms)

	for i := 0; i < maxRetries; i++ {
		resp, err := http.Get(addr)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 || resp.StatusCode == 401 { // 401 is OK, means API is up but needs auth
				log.Printf("API server ready")
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Printf("Warning: API server not responding after 5s, continuing anyway")
}

// runAPI builds the route table, wraps it in middleware, and starts
// ListenAndServe. Called as a goroutine from Start(); blocks forever
// (or exits the process via log.Fatalf if ListenAndServe fails).
func (a *Agent) runAPI() {
	// Set Swagger host to empty so it uses the current request's host
	// This makes it work automatically for both localhost and cloudflared tunnels
	docs.SwaggerInfo.Host = ""

	r := mux.NewRouter()

	// --- Workload CRUD (handlers_workloads.go) -----------------------
	r.HandleFunc("/api/workloads", a.apiListWorkloads).Methods("GET")
	r.HandleFunc("/api/workloads", a.apiCreateWorkload).Methods("POST")
	r.HandleFunc("/api/workloads/{name}", a.apiGetWorkload).Methods("GET")
	r.HandleFunc("/api/workloads/{name}", a.apiUpdateWorkload).Methods("PATCH")
	r.HandleFunc("/api/workloads/{name}", a.apiDeleteWorkload).Methods("DELETE")
	r.HandleFunc("/api/workloads/{name}/move", a.apiMoveWorkload).Methods("POST")
	r.HandleFunc("/api/workloads/{name}/logs", a.apiWorkloadLogs).Methods("GET")
	r.HandleFunc("/api/workloads/{name}/start", a.apiStartWorkload).Methods("POST")
	r.HandleFunc("/api/workloads/{name}/stop", a.apiStopWorkload).Methods("POST")
	r.HandleFunc("/api/workloads/{name}/restart", a.apiRestartWorkload).Methods("POST")
	r.HandleFunc("/api/workloads/{name}/prepull", a.apiPrePullWorkload).Methods("POST")

	// --- Cluster status & internal sync (handlers_cluster.go) --------
	r.HandleFunc("/api/status", a.apiStatus).Methods("GET")
	r.HandleFunc("/api/health", a.apiHealth).Methods("GET")
	r.HandleFunc("/api/sync", a.apiSync).Methods("GET")
	r.HandleFunc("/api/peer-announce", a.apiPeerAnnounce).Methods("POST")
	r.HandleFunc("/api/heartbeat", a.apiHeartbeat).Methods("POST")
	r.HandleFunc("/api/tunnel/sync", a.apiTunnelSync).Methods("POST")

	// --- Cluster join (join.go) --------------------------------------
	r.HandleFunc("/api/join", a.apiJoin).Methods("POST")

	// --- Node CRUD (handlers_nodes.go) -------------------------------
	r.HandleFunc("/api/nodes", a.apiListNodes).Methods("GET")
	r.HandleFunc("/api/nodes/{id}", a.apiRemoveNode).Methods("DELETE")
	r.HandleFunc("/api/nodes/{id}/update", a.apiUpdateNode).Methods("POST")

	// --- Cloudflare Tunnel (handlers_tunnel.go) ----------------------
	r.HandleFunc("/api/tunnel", a.apiGetTunnel).Methods("GET")
	r.HandleFunc("/api/tunnel", a.apiSetTunnel).Methods("POST")
	r.HandleFunc("/api/tunnel", a.apiDeleteTunnel).Methods("DELETE")

	// --- Userspace WS tunnel (tunnel.go) -----------------------------
	r.HandleFunc("/api/tunnel/ws", a.apiTunnelWs) // WebSocket for packet tunneling

	// --- Web terminals (handlers_terminal.go) ------------------------
	// WebSocket-backed PTY into a workload's container, or a host shell.
	// Both require JETTY_SECRET via ?api_key= or X-API-Key. Host shell
	// is additionally gated by JETTY_HOST_SHELL=true.
	r.HandleFunc("/api/workloads/{name}/exec", a.apiWorkloadExec)
	r.HandleFunc("/api/host/shell", a.apiHostShell)

	// --- Encrypted env vars (handlers_env.go) ------------------------
	r.HandleFunc("/api/env", a.apiListEnv).Methods("GET")
	r.HandleFunc("/api/env", a.apiSetEnv).Methods("POST")
	r.HandleFunc("/api/env/{key}", a.apiGetEnv).Methods("GET")
	r.HandleFunc("/api/env/{key}", a.apiDeleteEnv).Methods("DELETE")

	// --- Host-wide Docker introspection (handlers_host.go) -----------
	// Surface containers on this node that Jetty did NOT deploy, so the
	// dashboard can show everything running, not just managed workloads.
	r.HandleFunc("/api/host/containers", a.apiHostContainers).Methods("GET")
	r.HandleFunc("/api/host/compose", a.apiHostCompose).Methods("GET")

	// --- Backup / restore (handlers_backup.go) -----------------------
	r.HandleFunc("/api/backup", a.apiBackup).Methods("GET")
	r.HandleFunc("/api/restore", a.apiRestore).Methods("POST")

	// --- HTTP proxy to a workload by mesh IP (handlers_workloads.go) -
	r.PathPrefix("/api/proxy/").HandlerFunc(a.apiWorkloadProxy)

	// Dashboard UI (embedded)
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(dashboardHTML)
	}).Methods("GET")

	// Swagger UI
	r.PathPrefix("/swagger/").Handler(httpSwagger.WrapHandler)

	// Wrap router with API key middleware and CORS middleware
	handler := a.corsMiddleware(a.apiKeyMiddleware(r))

	addr := fmt.Sprintf(":%d", a.apiPort)
	log.Printf("API on %s (auth=%v)", addr, a.clusterSecret != "")
	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatalf("API server failed: %v", err)
	}
}
