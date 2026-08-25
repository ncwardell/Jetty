package agent

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// =============================================================================
// Validation
// =============================================================================

// Valid workload name pattern (alphanumeric, dash, underscore only)
var validNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// Valid peer hostname pattern. Slightly more permissive than workload
// names because operators set hostnames via /etc/hostname and FQDNs
// contain dots. The point is to reject anything that could break out of
// a single /etc/hosts line (newlines, tabs, spaces, comment chars).
var validPeerNamePattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// Valid environment variable key pattern
var envKeyPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Valid peer version/arch pattern. These are display-only metadata
// ("0.0.2", "dev", "amd64") but they are peer-supplied and the dashboard
// interpolates them into HTML - so restrict to a charset that cannot
// carry markup. Empty is allowed (older agents don't send them).
var validVersionArchPattern = regexp.MustCompile(`^[a-zA-Z0-9._+-]{0,64}$`)

// Valid tunnel hostname pattern. Unlike version/arch this is not display-only:
// it is interpolated directly into an https:// URL that we then send
// authenticated requests to. So the charset has to exclude everything that
// could change what the URL means - '/' (path/authority injection), '@'
// (userinfo, which relocates the host), ':' (port/scheme), and whitespace.
// A bare DNS name is all this field is ever meant to hold.
var validTunnelHostPattern = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9.-]{0,251}[a-zA-Z0-9])?$`)

// sanitizeTunnelHost blanks a peer-supplied tunnel hostname that is not a
// plain DNS name. Sanitize-not-reject, like version/arch: getPeerAPIURL just
// falls back to the cluster-wide domain, which is where we were before this
// field existed. Rejecting the peer outright would turn a cosmetic misconfig
// into a node dropping out of the cluster.
func sanitizeTunnelHost(id string, host *string) {
	if !validTunnelHostPattern.MatchString(*host) && *host != "" {
		logInfof("Sync: blanking invalid tunnel host %q from peer %q", *host, id)
		*host = ""
	}
}

// sanitizePeerMeta blanks peer-supplied version/arch fields that do not
// match the safe charset. Sanitize-not-reject: a peer with a weird build
// string should stay in the cluster, just without the unsafe metadata.
func sanitizePeerMeta(id string, version, arch *string) {
	if !validVersionArchPattern.MatchString(*version) {
		logInfof("Sync: blanking invalid version %q from peer %q", *version, id)
		*version = ""
	}
	if !validVersionArchPattern.MatchString(*arch) {
		logInfof("Sync: blanking invalid arch %q from peer %q", *arch, id)
		*arch = ""
	}
}

// validateRegistryAuth checks the shape of a workload's optional registry
// credential. nil is valid (no private-registry auth - default behavior).
// We validate shape only, not that the referenced env key exists: operators
// may attach registry_auth before adding the secret, and pull-time surfaces
// a clear "token_ref not found" error if it's still missing.
func validateRegistryAuth(ra *RegistryAuth) error {
	if ra == nil {
		return nil
	}
	if ra.Registry == "" {
		return fmt.Errorf("registry_auth.registry required (e.g. \"ghcr.io\")")
	}
	if strings.ContainsAny(ra.Registry, " \t\r\n/") {
		return fmt.Errorf("registry_auth.registry %q is not a valid registry host", ra.Registry)
	}
	if ra.TokenRef == "" {
		return fmt.Errorf("registry_auth.token_ref required (name of an env-store key holding the token)")
	}
	if !envKeyPattern.MatchString(ra.TokenRef) {
		return fmt.Errorf("registry_auth.token_ref %q must be a valid env key (alphanumeric/underscore, not starting with a digit)", ra.TokenRef)
	}
	return nil
}

// validTagPattern accepts lowercase alphanumerics + dash + underscore
// (and the colon character so users can do prefix tags like "env:prod"
// if they want, without us needing a full key=value labels system).
// Lowercase-only keeps the deterministic-color hash stable across
// case differences a human typed.
var validTagPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_:-]{0,62}$`)

// ValidateTag returns true if t is an acceptable tag string.
func ValidateTag(t string) bool {
	return validTagPattern.MatchString(t)
}

// normalizeTags lowercases, trims, validates, dedupes, and sorts a
// tag slice. Returns the canonical slice and the first invalid tag
// (empty string if all valid). Used on every ingest path so the wire
// form is canonical and equality comparisons are trivial.
func normalizeTags(tags []string) ([]string, string) {
	if len(tags) == 0 {
		return nil, ""
	}
	seen := make(map[string]bool, len(tags))
	out := make([]string, 0, len(tags))
	for _, raw := range tags {
		t := strings.ToLower(strings.TrimSpace(raw))
		if t == "" {
			continue
		}
		if !ValidateTag(t) {
			return nil, raw
		}
		if seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	if len(out) == 0 {
		return nil, ""
	}
	sortStrings(out)
	return out, ""
}

// sortStrings is a tiny wrapper around sort.Strings kept here so this
// file doesn't need a sort import (the file is already a kitchen sink
// of helpers; the sort dependency is only used by normalizeTags).
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// ValidateWorkloadName checks if a workload name is valid
func ValidateWorkloadName(name string) bool {
	return validNamePattern.MatchString(name)
}

// ValidateEnvKey checks if an environment variable key is valid
func ValidateEnvKey(key string) bool {
	return envKeyPattern.MatchString(key)
}

// =============================================================================
// JSON Response Helpers
// =============================================================================

// writeJSON writes a JSON response with 200 OK status
func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// writeJSONStatus writes a JSON response with a custom status code
func writeJSONStatus(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// writeError writes a JSON error response
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSONStatus(w, status, map[string]string{"error": message})
}

// =============================================================================
// Owner Info Helpers
// =============================================================================

// buildOwnerInfo creates an owner info map for API responses
func buildOwnerInfo(id, name, ip string) OwnerInfo {
	return OwnerInfo{
		ID:   id,
		Name: name,
		IP:   ip,
	}
}

// selfOwnerInfo returns owner info for this node
func (a *Agent) selfOwnerInfo() OwnerInfo {
	return buildOwnerInfo(a.hwid, a.hostname, a.warpIP())
}

// peerOwnerInfo returns owner info for a peer, or unknown info if peer is nil
func peerOwnerInfo(peer *Peer) OwnerInfo {
	if peer == nil {
		return OwnerInfo{ID: "unknown", Name: "unknown", IP: "unknown"}
	}
	return buildOwnerInfo(peer.ID, peer.Name, peer.IP)
}

// ownerInfoMap converts OwnerInfo to a map for JSON responses (backwards compatible)
func (o OwnerInfo) ToMap() map[string]string {
	return map[string]string{
		"id":   o.ID,
		"name": o.Name,
		"ip":   o.IP,
	}
}

// =============================================================================
// Environment Helpers
// =============================================================================

// getEnv returns environment variable value or default
func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// getEnvInt returns environment variable as int or default
func getEnvInt(key string, defaultValue int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			logWarnf("invalid integer value for %s: %q, using default %d", key, v, defaultValue)
			return defaultValue
		}
		return n
	}
	return defaultValue
}

// getHostname returns the system hostname or "node" as fallback
func getHostname() string {
	h, _ := os.Hostname()
	if h == "" {
		return "node"
	}
	return h
}

// =============================================================================
// Network Helpers
// =============================================================================

// getPublicIP attempts to determine the public IP address of this node
func getPublicIP() string {
	// Allow override via environment variable (useful in containers)
	if ip := os.Getenv("JETTY_PUBLIC_IP"); ip != "" {
		return ip
	}

	// Try external services to get actual public IP (with short timeout)
	client := &http.Client{Timeout: QuickHTTPTimeout}
	services := []string{
		"https://api.ipify.org",
		"https://ifconfig.me/ip",
		"https://icanhazip.com",
	}

	for _, svc := range services {
		resp, err := client.Get(svc)
		if err != nil {
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}
		ip := strings.TrimSpace(string(body))
		// Validate it looks like an IP
		if parsed := net.ParseIP(ip); parsed != nil {
			return ip
		}
	}

	// Fallback: get outbound IP (local IP used to reach internet)
	dialer := net.Dialer{Timeout: 2 * time.Second}
	c, err := dialer.Dial("udp", "8.8.8.8:80")
	if err == nil {
		defer c.Close()
		if addr, ok := c.LocalAddr().(*net.UDPAddr); ok && !addr.IP.IsLoopback() {
			return addr.IP.String()
		}
	}

	// Last resort: find first non-loopback interface IP
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}

	return "127.0.0.1"
}

// =============================================================================
// ID Generation
// =============================================================================

// genID generates a random hex ID
func genID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// shortID returns id truncated to n chars, safe for short or empty ids.
// Used in log lines that previously did id[:12] / id[:8] - those panicked
// when given an unexpectedly short id (which can happen in tests or if a
// peer sends a malformed identifier on the wire).
func shortID(id string, n int) string {
	if len(id) <= n {
		return id
	}
	return id[:n]
}

// =============================================================================
// URL Helpers
// =============================================================================

// getPeerAPIURL returns the URL to reach a specific peer's API.
//
// The ordering matters. WARP is direct and unambiguous, so it wins when
// available. The interesting case is the fallback, used before WARP comes up:
// a.tunnelDomain is the *cluster-wide* hostname, and Cloudflare resolves it to
// whichever node it feels like. So the shared-domain fallback does not address
// the peer we asked for - it addresses some node, possibly ourselves. Requests
// that mutate peer-specific state (rotate-key, leave, move) silently land on
// the wrong node.
//
// peer.TunnelHost is that peer's own hostname, gossiped in NodeMeta, and it
// resolves to exactly one node. Prefer it. The shared domain stays as a
// last-ditch fallback rather than being removed, because for read-only calls
// against a single-node cluster it is still better than returning "".
func (a *Agent) getPeerAPIURL(peer *Peer, path string) string {
	if peer == nil {
		return ""
	}
	// Use WARP IP for direct node-to-node communication
	if peer.IP != "" {
		return fmt.Sprintf("http://%s:%d%s", peer.IP, a.apiPort, path)
	}
	// This peer's own hostname: resolves to this peer and no other.
	if peer.TunnelHost != "" {
		return fmt.Sprintf("https://%s%s", peer.TunnelHost, path)
	}
	// Cluster-wide domain: reaches *a* node, not necessarily this one.
	if a.tunnelDomain != "" {
		return fmt.Sprintf("https://%s%s", a.tunnelDomain, path)
	}
	return ""
}

// getTunnelAPIURL returns the Cloudflare tunnel URL for API calls.
// Returns empty string if not in tunnel-only mode.
func (a *Agent) getTunnelAPIURL(path string) string {
	if a.tunnelDomain == "" {
		return ""
	}
	return fmt.Sprintf("https://%s%s", a.tunnelDomain, path)
}

// peerRequest creates an HTTP request to a peer with auth headers set.
// Uses this node's SelfAPIKey - the receiver's apiKeyMiddleware will
// match it against any registered Peer.APIKey.
func (a *Agent) peerRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	a.stateMu.RLock()
	apiKey := a.state.SelfAPIKey
	a.stateMu.RUnlock()
	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}
	if method == "POST" || method == "PUT" || method == "PATCH" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}
