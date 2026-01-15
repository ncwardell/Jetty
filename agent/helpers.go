package agent

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
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

// Valid environment variable key pattern
var envKeyPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

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
	return buildOwnerInfo(a.hwid, a.hostname, a.ip)
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
			log.Printf("Warning: invalid integer value for %s: %q, using default %d", key, v, defaultValue)
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

// =============================================================================
// URL Helpers
// =============================================================================

// getPeerAPIURL returns the URL to reach a peer's API via WARP.
func (a *Agent) getPeerAPIURL(peer *Peer, path string) string {
	// Use WARP IP for direct node-to-node communication
	if peer.IP != "" {
		return fmt.Sprintf("http://%s:%d%s", peer.IP, a.apiPort, path)
	}
	// Fall back to tunnel domain if peer IP unknown (WARP not yet connected)
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
func (a *Agent) peerRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	if a.clusterSecret != "" {
		req.Header.Set("X-API-Key", a.clusterSecret)
	}
	if method == "POST" || method == "PUT" || method == "PATCH" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}
