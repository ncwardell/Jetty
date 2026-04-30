package agent

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

// =============================================================================
// HTTP Handler - admin-key rotation
// =============================================================================
//
// POST /api/admin-key/rotate
//
// Body (optional):
//   {"new_key": "<32+ chars>"}
//
// If new_key is empty, the server generates a random 256-bit key.
// The new key:
//   1. Replaces state.AdminKey on this node, persists immediately.
//   2. Is gossiped via memberlist so every peer adopts it.
//   3. Is returned in the response so the calling dashboard can
//      update its local cache.
//
// Use case: respond to a suspected AdminKey leak without nuking the
// cluster. Old key is invalid as soon as the rotating node persists.

// apiRotateAdminKey godoc
// @Summary Rotate the cluster admin key
// @Description Generates (or accepts) a new admin key, persists it on this node, and broadcasts it via memberlist so every peer adopts it. Returns the new key in the response. Admin only.
// @Tags admin
// @Accept json
// @Produce json
// @Param request body RotateAdminKeyRequest false "Rotation parameters (new_key optional - server generates if absent)"
// @Success 200 {object} RotateAdminKeyResponse
// @Failure 400 {object} ErrorResponse "Invalid new key"
// @Failure 401 {object} ErrorResponse "Admin auth required"
// @Router /admin-key/rotate [post]
func (a *Agent) apiRotateAdminKey(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthorize(r) {
		http.Error(w, "unauthorized: admin key required", http.StatusUnauthorized)
		return
	}

	var req struct {
		NewKey string `json:"new_key,omitempty"`
	}
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	newKey := strings.TrimSpace(req.NewKey)

	// If the operator didn't supply one, generate a strong random key.
	// 32 bytes from crypto/rand encoded base64-RawURL = 43 chars, 256
	// bits of entropy. That's the same shape SelfAPIKey/JoinToken use.
	if newKey == "" {
		k, err := generateAPIKey()
		if err != nil {
			http.Error(w, "generate key: "+err.Error(), 500)
			return
		}
		newKey = k
	}
	if len(newKey) < 16 {
		http.Error(w, "new_key must be at least 16 characters", http.StatusBadRequest)
		return
	}

	a.stateMu.Lock()
	a.state.AdminKey = newKey
	a.stateMu.Unlock()
	a.saveState()

	// Broadcast to peers so they all flip to the new key together.
	a.broadcastAdminKey(newKey)

	log.Printf("AdminKey rotated by %s (gossiping to peers)", r.RemoteAddr)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "rotated",
		"new_key": newKey,
		"hint":    "Update any saved credentials (dashboard, scripts, CI). The old key is now invalid on this node and will be invalid cluster-wide as gossip propagates.",
	})

	// Belt-and-suspenders: trigger a state push to peers so they get
	// the new key faster than the gossip ticker would deliver it.
	go a.broadcastState()
}
