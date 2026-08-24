package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// =============================================================================
// HTTP Handler - rotate a peer's SelfAPIKey
// =============================================================================
//
// POST /api/peers/{id}/rotate-key
//
// Admin-only. Replaces the target peer's SelfAPIKey with a fresh
// random value. Only the peer itself owns its SelfAPIKey, so when the
// caller hits this endpoint on a node that isn't the target, we proxy
// to the target. The target regenerates SelfAPIKey, persists, and
// updates its memberlist NodeMeta - which all other peers adopt via
// NotifyUpdate (now safe because we enforce meta.ID == node.Name).
//
// Use case: "I'm not 100% sure if peer X is compromised, but I don't
// want to take it down." Rotation is gentler than deleting + rejoining.

// apiRotatePeerKey godoc
// @Summary Rotate a peer's SelfAPIKey
// @Description Admin-only. Forces the named peer to regenerate its outbound peer credential. The peer itself owns the key, so the request is proxied to the target node when needed. After rotation gossip propagates, the old key is invalid cluster-wide.
// @Tags admin
// @Param id path string true "Peer HWID or hostname (use 'self' for this node)"
// @Produce json
// @Success 200 {object} RotatePeerKeyResponse
// @Failure 401 {object} ErrorResponse "Admin auth required"
// @Failure 404 {object} ErrorResponse "Peer not found"
// @Router /peers/{id}/rotate-key [post]
func (a *Agent) apiRotatePeerKey(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthorize(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized: admin key required")
		return
	}
	id := mux.Vars(r)["id"]
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing id")
		return
	}

	// Self path: rotate locally.
	if id == "self" || id == a.hwid || id == a.hostname {
		newKey, err := a.rotateSelfAPIKeyLocal()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "rotate failed: "+err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "rotated",
			"peer_id": a.hwid,
			"hint":    "The new SelfAPIKey is now propagating via memberlist NodeMeta. Other peers will adopt it within a few hundred ms.",
			// Don't return the new key itself - the operator never
			// needs to see it; only the agent and its peers do.
		})
		log.Printf("Self APIKey rotated by admin from %s; new key has %d chars", r.RemoteAddr, len(newKey))
		return
	}

	// Other peer: find them and proxy.
	a.stateMu.RLock()
	var target *Peer
	for _, p := range a.state.Peers {
		if p.ID == id || p.Name == id {
			target = p
			break
		}
	}
	a.stateMu.RUnlock()
	if target == nil {
		writeError(w, http.StatusNotFound, "peer not found")
		return
	}
	if !target.Healthy {
		writeError(w, http.StatusServiceUnavailable, "peer is unhealthy; cannot rotate its key remotely (peer must be reachable to mint a new SelfAPIKey)")
		return
	}

	url := a.getPeerAPIURL(target, "/api/peers/self/rotate-key")
	if url == "" {
		writeError(w, http.StatusServiceUnavailable, "no route to peer")
		return
	}
	// The proxied request needs admin auth at the target. We carry
	// the operator's X-API-Key forward verbatim - the target will
	// validate it against its own state.AdminKey, which (because
	// AdminKey is cluster-wide) matches what the operator sent us.
	pr, err := http.NewRequest("POST", url, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "build proxy request: "+err.Error())
		return
	}
	if k := r.Header.Get("X-API-Key"); k != "" {
		pr.Header.Set("X-API-Key", k)
	}
	resp, err := httpClient.Do(pr)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("proxy to peer: %v", err))
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// rotateSelfAPIKeyLocal generates a new SelfAPIKey on this node,
// persists it, and pushes it through memberlist so peers adopt it.
// Returns the new key (caller decides whether to expose it).
func (a *Agent) rotateSelfAPIKeyLocal() (string, error) {
	newKey, err := generateAPIKey()
	if err != nil {
		return "", err
	}
	a.stateMu.Lock()
	a.state.SelfAPIKey = newKey
	// Also update our own Peer entry, if one exists in our own state
	// (some tests + sanity flows store it). Doesn't hurt in prod.
	for _, p := range a.state.Peers {
		if p.ID == a.hwid {
			p.APIKey = newKey
		}
	}
	a.stateMu.Unlock()
	a.saveState()

	// Push the rotation through memberlist so other peers update
	// their stored Peer.APIKey for us.
	if a.mlDelegate != nil {
		a.mlDelegate.updateNodeMeta(a.ip)
		if a.memberlist != nil {
			a.memberlist.UpdateNode(0)
		}
	}
	return newKey, nil
}
