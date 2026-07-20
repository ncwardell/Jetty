package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// =============================================================================
// HTTP Handlers - one-time join tokens
// =============================================================================
//
// All endpoints in this file are admin-only: the caller's X-API-Key
// must equal state.AdminKey. Peer-API-keys cannot mint or list tokens
// because they cannot mint joining credentials for new nodes - only
// the human operator should be able to do that.
//
//   POST   /api/tokens          - mint a one-time token
//   GET    /api/tokens          - list pending and recently-burned tokens
//   DELETE /api/tokens/{id}     - revoke (or expunge a burned record)

// Defaults for token TTL.
const (
	defaultJoinTokenTTL = 1 * time.Hour
	maxJoinTokenTTL     = 7 * 24 * time.Hour
	usedTokenRetention  = 24 * time.Hour
)

// apiCreateToken godoc
// @Summary Mint a one-time join token
// @Description Generates a fresh single-use bearer token a new node can pass as JETTY_JOIN_TOKEN. Burned on first successful /api/join. Admin only.
// @Tags tokens
// @Accept json
// @Produce json
// @Param request body CreateTokenRequest false "TTL + note (both optional)"
// @Success 200 {object} CreateTokenResponse
// @Failure 401 {object} ErrorResponse "Admin auth required"
// @Router /tokens [post]
func (a *Agent) apiCreateToken(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthorize(r) {
		http.Error(w, "unauthorized: admin key required", http.StatusUnauthorized)
		return
	}

	var req struct {
		TTLSeconds int    `json:"ttl_seconds,omitempty"`
		Note       string `json:"note,omitempty"`
	}
	// Body is optional - empty body uses defaults.
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
			return
		}
	}
	ttl := defaultJoinTokenTTL
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
	}
	if ttl > maxJoinTokenTTL {
		ttl = maxJoinTokenTTL
	}
	if ttl < time.Minute {
		ttl = time.Minute
	}

	tokenID, err := generateAPIKey()
	if err != nil {
		http.Error(w, fmt.Sprintf("generate token: %v", err), http.StatusInternalServerError)
		return
	}
	now := time.Now()
	tok := &JoinToken{
		ID:        tokenID,
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
		Note:      req.Note,
	}

	a.stateMu.Lock()
	if a.state.JoinTokens == nil {
		a.state.JoinTokens = make(map[string]*JoinToken)
	}
	a.state.JoinTokens[tokenID] = tok
	a.stateMu.Unlock()
	a.saveState()

	log.Printf("token: minted (expires %s, note=%q)", tok.ExpiresAt.Format(time.RFC3339), tok.Note)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"token":      tokenID,
		"expires_at": tok.ExpiresAt,
		"note":       tok.Note,
	})
}

// apiListTokens godoc
// @Summary List join tokens
// @Description Returns pending and recently-burned tokens. Pending token IDs are redacted (only an 8-char prefix shown); used tokens show in full because they can no longer be replayed. Admin only.
// @Tags tokens
// @Produce json
// @Success 200 {object} ListTokensResponse
// @Failure 401 {object} ErrorResponse "Admin auth required"
// @Router /tokens [get]
func (a *Agent) apiListTokens(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthorize(r) {
		http.Error(w, "unauthorized: admin key required", http.StatusUnauthorized)
		return
	}

	a.gcExpiredTokens()

	a.stateMu.RLock()
	tokens := make([]*JoinToken, 0, len(a.state.JoinTokens))
	for _, t := range a.state.JoinTokens {
		// Return a redacted copy so the listing doesn't expose the
		// secret value of any *unused* token. Used tokens are safe to
		// show because they can't be replayed.
		copy := *t
		if !copy.Used {
			copy.ID = redactTokenID(copy.ID)
		}
		tokens = append(tokens, &copy)
	}
	a.stateMu.RUnlock()

	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i].CreatedAt.After(tokens[j].CreatedAt)
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"tokens": tokens})
}

// apiDeleteToken godoc
// @Summary Revoke a join token
// @Description Deletes a token by ID. Works on both pending tokens (true revocation) and already-burned tokens (just expunges the audit record). Admin only.
// @Tags tokens
// @Param id path string true "Token ID (the secret value itself)"
// @Produce json
// @Success 200 {object} RevokeTokenResponse
// @Failure 401 {object} ErrorResponse "Admin auth required"
// @Router /tokens/{id} [delete]
func (a *Agent) apiDeleteToken(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthorize(r) {
		http.Error(w, "unauthorized: admin key required", http.StatusUnauthorized)
		return
	}

	id := mux.Vars(r)["id"]
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	a.stateMu.Lock()
	_, existed := a.state.JoinTokens[id]
	if existed {
		delete(a.state.JoinTokens, id)
	} else if strings.HasSuffix(id, "...") {
		// apiListTokens redacts *unused* token IDs to an 8-char prefix + "..."
		// so the secret isn't exposed in the listing. Accept that displayed
		// form here too - otherwise a pending token can never be revoked from
		// the dashboard (the UI only ever has the redacted id), only via its
		// full secret value. Match by prefix, but only act on a UNIQUE hit so
		// an ambiguous prefix can't nuke the wrong token.
		prefix := strings.TrimSuffix(id, "...")
		if len(prefix) >= 6 {
			matches := 0
			var matchID string
			for tid := range a.state.JoinTokens {
				if strings.HasPrefix(tid, prefix) {
					matchID = tid
					matches++
				}
			}
			if matches == 1 {
				delete(a.state.JoinTokens, matchID)
				existed = true
			}
		}
	}
	a.stateMu.Unlock()
	if existed {
		a.saveState()
		log.Printf("token: revoked id=%s", redactTokenID(id))
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"revoked": existed})
}

// consumeJoinToken validates and burns a token. Called from apiJoin.
// Returns the token (with Used=true, UsedBy filled) on success, or an
// error describing why the token was rejected.
//
// The burn is persisted to disk before this function returns, so a
// crash anywhere later in apiJoin cannot lead to a replay-able token.
// Persistence happens AFTER releasing stateMu (saveState takes its own
// RLock) to keep the critical section small.
func (a *Agent) consumeJoinToken(tokenID, joinerID string) (*JoinToken, error) {
	if tokenID == "" {
		return nil, fmt.Errorf("missing join_token")
	}
	a.stateMu.Lock()
	tok := a.state.JoinTokens[tokenID]
	if tok == nil {
		a.stateMu.Unlock()
		return nil, fmt.Errorf("invalid join_token")
	}
	if tok.Used {
		a.stateMu.Unlock()
		return nil, fmt.Errorf("join_token already used")
	}
	if time.Now().After(tok.ExpiresAt) {
		a.stateMu.Unlock()
		return nil, fmt.Errorf("join_token expired")
	}
	tok.Used = true
	tok.UsedBy = joinerID
	tok.UsedAt = time.Now()
	a.stateMu.Unlock()

	// Commit the burn to disk immediately. A crash between here and the
	// remainder of apiJoin must NOT leave the token replay-able.
	a.saveState()
	return tok, nil
}

// gcExpiredTokens removes expired-and-unused tokens, and burned tokens
// older than usedTokenRetention. Cheap; called from apiListTokens and
// from a periodic background loop.
func (a *Agent) gcExpiredTokens() {
	now := time.Now()
	a.stateMu.Lock()
	defer a.stateMu.Unlock()
	for id, t := range a.state.JoinTokens {
		if t.Used && now.Sub(t.UsedAt) > usedTokenRetention {
			delete(a.state.JoinTokens, id)
			continue
		}
		if !t.Used && now.After(t.ExpiresAt) {
			delete(a.state.JoinTokens, id)
		}
	}
}

// redactTokenID returns a short prefix for display - so logs and the
// dashboard list can refer to a token without exposing it. Tokens are
// 32 bytes encoded base64-URL, so 6 chars is plenty unique for humans.
func redactTokenID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8] + "..."
}
