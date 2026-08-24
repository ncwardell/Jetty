package agent

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"golang.org/x/crypto/argon2"
)

// =============================================================================
// Encryption (AES-256-GCM with raw 32-byte key)
// =============================================================================
//
// EnvData is encrypted under State.EncryptionKey - 32 random bytes generated
// once on the bootstrap node and propagated to joiners via /api/join. The
// joiner persists the same key so it can read env_data and write new
// entries that other nodes can also read.
//
// Ciphertext format: "v2:" + base64(nonce || gcm_seal(plaintext)).
//
// Legacy reader: clusters that ran older code derived the key via
// Argon2id(JETTY_SECRET, EncryptionSalt). When this code finds a state
// with EncryptionSalt set but no EncryptionKey, it bootstraps a fresh
// EncryptionKey, decrypts every existing env_data entry under the old
// key, re-encrypts under the new key, and persists. After that one-shot
// migration, EncryptionSalt is unused.

const (
	encryptionKeySize = 32 // AES-256
	encryptedV2Prefix = "v2:"

	// Legacy Argon2id parameters - kept ONLY for migrating old ciphertext.
	// New deployments never run this code path.
	legacyArgon2Time    uint32 = 1
	legacyArgon2Memory  uint32 = 64 * 1024
	legacyArgon2Threads uint8  = 4
	legacyArgon2KeyLen  uint32 = 32
)

// ensureEncryptionKey generates a fresh 32-byte EncryptionKey if not yet
// set. Called by the bootstrap node on first encrypt; joining nodes
// adopt the existing key via the /api/join response and never call this.
// Caller must hold a.stateMu.Lock().
func (a *Agent) ensureEncryptionKey() error {
	if len(a.state.EncryptionKey) == encryptionKeySize {
		return nil
	}
	key := make([]byte, encryptionKeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return fmt.Errorf("generate encryption key: %w", err)
	}
	a.state.EncryptionKey = key
	return nil
}

// currentKey returns the active AES key. Read-only on state; safe to
// call without holding stateMu (uses RLock internally).
func (a *Agent) currentKey() ([]byte, error) {
	a.stateMu.RLock()
	key := a.state.EncryptionKey
	a.stateMu.RUnlock()
	if len(key) != encryptionKeySize {
		return nil, fmt.Errorf("encryption key not initialized: cluster has not bootstrapped or join did not complete")
	}
	return key, nil
}

// legacyKey returns the Argon2id-derived key from JETTY_SECRET and the
// stored EncryptionSalt. Used exactly once during migration to read
// pre-existing env_data; never used for new ciphertext.
func (a *Agent) legacyKey() ([]byte, error) {
	if a.clusterSecret == "" {
		return nil, fmt.Errorf("legacy decrypt: JETTY_SECRET not set")
	}
	a.stateMu.RLock()
	salt := append([]byte(nil), a.state.EncryptionSalt...)
	a.stateMu.RUnlock()
	if len(salt) == 0 {
		return nil, fmt.Errorf("legacy decrypt: no EncryptionSalt in state")
	}
	return argon2.IDKey([]byte(a.clusterSecret), salt, legacyArgon2Time, legacyArgon2Memory, legacyArgon2Threads, legacyArgon2KeyLen), nil
}

// encryptValue encrypts a plaintext value under EncryptionKey. Returns
// "v2:"-prefixed base64 ciphertext.
func (a *Agent) encryptValue(plaintext string) (string, error) {
	a.stateMu.Lock()
	if err := a.ensureEncryptionKey(); err != nil {
		a.stateMu.Unlock()
		return "", err
	}
	a.stateMu.Unlock()

	key, err := a.currentKey()
	if err != nil {
		return "", err
	}
	return encryptWithKey(key, plaintext)
}

// decryptValue decrypts a "v2:"-prefixed base64 ciphertext using
// EncryptionKey.
func (a *Agent) decryptValue(encrypted string) (string, error) {
	key, err := a.currentKey()
	if err != nil {
		return "", err
	}
	return decryptWithKey(key, encrypted)
}

// encryptWithKey is the keyed-primitive variant. Used by encryptValue
// and the migration helper.
func encryptWithKey(key []byte, plaintext string) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create GCM: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}
	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return encryptedV2Prefix + base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decryptWithKey is the keyed-primitive variant.
func decryptWithKey(key []byte, encrypted string) (string, error) {
	if !strings.HasPrefix(encrypted, encryptedV2Prefix) {
		return "", fmt.Errorf("unsupported ciphertext format: missing %q prefix", encryptedV2Prefix)
	}
	encrypted = encrypted[len(encryptedV2Prefix):]
	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", fmt.Errorf("decode base64: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create GCM: %w", err)
	}
	if len(ciphertext) < gcm.NonceSize() {
		return "", fmt.Errorf("ciphertext too short")
	}
	nonce := ciphertext[:gcm.NonceSize()]
	ciphertext = ciphertext[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt: %w", err)
	}
	return string(plaintext), nil
}

// migrateLegacyEnvData performs a one-shot re-encrypt of env_data under
// the new EncryptionKey. Runs at startup if the state has both an old
// EncryptionSalt and an env_data map but no EncryptionKey. Idempotent.
// On any decryption failure leaves the entry alone and logs - so a
// hand-written stale entry doesn't block the rest of the migration.
func (a *Agent) migrateLegacyEnvData() {
	a.stateMu.Lock()
	hasLegacy := len(a.state.EncryptionSalt) > 0
	hasNewKey := len(a.state.EncryptionKey) == encryptionKeySize
	envCount := len(a.state.EnvData)
	a.stateMu.Unlock()

	if hasNewKey || !hasLegacy || envCount == 0 {
		return
	}

	oldKey, err := a.legacyKey()
	if err != nil {
		logInfof("env migration: cannot derive legacy key (%v) - leaving env_data alone; you may need to re-enter env vars", err)
		return
	}

	a.stateMu.Lock()
	// Generate the new key first so encryption below uses it.
	if err := a.ensureEncryptionKey(); err != nil {
		a.stateMu.Unlock()
		logErrorf("env migration: failed to generate new encryption key: %v", err)
		return
	}
	newKey := append([]byte(nil), a.state.EncryptionKey...)
	encrypted := make(map[string]string, len(a.state.EnvData))
	for k, v := range a.state.EnvData {
		encrypted[k] = v
	}
	a.stateMu.Unlock()

	migrated := 0
	rewritten := make(map[string]string, len(encrypted))
	for k, v := range encrypted {
		plaintext, err := decryptWithKey(oldKey, v)
		if err != nil {
			logErrorf("env migration: skipping %s (legacy decrypt failed: %v)", k, err)
			rewritten[k] = v // keep as-is so caller still sees something
			continue
		}
		newCt, err := encryptWithKey(newKey, plaintext)
		if err != nil {
			logInfof("env migration: re-encrypt failed for %s: %v", k, err)
			rewritten[k] = v
			continue
		}
		rewritten[k] = newCt
		migrated++
	}

	a.stateMu.Lock()
	if bytes.Equal(a.state.EncryptionKey, newKey) {
		a.state.EnvData = rewritten
	}
	a.stateMu.Unlock()
	a.saveState()
	logInfof("env migration: re-encrypted %d/%d env vars under new EncryptionKey", migrated, len(encrypted))
}

// bootstrapKeys generates this node's per-cluster credentials on first
// boot:
//
//   - SelfAPIKey: 32-byte random hex, this node's outbound peer auth
//     credential. Once issued, never rotates automatically (operator
//     can do so by clearing it from state).
//   - EncryptionKey: 32 random bytes, the cluster-wide AES key for
//     env_data. Generated on the bootstrap node only; joining nodes
//     get this via the /api/join response and never call ensureEncryption
//     Key here (their state already has it).
//   - AdminKey: the operator/dashboard credential. Bootstrapped from
//     JETTY_SECRET on the first node, persisted, and propagated to
//     joiners via /api/join. If state has no AdminKey AND JETTY_SECRET
//     is empty AND no peers exist (i.e. fresh first-node), we leave it
//     empty - the agent runs unauthenticated and Start() prints a loud
//     warning, same as before.
//
// Idempotent: running on a state that already has these fields is a
// no-op.
func (a *Agent) bootstrapKeys() {
	a.stateMu.Lock()
	dirty := false

	if a.state.SelfAPIKey == "" {
		key, err := generateAPIKey()
		if err != nil {
			logErrorf("bootstrap: failed to generate SelfAPIKey: %v", err)
		} else {
			a.state.SelfAPIKey = key
			dirty = true
			logInfof("bootstrap: generated SelfAPIKey")
		}
	}

	// AdminKey: only bootstrapped on the first ever node (no peers,
	// no existing AdminKey). Joiners get it from /api/join.
	if a.state.AdminKey == "" && a.clusterSecret != "" && len(a.state.Peers) == 0 && a.joinURL == "" {
		a.state.AdminKey = a.clusterSecret
		dirty = true
		logInfof("bootstrap: AdminKey initialized from JETTY_SECRET")
	}

	// EncryptionKey: same constraint - generated only on the first
	// node. Joiners receive it via /api/join.
	if len(a.state.EncryptionKey) != encryptionKeySize && len(a.state.Peers) == 0 && a.joinURL == "" {
		if err := a.ensureEncryptionKey(); err != nil {
			logErrorf("bootstrap: failed to generate EncryptionKey: %v", err)
		} else {
			dirty = true
			logInfof("bootstrap: generated EncryptionKey")
		}
	}
	a.stateMu.Unlock()

	if dirty {
		a.saveState()
	}
}

// generateAPIKey returns a 64-char hex string sourced from crypto/rand
// (32 bytes of entropy). Used for SelfAPIKey, peer APIKeys, and
// JoinToken IDs.
func generateAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// getDecryptedEnv returns all environment variables decrypted.
func (a *Agent) getDecryptedEnv() (map[string]string, error) {
	a.stateMu.RLock()
	encrypted := make(map[string]string, len(a.state.EnvData))
	for k, v := range a.state.EnvData {
		encrypted[k] = v
	}
	a.stateMu.RUnlock()

	result := make(map[string]string, len(encrypted))
	for key, ciphertext := range encrypted {
		value, err := a.decryptValue(ciphertext)
		if err != nil {
			return nil, fmt.Errorf("decrypt %s: %w", key, err)
		}
		result[key] = value
	}
	return result, nil
}
