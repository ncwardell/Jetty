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
// Encryption (AES-256-GCM with Argon2id key derivation)
// =============================================================================
//
// The cluster secret is run through Argon2id with a per-cluster salt to derive
// the AES key. The salt is generated once on the bootstrap node and then
// propagated to joining nodes via the /api/join response. Without the salt,
// a stolen state.json reveals nothing about the secret beyond what brute-force
// against an Argon2id-hardened key can recover - which is the point.
//
// Ciphertext format: "v2:" + base64(nonce || gcm_seal(plaintext)).
// The "v2:" prefix exists so future format changes (parameter bumps, KDF
// migration) can be detected without breaking existing values.

const (
	// Argon2id parameters. Picked for "good-enough" defense on a typical
	// cluster node: ~1 vCPU-second per derivation, 64 MiB memory.
	argon2Time    uint32 = 1
	argon2Memory  uint32 = 64 * 1024
	argon2Threads uint8  = 4
	argon2KeyLen  uint32 = 32

	encryptionSaltSize = 32
	encryptedV2Prefix  = "v2:"
)

// ensureEncryptionSalt generates the cluster-wide salt if not yet set.
// Called by the bootstrap node on first encrypt; joining nodes adopt the
// existing salt via the /api/join response and never call this.
// Caller must hold a.stateMu.Lock().
func (a *Agent) ensureEncryptionSalt() error {
	if len(a.state.EncryptionSalt) >= encryptionSaltSize {
		return nil
	}
	salt := make([]byte, encryptionSaltSize)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return fmt.Errorf("generate encryption salt: %w", err)
	}
	a.state.EncryptionSalt = salt
	return nil
}

// deriveKey derives a 32-byte AES key from the cluster secret and salt.
// Memoizes the result; the (secret, salt) pair only changes once at bootstrap.
func (a *Agent) deriveKey() ([]byte, error) {
	if a.clusterSecret == "" {
		return nil, fmt.Errorf("cluster secret not configured")
	}
	a.stateMu.RLock()
	salt := a.state.EncryptionSalt
	a.stateMu.RUnlock()
	if len(salt) == 0 {
		return nil, fmt.Errorf("encryption salt not initialized: cluster has not bootstrapped or join did not complete")
	}

	a.derivedKeyMu.Lock()
	defer a.derivedKeyMu.Unlock()
	if bytes.Equal(a.derivedKeySalt, salt) && len(a.derivedKey) == int(argon2KeyLen) {
		return a.derivedKey, nil
	}
	saltCopy := append([]byte(nil), salt...)
	key := argon2.IDKey([]byte(a.clusterSecret), saltCopy, argon2Time, argon2Memory, argon2Threads, argon2KeyLen)
	a.derivedKey = key
	a.derivedKeySalt = saltCopy
	return key, nil
}

// encryptValue encrypts a plaintext value using AES-GCM.
// Returns "v2:"-prefixed base64 ciphertext.
func (a *Agent) encryptValue(plaintext string) (string, error) {
	// Bootstrap path: generate salt on first use if missing.
	a.stateMu.Lock()
	if err := a.ensureEncryptionSalt(); err != nil {
		a.stateMu.Unlock()
		return "", err
	}
	a.stateMu.Unlock()

	key, err := a.deriveKey()
	if err != nil {
		return "", err
	}

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

// decryptValue decrypts a "v2:"-prefixed base64 ciphertext using AES-GCM.
func (a *Agent) decryptValue(encrypted string) (string, error) {
	if !strings.HasPrefix(encrypted, encryptedV2Prefix) {
		return "", fmt.Errorf("unsupported ciphertext format: missing %q prefix", encryptedV2Prefix)
	}
	encrypted = encrypted[len(encryptedV2Prefix):]

	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", fmt.Errorf("decode base64: %w", err)
	}

	key, err := a.deriveKey()
	if err != nil {
		return "", err
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
