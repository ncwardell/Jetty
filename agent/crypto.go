package agent

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
)

// =============================================================================
// Encryption (AES-256-GCM)
// =============================================================================

// deriveKey derives a 32-byte AES key from the cluster secret using SHA-256.
func (a *Agent) deriveKey() []byte {
	hash := sha256.Sum256([]byte(a.clusterSecret))
	return hash[:]
}

// encryptValue encrypts a plaintext value using AES-GCM with the cluster secret.
// Returns base64-encoded ciphertext.
func (a *Agent) encryptValue(plaintext string) (string, error) {
	if a.clusterSecret == "" {
		return "", fmt.Errorf("cluster secret not configured")
	}

	key := a.deriveKey()
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create GCM: %w", err)
	}

	// Generate random nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}

	// Encrypt and prepend nonce to ciphertext
	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decryptValue decrypts a base64-encoded ciphertext using AES-GCM with the cluster secret.
// Returns the plaintext value.
func (a *Agent) decryptValue(encrypted string) (string, error) {
	if a.clusterSecret == "" {
		return "", fmt.Errorf("cluster secret not configured")
	}

	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", fmt.Errorf("decode base64: %w", err)
	}

	key := a.deriveKey()
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

	// Extract nonce and decrypt
	nonce := ciphertext[:gcm.NonceSize()]
	ciphertext = ciphertext[gcm.NonceSize():]

	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt: %w", err)
	}

	return string(plaintext), nil
}

// getDecryptedEnv returns all environment variables decrypted.
// Returns a map of key -> decrypted value.
func (a *Agent) getDecryptedEnv() (map[string]string, error) {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()

	result := make(map[string]string)
	for key, encrypted := range a.state.EnvData {
		value, err := a.decryptValue(encrypted)
		if err != nil {
			return nil, fmt.Errorf("decrypt %s: %w", key, err)
		}
		result[key] = value
	}
	return result, nil
}
