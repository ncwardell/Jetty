package agent

import (
	"strings"
	"sync"
	"testing"

	"golang.org/x/crypto/argon2"
)

// newTestAgent returns a minimally-initialized Agent suitable for unit
// testing crypto/state logic. It does NOT touch the host network or fs.
func newTestAgent(secret string) *Agent {
	a := &Agent{
		clusterSecret: secret,
		state:         NewState(),
		stateMu:       sync.RWMutex{},
		dataDir:       "", // saveState is not invoked from these tests
	}
	return a
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	a := newTestAgent("")

	plain := "postgres://user:pass@db:5432/app"
	enc, err := a.encryptValue(plain)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if !strings.HasPrefix(enc, encryptedV2Prefix) {
		t.Fatalf("expected v2 prefix, got %q", enc[:min(8, len(enc))])
	}
	if len(a.state.EncryptionKey) != encryptionKeySize {
		t.Fatalf("expected %d-byte EncryptionKey to be auto-generated", encryptionKeySize)
	}

	dec, err := a.decryptValue(enc)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if dec != plain {
		t.Fatalf("got %q, want %q", dec, plain)
	}
}

func TestEncryptValueProducesUniqueCiphertext(t *testing.T) {
	a := newTestAgent("")
	plain := "same-input"

	a1, err := a.encryptValue(plain)
	if err != nil {
		t.Fatal(err)
	}
	a2, err := a.encryptValue(plain)
	if err != nil {
		t.Fatal(err)
	}
	if a1 == a2 {
		t.Fatalf("identical ciphertext for identical plaintext - nonce isn't random")
	}
}

func TestDecryptRejectsMissingPrefix(t *testing.T) {
	a := newTestAgent("")
	enc, err := a.encryptValue("x")
	if err != nil {
		t.Fatal(err)
	}
	rawNoPrefix := strings.TrimPrefix(enc, encryptedV2Prefix)

	if _, err := a.decryptValue(rawNoPrefix); err == nil {
		t.Fatal("expected error decrypting unprefixed ciphertext")
	}
}

func TestDecryptWithDifferentKeyFails(t *testing.T) {
	a1 := newTestAgent("")
	enc, err := a1.encryptValue("secret data")
	if err != nil {
		t.Fatal(err)
	}

	// Second agent has its own freshly generated key.
	a2 := newTestAgent("")
	if _, err := a2.encryptValue("force-key-gen"); err != nil {
		t.Fatal(err)
	}
	if _, err := a2.decryptValue(enc); err == nil {
		t.Fatal("decrypt with wrong key should fail")
	}
}

func TestDecryptWithoutKeyFails(t *testing.T) {
	a := newTestAgent("")
	// Have not called encryptValue, so EncryptionKey is empty.
	if _, err := a.decryptValue("v2:fakebytes"); err == nil {
		t.Fatal("expected error decrypting before EncryptionKey is initialized")
	}
}

// TestMigrateLegacyEnvData verifies that a state created under the old
// Argon2id derivation is upgraded in place when the agent boots: legacy
// ciphertexts get re-encrypted under a freshly minted EncryptionKey.
func TestMigrateLegacyEnvData(t *testing.T) {
	t.Setenv("JETTY_DATA_DIR", t.TempDir())
	a := newTestAgent("legacy-secret")
	a.dataDir = t.TempDir()

	// Build a legacy salt + key and encrypt one entry under it.
	legacySalt := make([]byte, encryptionKeySize)
	for i := range legacySalt {
		legacySalt[i] = byte(i)
	}
	a.state.EncryptionSalt = legacySalt
	legacyKey := argon2.IDKey([]byte("legacy-secret"), legacySalt, legacyArgon2Time, legacyArgon2Memory, legacyArgon2Threads, legacyArgon2KeyLen)

	plain := "postgres://prod"
	legacyCT, err := encryptWithKey(legacyKey, plain)
	if err != nil {
		t.Fatal(err)
	}
	a.state.EnvData["DB_URL"] = legacyCT

	// Migrate.
	a.migrateLegacyEnvData()

	if len(a.state.EncryptionKey) != encryptionKeySize {
		t.Fatal("migration did not generate EncryptionKey")
	}
	// After migration, the entry must decrypt under the NEW key.
	got, err := a.decryptValue(a.state.EnvData["DB_URL"])
	if err != nil {
		t.Fatalf("decrypt after migration: %v", err)
	}
	if got != plain {
		t.Fatalf("got %q, want %q", got, plain)
	}
	// The stored ciphertext should have been rewritten.
	if a.state.EnvData["DB_URL"] == legacyCT {
		t.Fatal("ciphertext was not re-encrypted")
	}
}
