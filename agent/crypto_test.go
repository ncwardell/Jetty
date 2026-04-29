package agent

import (
	"strings"
	"sync"
	"testing"
)

// newTestAgent returns a minimally-initialized Agent suitable for unit
// testing crypto/state logic. It does NOT touch the host network or fs.
func newTestAgent(secret string) *Agent {
	return &Agent{
		clusterSecret: secret,
		state:         NewState(),
		stateMu:       sync.RWMutex{},
	}
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	a := newTestAgent("hunter2")

	plain := "postgres://user:pass@db:5432/app"
	enc, err := a.encryptValue(plain)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if !strings.HasPrefix(enc, encryptedV2Prefix) {
		t.Fatalf("expected v2 prefix, got %q", enc[:min(8, len(enc))])
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
	a := newTestAgent("hunter2")
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
	a := newTestAgent("hunter2")
	// Run encrypt to make sure the salt is generated, then strip the prefix.
	enc, err := a.encryptValue("x")
	if err != nil {
		t.Fatal(err)
	}
	rawNoPrefix := strings.TrimPrefix(enc, encryptedV2Prefix)

	if _, err := a.decryptValue(rawNoPrefix); err == nil {
		t.Fatal("expected error decrypting unprefixed ciphertext")
	}
}

func TestDecryptWithDifferentSecretFails(t *testing.T) {
	a1 := newTestAgent("right-secret")
	enc, err := a1.encryptValue("secret data")
	if err != nil {
		t.Fatal(err)
	}

	// Different agent, different secret, but copy the salt so the format
	// passes early validation.
	a2 := newTestAgent("wrong-secret")
	a2.state.EncryptionSalt = a1.state.EncryptionSalt

	if _, err := a2.decryptValue(enc); err == nil {
		t.Fatal("decrypt with wrong secret should fail")
	}
}

func TestEncryptWithoutSecretFails(t *testing.T) {
	a := newTestAgent("")
	if _, err := a.encryptValue("x"); err == nil {
		t.Fatal("encrypt without cluster secret should fail")
	}
}

func TestDeriveKeyMemoization(t *testing.T) {
	a := newTestAgent("hunter2")
	if err := func() error {
		a.stateMu.Lock()
		defer a.stateMu.Unlock()
		return a.ensureEncryptionSalt()
	}(); err != nil {
		t.Fatal(err)
	}

	k1, err := a.deriveKey()
	if err != nil {
		t.Fatal(err)
	}
	k2, err := a.deriveKey()
	if err != nil {
		t.Fatal(err)
	}
	// Same memoized slice (Argon2id is expensive; second call must hit cache).
	if &k1[0] != &k2[0] {
		t.Fatal("deriveKey didn't return memoized key")
	}
}

