package agent

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateRegistryAuth(t *testing.T) {
	cases := []struct {
		name    string
		ra      *RegistryAuth
		wantErr bool
	}{
		{"nil is valid (default behavior)", nil, false},
		{"full valid", &RegistryAuth{Registry: "ghcr.io", Username: "bot", TokenRef: "GHCR_FOO"}, false},
		{"username optional", &RegistryAuth{Registry: "ghcr.io", TokenRef: "GHCR_FOO"}, false},
		{"missing registry", &RegistryAuth{TokenRef: "GHCR_FOO"}, true},
		{"missing token_ref", &RegistryAuth{Registry: "ghcr.io"}, true},
		{"registry with slash", &RegistryAuth{Registry: "ghcr.io/org", TokenRef: "GHCR_FOO"}, true},
		{"registry with space", &RegistryAuth{Registry: "ghcr.io ", TokenRef: "GHCR_FOO"}, true},
		{"token_ref starting with digit", &RegistryAuth{Registry: "ghcr.io", TokenRef: "1FOO"}, true},
		{"token_ref with dash", &RegistryAuth{Registry: "ghcr.io", TokenRef: "GHCR-FOO"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateRegistryAuth(c.ra)
			if (err != nil) != c.wantErr {
				t.Fatalf("validateRegistryAuth(%+v) err=%v, wantErr=%v", c.ra, err, c.wantErr)
			}
		})
	}
}

func TestWriteRegistryConfig(t *testing.T) {
	env := map[string]string{"GHCR_FOO": "ghp_secrettoken"}

	t.Run("writes config.json with base64 auth", func(t *testing.T) {
		ra := &RegistryAuth{Registry: "ghcr.io", Username: "foo-bot", TokenRef: "GHCR_FOO"}
		dir, err := writeRegistryConfig(ra, env)
		if err != nil {
			t.Fatalf("writeRegistryConfig: %v", err)
		}
		defer os.RemoveAll(dir)

		blob, err := os.ReadFile(filepath.Join(dir, "config.json"))
		if err != nil {
			t.Fatalf("read config.json: %v", err)
		}
		var cfg struct {
			Auths map[string]struct {
				Auth string `json:"auth"`
			} `json:"auths"`
		}
		if err := json.Unmarshal(blob, &cfg); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		entry, ok := cfg.Auths["ghcr.io"]
		if !ok {
			t.Fatalf("no auth entry for ghcr.io: %s", blob)
		}
		decoded, _ := base64.StdEncoding.DecodeString(entry.Auth)
		if string(decoded) != "foo-bot:ghp_secrettoken" {
			t.Fatalf("auth = %q, want foo-bot:ghp_secrettoken", decoded)
		}
	})

	t.Run("defaults username to x-access-token", func(t *testing.T) {
		ra := &RegistryAuth{Registry: "ghcr.io", TokenRef: "GHCR_FOO"}
		dir, err := writeRegistryConfig(ra, env)
		if err != nil {
			t.Fatalf("writeRegistryConfig: %v", err)
		}
		defer os.RemoveAll(dir)
		blob, _ := os.ReadFile(filepath.Join(dir, "config.json"))
		if !strings.Contains(string(blob), base64.StdEncoding.EncodeToString([]byte("x-access-token:ghp_secrettoken"))) {
			t.Fatalf("expected x-access-token default in %s", blob)
		}
	})

	t.Run("missing token_ref in env errors", func(t *testing.T) {
		ra := &RegistryAuth{Registry: "ghcr.io", TokenRef: "MISSING"}
		if _, err := writeRegistryConfig(ra, env); err == nil {
			t.Fatal("expected error for missing token_ref, got nil")
		}
	})

	t.Run("config.json has 0600 perms", func(t *testing.T) {
		ra := &RegistryAuth{Registry: "ghcr.io", TokenRef: "GHCR_FOO"}
		dir, err := writeRegistryConfig(ra, env)
		if err != nil {
			t.Fatalf("writeRegistryConfig: %v", err)
		}
		defer os.RemoveAll(dir)
		fi, err := os.Stat(filepath.Join(dir, "config.json"))
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if fi.Mode().Perm() != 0600 {
			t.Fatalf("perms = %o, want 600", fi.Mode().Perm())
		}
	})
}
