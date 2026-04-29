package agent

import (
	"strings"
	"testing"
)

const sampleCompose = `services:
  web:
    image: nginx
    ports:
      - "80:80"
  worker:
    image: myapp
    depends_on:
      - web
`

func TestParseComposeServiceNames(t *testing.T) {
	names, err := parseComposeServiceNames([]byte(sampleCompose))
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "web" || names[1] != "worker" {
		t.Fatalf("got %v, want [web worker]", names)
	}
}

func TestParseComposeServiceNamesEmpty(t *testing.T) {
	for _, in := range []string{"", "version: '3'", "services: {}"} {
		names, err := parseComposeServiceNames([]byte(in))
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if len(names) != 0 {
			t.Fatalf("%q: got %v, want empty", in, names)
		}
	}
}

func TestBuildHostsOverrideEmits(t *testing.T) {
	out, err := buildHostsOverride(
		[]string{"web", "worker"},
		map[string]string{
			"postgres":  "10.100.0.51",
			"wordpress": "10.100.0.50",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, want := range []string{
		"services:",
		"web:",
		"worker:",
		"extra_hosts:",
		"postgres:10.100.0.51",
		"wordpress:10.100.0.50",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("output missing %q\nfull output:\n%s", want, s)
		}
	}
}

func TestBuildHostsOverrideDeterministic(t *testing.T) {
	// Same input must produce byte-identical output, so the file's hash is
	// stable and the reconciler doesn't trigger spurious refreshes.
	hosts := map[string]string{
		"a": "10.0.0.1",
		"b": "10.0.0.2",
		"c": "10.0.0.3",
	}
	out1, _ := buildHostsOverride([]string{"web"}, hosts)
	out2, _ := buildHostsOverride([]string{"web"}, hosts)
	if string(out1) != string(out2) {
		t.Fatalf("non-deterministic output\nrun1:\n%s\nrun2:\n%s", out1, out2)
	}
}

func TestBuildHostsOverrideEmptyHosts(t *testing.T) {
	out, err := buildHostsOverride([]string{"web"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "services") {
		t.Errorf("expected a placeholder, got %q", out)
	}
	// Should not contain an extra_hosts: key (the comment mentioning it is fine)
	if strings.Contains(string(out), "extra_hosts:") {
		t.Errorf("empty-hosts output should not contain extra_hosts: key: %s", out)
	}
}

func TestComposeOverridePassesThroughDockerCompose(t *testing.T) {
	// Sanity check: the generated YAML is something compose would accept.
	// We can't run compose in tests, but parsing it back as compose-shaped
	// YAML catches obvious malformation.
	out, err := buildHostsOverride(
		[]string{"web"},
		map[string]string{"db": "10.100.0.10"},
	)
	if err != nil {
		t.Fatal(err)
	}
	names, err := parseComposeServiceNames(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "web" {
		t.Errorf("round-trip mismatch: %v", names)
	}
}
