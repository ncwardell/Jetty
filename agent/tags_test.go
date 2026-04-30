package agent

import (
	"reflect"
	"testing"
)

func TestNormalizeTags_HappyPath(t *testing.T) {
	got, bad := normalizeTags([]string{"prod", "MEDIA", "  experimental  ", "prod"})
	if bad != "" {
		t.Fatalf("unexpected bad tag: %q", bad)
	}
	want := []string{"experimental", "media", "prod"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestNormalizeTags_RejectsInvalid(t *testing.T) {
	cases := []string{
		"WITH SPACES",
		"slash/in/tag",
		"",                   // empty after trim is dropped silently, see below
		"period.in.tag",      // periods not allowed (could collide with FQDNs)
		"-leading-dash",      // must start with alphanumeric
		"trailing dash bad ", // space
	}
	for _, in := range cases {
		got, bad := normalizeTags([]string{"valid", in})
		if in == "" {
			// Empty/whitespace-only entries are silently dropped, not rejected.
			if bad != "" {
				t.Errorf("empty tag should be dropped silently, got bad=%q", bad)
			}
			if !reflect.DeepEqual(got, []string{"valid"}) {
				t.Errorf("empty tag mishandled: got %v", got)
			}
			continue
		}
		if bad == "" {
			t.Errorf("expected %q to be rejected", in)
		}
	}
}

func TestNormalizeTags_DedupesAndSorts(t *testing.T) {
	got, _ := normalizeTags([]string{"zeta", "alpha", "alpha", "ALPHA", "mid"})
	want := []string{"alpha", "mid", "zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestNormalizeTags_NilOrEmptyReturnsNil(t *testing.T) {
	if got, _ := normalizeTags(nil); got != nil {
		t.Errorf("nil input should return nil, got %v", got)
	}
	if got, _ := normalizeTags([]string{}); got != nil {
		t.Errorf("empty input should return nil, got %v", got)
	}
	if got, _ := normalizeTags([]string{"  ", ""}); got != nil {
		t.Errorf("whitespace-only input should return nil, got %v", got)
	}
}

func TestNormalizeTags_AcceptsColonNamespacing(t *testing.T) {
	// Operators may want pseudo-namespacing like env:prod, team:backend.
	// We don't impose key=value semantics, but the colon character is
	// allowed.
	got, bad := normalizeTags([]string{"env:prod", "team:backend"})
	if bad != "" {
		t.Fatalf("unexpected bad tag: %q", bad)
	}
	if !reflect.DeepEqual(got, []string{"env:prod", "team:backend"}) {
		t.Errorf("got %v", got)
	}
}
