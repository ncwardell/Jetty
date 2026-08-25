package agent

import (
	"encoding/json"
	"testing"
)

// NodeMeta used to handle memberlist's 512-byte cap with `d.meta[:limit]`,
// slicing JSON mid-string. That publishes a payload every peer fails to
// parse - the node effectively vanishes from gossip while appearing to
// participate. Truncating a serialized structure is corruption, not
// degradation.
//
// Latent while the payload happened to fit, and a live hazard the moment
// anything is added to NodeMeta - which the JettyOS capability-labels work
// intends to do.

func TestNodeMetaNeverPublishesInvalidJSON(t *testing.T) {
	d := &jettyDelegate{agent: newTestAgent("s")}

	full, err := json.Marshal(NodeMeta{
		ID: "abcdef0123456789", Name: "some-node-name", IP: "100.96.0.2",
		Version: "0.0.4-dev", Arch: "arm64", APIPort: 6880,
		APIKey: "aVeryLongApiKeyValueThatTakesUpQuiteALotOfSpace12345",
	})
	if err != nil {
		t.Fatal(err)
	}
	d.meta = full

	// Every limit from "plenty" down to "impossible".
	for limit := len(full) + 10; limit >= 0; limit-- {
		out := d.NodeMeta(limit)
		if out == nil {
			continue // refusing to publish is the correct degradation
		}
		if len(out) > limit {
			t.Fatalf("limit=%d: returned %d bytes, over the cap", limit, len(out))
		}
		var parsed NodeMeta
		if err := json.Unmarshal(out, &parsed); err != nil {
			t.Fatalf("limit=%d: published unparseable metadata (%q): %v", limit, out, err)
		}
		// Whatever survives must keep the fields peers act on.
		if parsed.ID == "" {
			t.Fatalf("limit=%d: shed the node ID; peers key on it", limit)
		}
	}
}

func TestNodeMetaShedsOptionalFieldsBeforeGivingUp(t *testing.T) {
	d := &jettyDelegate{agent: newTestAgent("s")}
	meta := NodeMeta{
		ID: "abcdef0123456789", Name: "a-fairly-long-hostname", IP: "100.96.0.2",
		Version: "0.0.4-dev-with-a-long-suffix", Arch: "arm64", APIPort: 6880,
		APIKey: "key",
	}
	full, _ := json.Marshal(meta)
	d.meta = full

	// A limit that only fits once the informational fields are gone.
	out := d.NodeMeta(len(full) - 20)
	if out == nil {
		t.Fatal("gave up instead of shedding optional fields")
	}
	var parsed NodeMeta
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("shed payload is not valid JSON: %v", err)
	}
	// Arch drives arch-gated placement and must survive; Version must not.
	if parsed.Arch != "arm64" {
		t.Error("Arch was shed, but placement decisions depend on it")
	}
	if parsed.Version != "" {
		t.Error("Version should be shed first - it is purely informational")
	}
}

func TestNodeMetaPassesThroughWhenItFits(t *testing.T) {
	d := &jettyDelegate{agent: newTestAgent("s")}
	full, _ := json.Marshal(NodeMeta{ID: "n1", IP: "100.96.0.2", APIPort: 6880})
	d.meta = full

	if got := d.NodeMeta(512); string(got) != string(full) {
		t.Errorf("payload was altered despite fitting: %q vs %q", got, full)
	}
}
