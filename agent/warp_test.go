package agent

import (
	"sync"
	"testing"
)

// a.ip was a plain string field written by the WARP monitor goroutine and read
// from ~40 places, none of which held a common lock. A genuine data race that
// -race would eventually surface on an unrelated change, and which
// ensurePeerTunnel hit from five call sites once route reconciliation moved off
// stateMu.
//
// Run under -race: without the atomic this fails.
func TestWarpIPIsSafeUnderConcurrentAccess(t *testing.T) {
	a := newTestAgent("s")

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: the WARP monitor changing the address.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			a.setWarpIP("100.96.0." + itoa(i%250+1))
		}
	}()

	// Readers: everything else in the agent.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				_ = a.warpIP()
			}
		}()
	}

	// Let the readers finish, then stop the writer.
	go func() { <-stop }()
	for i := 0; i < 200; i++ {
		_ = a.warpIP()
	}
	close(stop)
	wg.Wait()
}

func TestWarpIPDefaultsToEmpty(t *testing.T) {
	// Read before any WARP connection must not panic on a nil pointer - the
	// agent reads this during startup before detectWarpIP runs.
	a := &Agent{}
	if got := a.warpIP(); got != "" {
		t.Errorf("warpIP() on a fresh agent = %q, want empty", got)
	}
}

func TestWarpIPRoundTrips(t *testing.T) {
	a := newTestAgent("s")
	a.setWarpIP("100.96.0.42")
	if got := a.warpIP(); got != "100.96.0.42" {
		t.Errorf("warpIP() = %q, want 100.96.0.42", got)
	}
	a.setWarpIP("")
	if got := a.warpIP(); got != "" {
		t.Errorf("warpIP() after clearing = %q, want empty", got)
	}
}
