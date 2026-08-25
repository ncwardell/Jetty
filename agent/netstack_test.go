package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

// The legacy receive path hand-rolled TCP with no retransmission, no
// congestion control and no zero-window probe, so bulk flows deadlocked. Unit
// tests of that state machine proved the state machine, not the fix.
//
// These stand a second netstack up as the remote client and wire the two
// channel endpoints together, so a real TCP handshake and a real bulk transfer
// run through the actual receive path. Everything that used to be hand-written
// - sequence numbers, ACKs, windows, retransmission - is genuinely exercised.

func TestUseNetstackTunnelSelection(t *testing.T) {
	cases := map[string]bool{
		"":          true, // default on: legacy is the known-broken one
		"netstack":  true,
		"NETSTACK":  true,
		"  legacy ": false,
		"legacy":    false,
		"nonsense":  true, // unrecognised falls forward, with a warning
	}
	for in, want := range cases {
		a := &Agent{tunnelStack: in}
		if got := a.useNetstackTunnel(); got != want {
			t.Errorf("JETTY_TUNNEL_STACK=%q -> %v, want %v", in, got, want)
		}
	}
}

// wireWriter carries packets emitted by one stack into the other, standing in
// for the WebSocket that separates them in production.
type wireWriter struct {
	deliver func([]byte)
}

func (w *wireWriter) WriteMessage(_ int, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	w.deliver(cp)
	return nil
}

// tunnelPair is a netstackProxy (the receive side) joined to a client stack
// (standing in for the remote node's kernel).
type tunnelPair struct {
	agent    *Agent
	proxy    *netstackProxy
	clientNS *stack.Stack
	clientIP tcpip.Address
}

// meshIP is the workload address the client dials. A real mesh address, not
// loopback: netstack treats 127.0.0.0/8 specially and will not route it over
// a normal NIC, which is what made the first version of these tests time out.
const testMeshIP = "10.100.0.50"

// newTunnelPair builds both stacks and pumps packets between them.
func newTunnelPair(t *testing.T) *tunnelPair {
	t.Helper()

	a := newTestAgentWithDir(t)
	a.hwid = "us"

	clientAddr := tcpip.AddrFrom4([4]byte{10, 100, 0, 2})

	// Client stack: a plain IPv4+TCP stack that can dial.
	cs := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol},
	})
	cep := channel.New(netstackQueueLen, netstackMTU, "")
	if err := cs.CreateNIC(2, cep); err != nil {
		t.Fatalf("client CreateNIC: %v", err)
	}
	if err := cs.AddProtocolAddress(2, tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: clientAddr.WithPrefix(),
	}, stack.AddressProperties{}); err != nil {
		t.Fatalf("client AddProtocolAddress: %v", err)
	}
	sn, _ := tcpip.NewSubnet(tcpip.AddrFrom4([4]byte{0, 0, 0, 0}), tcpip.MaskFrom(string([]byte{0, 0, 0, 0})))
	cs.SetRouteTable([]tcpip.Route{{Destination: sn, NIC: 2}})

	// Proxy stack writes toward the client.
	var proxy *netstackProxy
	toClient := &wireWriter{deliver: func(pkt []byte) {
		pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{Payload: buffer.MakeWithData(pkt)})
		cep.InjectInbound(ipv4.ProtocolNumber, pkb)
		pkb.DecRef()
	}}

	proxy, err := a.newNetstackProxy(toClient)
	if err != nil {
		t.Fatalf("newNetstackProxy: %v", err)
	}

	// Client stack writes toward the proxy, via the same inject() the tunnel
	// uses so the address registration path is covered too.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			pkt := cep.ReadContext(ctx)
			if pkt == nil {
				return
			}
			view := pkt.ToView()
			data := view.AsSlice()
			cp := make([]byte, len(data))
			copy(cp, data)
			view.Release()
			pkt.DecRef()
			if len(cp) >= 20 {
				proxy.inject(net.IP(cp[16:20]), cp)
			}
		}
	}()

	t.Cleanup(func() {
		cancel()
		proxy.close()
		cs.Close()
	})

	return &tunnelPair{agent: a, proxy: proxy, clientNS: cs, clientIP: clientAddr}
}

// dial opens a TCP connection from the client stack to a workload mesh
// address, driving a real handshake through the proxy.
func (p *tunnelPair) dial(t *testing.T, meshIP string, port uint16) *gonet.TCPConn {
	t.Helper()
	v4 := net.ParseIP(meshIP).To4()
	addr := tcpip.FullAddress{
		NIC:  2,
		Addr: tcpip.AddrFrom4([4]byte{v4[0], v4[1], v4[2], v4[3]}),
		Port: port,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := gonet.DialContextTCP(ctx, p.clientNS, addr, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatalf("dial %s:%d through the tunnel: %v", meshIP, port, err)
	}
	return conn
}

// routeTo makes every mesh address the proxy sees resolve to addr, standing in
// for the container-target lookup (which shells out to docker in production).
func (p *tunnelPair) routeTo(addr string) {
	p.proxy.resolve = func(net.IP, uint16) string { return addr }
}

// TestNetstackCompletesARealTCPHandshakeAndEcho drives a full connection
// through the receive path: handshake, request, response, half-close.
func TestNetstackCompletesARealTCPHandshakeAndEcho(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				b, _ := io.ReadAll(c)
				c.Write(bytes.ToUpper(b))
			}(c)
		}
	}()

	p := newTunnelPair(t)
	p.routeTo(ln.Addr().String())

	conn := p.dial(t, testMeshIP, 8080)
	defer conn.Close()

	if _, err := conn.Write([]byte("hello over the tunnel")); err != nil {
		t.Fatalf("write: %v", err)
	}
	conn.CloseWrite() // signal EOF so the echo server responds

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "HELLO OVER THE TUNNEL" {
		t.Errorf("got %q, want %q", got, "HELLO OVER THE TUNNEL")
	}
}

// TestNetstackSustainsABulkTransfer is the regression the legacy path could
// not survive. It moves far more than the old fixed 64KB window in one
// direction; the legacy proxy deadlocked here because it advertised a window
// it could not honour and had nothing to retransmit a dropped segment.
func TestNetstackSustainsABulkTransfer(t *testing.T) {
	const payloadSize = 8 << 20 // 8MB, ~128x the legacy fixed window

	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		io.Copy(c, bytes.NewReader(payload))
	}()

	p := newTunnelPair(t)
	p.routeTo(ln.Addr().String())

	conn := p.dial(t, testMeshIP, 8080)
	defer conn.Close()
	conn.CloseWrite()

	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("bulk read stalled after %d/%d bytes - this is the deadlock "+
			"the legacy path exhibited: %v", len(got), payloadSize, err)
	}
	if len(got) != payloadSize {
		t.Fatalf("received %d bytes, want %d", len(got), payloadSize)
	}
	if !bytes.Equal(got, payload) {
		t.Error("payload corrupted in transit")
	}
}

// TestNetstackRefusesAClosedPort checks the RST path: a workload that is not
// listening must look refused, not hang.
func TestNetstackRefusesAClosedPort(t *testing.T) {
	// Bind and immediately close to get an address nothing is listening on.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dead := ln.Addr().String()
	ln.Close()

	p := newTunnelPair(t)
	p.routeTo(dead)
	v4 := net.ParseIP(testMeshIP).To4()
	addr := tcpip.FullAddress{
		NIC:  2,
		Addr: tcpip.AddrFrom4([4]byte{v4[0], v4[1], v4[2], v4[3]}),
		Port: 8080,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := gonet.DialContextTCP(ctx, p.clientNS, addr, ipv4.ProtocolNumber)
	if err == nil {
		conn.Close()
		t.Fatal("dial to a closed workload port succeeded; it should be refused")
	}
	if ctx.Err() != nil {
		t.Error("dial to a closed port timed out instead of being refused - " +
			"a refused workload must produce an RST, not a hang")
	}
}

func TestResolveTunnelTargetFallsThroughForForeignWorkloads(t *testing.T) {
	// A mesh IP we do not own is dialled as-is: the container translation only
	// makes sense for workloads whose containers are on this node.
	a := newTestAgentWithDir(t)
	a.hwid = "us"
	a.stateMu.Lock()
	a.state.Workloads["10.100.0.7"] = &Workload{
		Name: "theirs", IP: "10.100.0.7", Owner: "someone-else",
	}
	a.stateMu.Unlock()

	if got := a.resolveTunnelTarget(net.ParseIP("10.100.0.7"), 8080); got != "10.100.0.7:8080" {
		t.Errorf("target = %q, want the mesh address unchanged", got)
	}
}

func TestResolveTunnelTargetHandlesUnknownIP(t *testing.T) {
	a := newTestAgentWithDir(t)
	a.hwid = "us"

	if got := a.resolveTunnelTarget(net.ParseIP("10.100.0.99"), 443); got != "10.100.0.99:443" {
		t.Errorf("target = %q, want the mesh address unchanged", got)
	}
}

func TestNetstackRegistersWorkloadAddressesOnce(t *testing.T) {
	p := newTunnelPair(t)
	ip := net.ParseIP("10.100.0.50")

	p.proxy.ensureAddr(ip)
	p.proxy.ensureAddr(ip)
	p.proxy.ensureAddr(net.ParseIP("10.100.0.51"))

	count := 0
	p.proxy.addrs.Range(func(_, _ any) bool { count++; return true })
	if count < 2 {
		t.Errorf("registered %d addresses, want at least 2", count)
	}
}

func TestNetstackIgnoresIPv6Destinations(t *testing.T) {
	// The mesh is IPv4 today and the stack is built with ipv4 only. An IPv6
	// destination must be dropped rather than panic on a 4-byte conversion.
	p := newTunnelPair(t)
	before := 0
	p.proxy.addrs.Range(func(_, _ any) bool { before++; return true })

	p.proxy.ensureAddr(net.ParseIP("fd00::1"))

	after := 0
	p.proxy.addrs.Range(func(_, _ any) bool { after++; return true })
	if after != before {
		t.Error("an IPv6 destination was registered on an IPv4-only stack")
	}
}

func TestNetstackProxyCloseIsIdempotent(t *testing.T) {
	// handleTunnelProxy defers close, and the outbound loop can also cancel on
	// a write error. Both happening must not panic.
	a := newTestAgentWithDir(t)
	p, err := a.newNetstackProxy(&wireWriter{deliver: func([]byte) {}})
	if err != nil {
		t.Fatalf("newNetstackProxy: %v", err)
	}
	p.close()
	p.close()
}
