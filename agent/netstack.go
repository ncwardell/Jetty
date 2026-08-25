package agent

import (
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/icmp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"
)

// =============================================================================
// Userspace tunnel receive path, on a real TCP stack
// =============================================================================
//
// The tunnel has two halves and only one of them ever had a TCP problem.
//
// The SEND half (tunReadLoop) reads packets the kernel already routed onto
// jetty_tun and forwards them over a WebSocket. It is a pure packet
// forwarder; the sending node's kernel does the TCP. That half is fine and is
// untouched here - which also means the TUN device and NET_ADMIN are still
// required. (An earlier claim that netstack removes them was wrong.)
//
// The RECEIVE half had to terminate TCP, and did so by hand: sequence numbers,
// checksums, SYN-ACK construction, an MSS option, and a bolted-on window
// check. It had no retransmission, no RTO, no congestion control and no
// zero-window probe. Bulk flows overran the receiver, segments were dropped
// with nothing to resend them, and the flow deadlocked - the CIFS hangs and
// the 10-15s stalls on ~21% of public requests.
//
// This replaces that half with gVisor's netstack: a real, tested TCP
// implementation. Packets arriving over the WebSocket are injected into it,
// packets it emits go back out over the same WebSocket, and connections it
// accepts are spliced to the local workload. The domain logic - translating a
// mesh IP and port to the container actually listening behind it - is
// preserved exactly, because that part was never the bug.
//
// Selected by JETTY_TUNNEL_STACK (netstack | legacy). Legacy remains so a
// rollback does not need a different binary.

// useNetstackTunnel reports whether the receive path should run on netstack.
//
// Defaults on: the legacy path is known to deadlock bulk transfers, so
// defaulting to the broken one to be cautious would be the wrong caution. The
// escape hatch is JETTY_TUNNEL_STACK=legacy, which needs no different binary.
func (a *Agent) useNetstackTunnel() bool {
	switch strings.ToLower(strings.TrimSpace(a.tunnelStack)) {
	case "legacy":
		return false
	case "", "netstack":
		return true
	default:
		logWarnf("JETTY_TUNNEL_STACK=%q is not recognised; using netstack. "+
			"Valid values are \"netstack\" and \"legacy\".", a.tunnelStack)
		return true
	}
}

const (
	netstackNICID = tcpip.NICID(1)

	// netstackMTU matches the jetty_tun MTU. Segment sizing is netstack's
	// problem now, but the link it thinks it has must still match the one
	// the packets actually traverse.
	netstackMTU = 1280

	// netstackQueueLen bounds packets buffered on the way out to the
	// WebSocket. Deep enough to absorb a burst, shallow enough that a stalled
	// peer applies backpressure instead of growing memory without limit.
	netstackQueueLen = 512

	// netstackDialTimeout matches the legacy path's upstream dial timeout.
	netstackDialTimeout = 5 * time.Second

	// netstackMaxInFlight caps half-open connections the forwarder will
	// track, bounding memory against a SYN flood arriving over the tunnel.
	netstackMaxInFlight = 1024
)

// netstackProxy terminates the tunnel's receive side for one WebSocket peer.
//
// One stack per connection rather than one shared stack: replies must go back
// out over the WebSocket they arrived on, and a per-connection stack makes
// that structural instead of a routing decision. Teardown is also just
// closing the connection.
type netstackProxy struct {
	agent *Agent
	out   packetWriter

	stack *stack.Stack
	ep    *channel.Endpoint

	ctx    context.Context
	cancel context.CancelFunc

	// addrs tracks which workload IPs are registered on the NIC. Netstack
	// only answers for addresses it owns - including ICMP echo, which it then
	// handles natively rather than us proxying pings by hand.
	addrs sync.Map

	// resolve maps a mesh address to the container address behind it.
	// Defaults to the agent's real resolver; a seam because the real one
	// shells out to docker, which a test cannot do.
	resolve func(net.IP, uint16) string
}

// packetWriter is where the stack's outbound packets go. In production this
// is the peer's *tunnelConn; taking an interface lets a test stand a second
// netstack up on the other end and exercise a real TCP handshake.
type packetWriter interface {
	WriteMessage(messageType int, data []byte) error
}

// newNetstackProxy builds a stack wired to a WebSocket peer.
func (a *Agent) newNetstackProxy(out packetWriter) (*netstackProxy, error) {
	s := stack.New(stack.Options{
		NetworkProtocols: []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{
			tcp.NewProtocol, udp.NewProtocol, icmp.NewProtocol4,
		},
	})

	ep := channel.New(netstackQueueLen, netstackMTU, "")
	if err := s.CreateNIC(netstackNICID, ep); err != nil {
		return nil, errFromTCPIP("create NIC", err)
	}

	// Spoofing lets the stack source replies from the workload mesh IPs,
	// which is exactly what we want it to do - the client addressed the
	// workload, so the reply has to come from the workload.
	s.SetSpoofing(netstackNICID, true)

	// SACK materially helps recovery on a lossy path, and the WebSocket
	// carrier can reorder under load.
	sackOpt := tcpip.TCPSACKEnabled(true)
	s.SetTransportProtocolOption(tcp.ProtocolNumber, &sackOpt)

	sn, snErr := tcpip.NewSubnet(
		tcpip.AddrFrom4([4]byte{0, 0, 0, 0}),
		tcpip.MaskFrom(string([]byte{0, 0, 0, 0})),
	)
	if snErr != nil {
		return nil, snErr
	}
	s.SetRouteTable([]tcpip.Route{{Destination: sn, NIC: netstackNICID}})

	ctx, cancel := context.WithCancel(context.Background())
	p := &netstackProxy{agent: a, out: out, stack: s, ep: ep, ctx: ctx, cancel: cancel}
	p.resolve = a.resolveTunnelTarget

	tcpFwd := tcp.NewForwarder(s, 0, netstackMaxInFlight, p.handleTCP)
	s.SetTransportProtocolHandler(tcp.ProtocolNumber, tcpFwd.HandlePacket)

	udpFwd := udp.NewForwarder(s, p.handleUDP)
	s.SetTransportProtocolHandler(udp.ProtocolNumber, udpFwd.HandlePacket)

	goSafe("netstackOutbound", p.outboundLoop)
	return p, nil
}

// close tears the stack down and releases the outbound loop.
func (p *netstackProxy) close() {
	p.cancel()
	p.ep.Close()
	p.stack.Close()
}

// inject hands a packet from the WebSocket to the stack.
func (p *netstackProxy) inject(dstIP net.IP, packet []byte) {
	p.ensureAddr(dstIP)
	pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{
		Payload: buffer.MakeWithData(packet),
	})
	p.ep.InjectInbound(ipv4.ProtocolNumber, pkb)
	pkb.DecRef()
}

// ensureAddr registers a workload IP on the NIC so the stack will answer for
// it. Idempotent and cheap after the first packet of a flow.
func (p *netstackProxy) ensureAddr(ip net.IP) {
	v4 := ip.To4()
	if v4 == nil {
		return
	}
	key := v4.String()
	if _, loaded := p.addrs.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	addr := tcpip.AddrFrom4([4]byte{v4[0], v4[1], v4[2], v4[3]})
	pa := tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: addr.WithPrefix(),
	}
	if err := p.stack.AddProtocolAddress(netstackNICID, pa, stack.AddressProperties{}); err != nil {
		logWarnf("netstack: could not add address %s: %v", key, err)
		p.addrs.Delete(key)
	}
}

// outboundLoop forwards packets the stack emits back over the WebSocket.
func (p *netstackProxy) outboundLoop() {
	for {
		pkt := p.ep.ReadContext(p.ctx)
		if pkt == nil {
			return // context cancelled: the connection is going away
		}
		view := pkt.ToView()
		data := view.AsSlice()
		err := p.out.WriteMessage(websocket.BinaryMessage, data)
		view.Release()
		pkt.DecRef()
		if err != nil {
			logWarnf("netstack: WebSocket write failed, closing tunnel: %v", err)
			p.cancel()
			return
		}
	}
}

// handleTCP accepts a connection the stack has parsed and splices it to the
// workload actually listening behind the mesh address.
func (p *netstackProxy) handleTCP(r *tcp.ForwarderRequest) {
	id := r.ID()
	dstIP := net.IP(id.LocalAddress.AsSlice())
	dstPort := id.LocalPort

	target := p.resolve(dstIP, dstPort)

	// Dial upstream BEFORE accepting. A refused workload should look refused
	// to the client - Complete(true) sends an RST, matching what a real
	// closed port does and what the legacy path did.
	upstream, err := net.DialTimeout("tcp", target, netstackDialTimeout)
	if err != nil {
		logWarnf("netstack: TCP connect to %s failed: %v", target, err)
		r.Complete(true)
		return
	}
	if t, ok := upstream.(*net.TCPConn); ok {
		// Half-open connections would otherwise leak file descriptors against
		// the workload when a peer's WebSocket dies mid-flow.
		t.SetKeepAlive(true)
		t.SetKeepAlivePeriod(30 * time.Second)
	}

	var wq waiter.Queue
	gep, tcpErr := r.CreateEndpoint(&wq)
	if tcpErr != nil {
		logWarnf("netstack: could not accept TCP to %s: %v", target, tcpErr)
		upstream.Close()
		r.Complete(true)
		return
	}
	r.Complete(false)

	client := gonet.NewTCPConn(&wq, gep)
	logInfof("netstack: TCP %s:%d -> %s", dstIP, dstPort, target)
	spliceTCP(client, upstream)
}

// spliceTCP copies in both directions and propagates half-close, so a client
// that finishes sending still receives the rest of the response.
func spliceTCP(client *gonet.TCPConn, upstream net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	goSafe("netstackSpliceUp", func() {
		defer wg.Done()
		io.Copy(upstream, client)
		if t, ok := upstream.(*net.TCPConn); ok {
			t.CloseWrite()
		}
	})
	goSafe("netstackSpliceDown", func() {
		defer wg.Done()
		io.Copy(client, upstream)
		client.CloseWrite()
	})

	goSafe("netstackSpliceClose", func() {
		wg.Wait()
		client.Close()
		upstream.Close()
	})
}

// handleUDP relays a datagram flow to the workload.
func (p *netstackProxy) handleUDP(r *udp.ForwarderRequest) {
	id := r.ID()
	dstIP := net.IP(id.LocalAddress.AsSlice())
	dstPort := id.LocalPort

	target := p.resolve(dstIP, dstPort)

	var wq waiter.Queue
	gep, err := r.CreateEndpoint(&wq)
	if err != nil {
		logWarnf("netstack: could not accept UDP to %s: %v", target, err)
		return
	}
	client := gonet.NewUDPConn(&wq, gep)

	upstream, derr := net.DialTimeout("udp", target, netstackDialTimeout)
	if derr != nil {
		logWarnf("netstack: UDP dial to %s failed: %v", target, derr)
		client.Close()
		return
	}

	logInfof("netstack: UDP %s:%d -> %s", dstIP, dstPort, target)
	goSafe("netstackUDPRelay", func() {
		defer client.Close()
		defer upstream.Close()
		var wg sync.WaitGroup
		wg.Add(2)
		goSafe("netstackUDPUp", func() { defer wg.Done(); io.Copy(upstream, client) })
		goSafe("netstackUDPDown", func() { defer wg.Done(); io.Copy(client, upstream) })
		wg.Wait()
	})
}

// resolveTunnelTarget maps a workload mesh address to the container address
// actually listening behind it.
//
// This is the one piece of the receive path that is domain logic rather than
// protocol, and it is why the naive "dial the mesh IP and port" is wrong.
// Asymmetric publications (vaultwarden's 8222:80) mean the container is not
// listening on the port the client addressed, and multi-container stacks need
// the per-port match to pick the right container. Kernel DNAT handles this for
// local traffic; over the tunnel we have to do it ourselves.
func (a *Agent) resolveTunnelTarget(dstIP net.IP, dstPort uint16) string {
	target := net.JoinHostPort(dstIP.String(), strconv.Itoa(int(dstPort)))

	a.stateMu.RLock()
	wl, exists := a.state.Workloads[dstIP.String()]
	isOurs := exists && wl != nil && wl.Owner == a.hwid
	name := ""
	if isOurs {
		name = wl.Name
	}
	a.stateMu.RUnlock()

	if !isOurs {
		return target
	}
	containerIP, containerPort := a.getWorkloadContainerTargetForPort(name, dstPort)
	if containerIP == "" {
		return target
	}
	dialPort := dstPort
	if containerPort != 0 {
		dialPort = containerPort
	}
	return net.JoinHostPort(containerIP, strconv.Itoa(int(dialPort)))
}

// errFromTCPIP renders a tcpip.Error as a plain error for construction paths.
func errFromTCPIP(what string, err tcpip.Error) error {
	return &netstackSetupError{what: what, msg: err.String()}
}

type netstackSetupError struct{ what, msg string }

func (e *netstackSetupError) Error() string { return "netstack " + e.what + ": " + e.msg }

// activeTunnelStack names the receive-path implementation in force. Exposed on
// /api/livez because "did netstack actually start, or did it fall back?" is
// otherwise invisible - and the fallback is silent by design.
func (a *Agent) activeTunnelStack() string {
	if a.useNetstackTunnel() {
		return "netstack"
	}
	return "legacy"
}
