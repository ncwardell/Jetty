package agent

import (
	cryptorand "crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/exec"
	"time"

	"github.com/gorilla/websocket"
	"github.com/songgao/water"
)

// randomISN returns a fresh TCP initial sequence number. Real RFC-6528 ISN
// generation hashes the 4-tuple plus a secret to make the ISN unguessable
// to off-path observers but deterministic per-connection. We simplify with
// crypto/rand: the WS endpoint is authenticated with JETTY_SECRET so
// off-path injection isn't reachable here, but using time.UnixNano (which
// the original code did) is a leak we can fix for free.
func randomISN() uint32 {
	var b [4]byte
	cryptorand.Read(b[:])
	return binary.BigEndian.Uint32(b[:])
}

// =============================================================================
// Userspace Tunnel (fallback when kernel IPIP/GRE not available)
// =============================================================================

// initUserspaceTunnel creates a TUN device for userspace tunneling.
// This is used when kernel IPIP/GRE modules aren't available (e.g., ChromeOS).
// Traffic to remote workloads (10.100.x.x) is captured via TUN, sent over WebSocket
// to the peer's API (port 6880), and proxied to the local workload.
// Responses are proxied back through the same WebSocket connection.
func (a *Agent) initUserspaceTunnel() error {
	// Create TUN device
	config := water.Config{
		DeviceType: water.TUN,
	}
	config.Name = "jetty_tun"

	tun, err := water.New(config)
	if err != nil {
		return fmt.Errorf("create TUN device: %w", err)
	}
	a.tunDevice = tun

	// Configure TUN device
	if err := exec.Command("ip", "link", "set", "up", "dev", "jetty_tun").Run(); err != nil {
		tun.Close()
		return fmt.Errorf("bring up TUN device: %w", err)
	}

	// Set MTU to account for WebSocket/TLS encapsulation overhead.
	// Without this, large packets get fragmented/dropped and TCP connections hang
	// after the handshake completes (small packets work, large responses don't).
	if err := exec.Command("ip", "link", "set", "dev", "jetty_tun", "mtu", "1280").Run(); err != nil {
		log.Printf("Warning: failed to set TUN MTU: %v", err)
	}

	// Start packet forwarding goroutine (reads from TUN, sends over WebSocket)
	go a.tunReadLoop()

	log.Printf("Userspace tunnel initialized (TUN: jetty_tun, WebSocket on API port %d)", a.apiPort)
	return nil
}

// tunReadLoop reads packets from TUN device and sends them via WebSocket to the appropriate peer.
// This handles both outgoing traffic (to remote workloads) and response traffic (back to tunnel sources).
func (a *Agent) tunReadLoop() {
	buf := make([]byte, 65535)
	for {
		n, err := a.tunDevice.Read(buf)
		if err != nil {
			select {
			case <-a.stopCh:
				return
			default:
				log.Printf("TUN read error: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		if n < 20 {
			continue // Too small to be valid IP packet
		}

		packet := make([]byte, n)
		copy(packet, buf[:n])

		if packet[0]>>4 != 4 {
			continue // Not IPv4
		}
		dstIP := net.IP(packet[16:20])
		srcIP := net.IP(packet[12:16])

		// Check if this is outgoing traffic to a remote workload
		dstIPStr := dstIP.String()
		a.workloadRoutesMu.Lock()
		ownerID := a.workloadRoutes[dstIPStr]
		a.workloadRoutesMu.Unlock()

		if ownerID == "" {
			continue // No route for this destination
		}

		// Get peer's WARP IP for WebSocket tunnel
		peerIPVal, ok := a.tunPeerIPs.Load(ownerID)
		if !ok {
			log.Printf("WS tunnel send: no peer IP for owner %s (dst=%s)", shortID(ownerID, 8), dstIP)
			continue
		}
		peerIP := peerIPVal.(string)

		// Get or establish WebSocket connection to peer
		conn, err := a.getTunPeerConn(ownerID, peerIP)
		if err != nil {
			log.Printf("WS tunnel send: failed to connect to %s: %v", peerIP, err)
			continue
		}

		log.Printf("WS tunnel send: %s -> %s via %s (%d bytes)", srcIP, dstIP, peerIP, n)

		// Send packet as binary WebSocket message
		if err := conn.WriteMessage(websocket.BinaryMessage, packet); err != nil {
			log.Printf("WS tunnel send: failed to send to %s: %v", peerIP, err)
			conn.Close()
			a.tunPeerConns.Delete(ownerID)
		}
	}
}

// getTunPeerConn gets or creates a WebSocket connection to a peer for tunneling.
func (a *Agent) getTunPeerConn(peerID, peerIP string) (*websocket.Conn, error) {
	// Check if we already have a connection
	if connVal, ok := a.tunPeerConns.Load(peerID); ok {
		return connVal.(*websocket.Conn), nil
	}

	// Establish new connection (with lock to prevent duplicate connections)
	a.tunConnMu.Lock()
	defer a.tunConnMu.Unlock()

	// Double-check after acquiring lock
	if connVal, ok := a.tunPeerConns.Load(peerID); ok {
		return connVal.(*websocket.Conn), nil
	}

	// Connect to peer's tunnel WebSocket endpoint
	url := fmt.Sprintf("ws://%s:%d/api/tunnel/ws", peerIP, a.apiPort)
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	// Add cluster secret header if configured
	headers := http.Header{}
	if a.clusterSecret != "" {
		headers.Set("X-API-Key", a.clusterSecret)
	}

	conn, _, err := dialer.Dial(url, headers)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", url, err)
	}

	// Store connection and start receiver goroutine
	a.tunPeerConns.Store(peerID, conn)
	go a.tunWsRecvLoop(peerID, conn)

	log.Printf("WS tunnel: established connection to %s (%s)", shortID(peerID, 8), peerIP)
	return conn, nil
}

// tunWsRecvLoop receives packets from a peer's WebSocket connection and injects them locally.
func (a *Agent) tunWsRecvLoop(peerID string, conn *websocket.Conn) {
	defer func() {
		conn.Close()
		a.tunPeerConns.Delete(peerID)
	}()

	for {
		msgType, packet, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-a.stopCh:
				return
			default:
				log.Printf("WS tunnel recv: connection to %s closed: %v", shortID(peerID, 8), err)
				return
			}
		}

		if msgType != websocket.BinaryMessage || len(packet) < 20 {
			continue
		}

		// Verify it's an IPv4 packet
		if packet[0]>>4 != 4 {
			continue
		}

		dstIP := net.IP(packet[16:20])
		srcIP := net.IP(packet[12:16])

		log.Printf("WS tunnel recv: from %s, packet %s -> %s (%d bytes)", shortID(peerID, 8), srcIP, dstIP, len(packet))

		// Inject packet into TUN device for local delivery
		if a.tunDevice != nil {
			if _, err := a.tunDevice.Write(packet); err != nil {
				log.Printf("WS tunnel recv: failed to write to TUN: %v", err)
			}
		}
	}
}

// updateTunPeerAddr updates the WARP IP for a peer's WebSocket tunnel endpoint.
func (a *Agent) updateTunPeerAddr(peerID, peerIP string) {
	if a.tunDevice == nil || peerIP == "" {
		return
	}
	a.tunPeerIPs.Store(peerID, peerIP)
}

// cleanupUserspaceTunnel closes all WebSocket connections and the TUN device.
func (a *Agent) cleanupUserspaceTunnel() {
	// Close all peer WebSocket connections
	a.tunPeerConns.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*websocket.Conn); ok {
			conn.Close()
		}
		return true
	})

	// Close all TCP proxy connections
	a.tunTCPConns.Range(func(key, value interface{}) bool {
		if proxyConn, ok := value.(*tcpProxyConn); ok {
			proxyConn.conn.Close()
		}
		return true
	})

	if a.tunDevice != nil {
		a.tunDevice.Close()
		exec.Command("ip", "link", "del", "jetty_tun").Run()
	}
}

// WebSocket upgrader for tunnel connections
var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// apiTunnelWs handles incoming WebSocket connections for packet tunneling.
// Peers connect here to send/receive encapsulated IP packets.
// SECURITY: This endpoint is protected by apiKeyMiddleware (requires JETTY_SECRET).
// Only authenticated cluster peers can establish tunnel connections.
//
// This uses a PROXY model: packets are forwarded to workloads via raw sockets,
// and responses are captured and sent back through the same WebSocket connection.
func (a *Agent) apiTunnelWs(w http.ResponseWriter, r *http.Request) {
	// The tunnel endpoint accepts arbitrary IPv4 packets and proxies them to local
	// services. With --net host that is anything reachable from the host, so this
	// endpoint MUST require authentication regardless of cluster secret config.
	if a.clusterSecret == "" {
		log.Printf("WS tunnel: refusing connection from %s - JETTY_SECRET not configured", r.RemoteAddr)
		http.Error(w, "tunnel disabled: cluster secret not configured", http.StatusUnauthorized)
		return
	}
	apiKey := r.Header.Get("X-API-Key")
	if subtle.ConstantTimeCompare([]byte(apiKey), []byte(a.clusterSecret)) != 1 {
		log.Printf("WS tunnel: rejected unauthenticated connection from %s", r.RemoteAddr)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WS tunnel: upgrade failed: %v", err)
		return
	}

	remoteAddr := r.RemoteAddr
	log.Printf("WS tunnel: authenticated connection from %s", remoteAddr)

	// Handle incoming packets from this peer using proxy model
	go a.handleTunnelProxy(conn, remoteAddr)
}

// handleTunnelProxy proxies packets from a peer to local workloads and captures responses.
// This uses proper ICMP proxying: parse request, send our own ping, repackage response.
func (a *Agent) handleTunnelProxy(conn *websocket.Conn, remoteAddr string) {
	defer conn.Close()

	// Wrap the websocket connection with a write mutex to prevent concurrent write panics
	tc := &tunnelConn{conn: conn}

	for {
		msgType, packet, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-a.stopCh:
				return
			default:
				log.Printf("WS tunnel: connection from %s closed: %v", remoteAddr, err)
				return
			}
		}

		if msgType != websocket.BinaryMessage || len(packet) < 20 {
			continue
		}

		// Verify it's an IPv4 packet
		if packet[0]>>4 != 4 {
			continue
		}

		dstIP := net.IP(packet[16:20])
		srcIP := net.IP(packet[12:16])

		// Security: only accept packets destined for our local workloads
		a.stateMu.RLock()
		var isLocalWorkload bool
		for _, wl := range a.state.Workloads {
			if wl.Owner == a.hwid && wl.IP == dstIP.String() {
				isLocalWorkload = true
				break
			}
		}
		a.stateMu.RUnlock()

		if !isLocalWorkload {
			log.Printf("WS tunnel: rejected packet to non-local IP %s from %s", dstIP, remoteAddr)
			continue
		}

		log.Printf("WS tunnel proxy: %s -> %s (%d bytes)", srcIP, dstIP, len(packet))

		// Parse IP header to determine protocol
		ihl := int(packet[0]&0x0f) * 4
		if len(packet) < ihl+8 {
			continue
		}
		protocol := packet[9]

		// Handle by protocol
		switch protocol {
		case 1: // ICMP - proxy at application level
			// Copy packet data since goroutine may outlive this iteration
			icmpPacket := make([]byte, len(packet))
			copy(icmpPacket, packet)
			icmpSrcIP := net.IP(icmpPacket[12:16])
			icmpDstIP := net.IP(icmpPacket[16:20])
			go a.proxyICMP(tc, icmpPacket, icmpSrcIP, icmpDstIP, ihl)
		case 6: // TCP - proxy connections
			// Process TCP packets synchronously to maintain ordering.
			// Using goroutines here causes race conditions where packets are
			// processed out of order (e.g., body before headers).
			tcpPacket := make([]byte, len(packet))
			copy(tcpPacket, packet)
			tcpSrcIP := net.IP(tcpPacket[12:16])
			tcpDstIP := net.IP(tcpPacket[16:20])
			a.proxyTCP(tc, tcpPacket, tcpSrcIP, tcpDstIP, ihl)
		case 17: // UDP - proxy datagrams
			// Copy packet data since goroutine may outlive this iteration
			udpPacket := make([]byte, len(packet))
			copy(udpPacket, packet)
			udpSrcIP := net.IP(udpPacket[12:16])
			udpDstIP := net.IP(udpPacket[16:20])
			go a.proxyUDP(tc, udpPacket, udpSrcIP, udpDstIP, ihl)
		default:
			log.Printf("WS tunnel proxy: unsupported protocol %d", protocol)
		}
	}
}

// proxyICMP proxies an ICMP packet by sending our own request and repackaging the response.
func (a *Agent) proxyICMP(tc *tunnelConn, origPacket []byte, origSrc, dstIP net.IP, ihl int) {
	icmpData := origPacket[ihl:]
	if len(icmpData) < 8 {
		return
	}

	icmpType := icmpData[0]
	// Only proxy echo requests (type 8)
	if icmpType != 8 {
		return
	}

	// Extract ICMP ID and sequence for matching
	icmpID := uint16(icmpData[4])<<8 | uint16(icmpData[5])
	icmpSeq := uint16(icmpData[6])<<8 | uint16(icmpData[7])

	// Send our own ICMP echo request to the workload and wait for reply
	icmpConn, err := net.DialTimeout("ip4:icmp", dstIP.String(), 5*time.Second)
	if err != nil {
		log.Printf("WS tunnel proxy: failed to dial ICMP to %s: %v", dstIP, err)
		return
	}
	defer icmpConn.Close()

	// Build ICMP echo request with same ID and sequence
	icmpReq := make([]byte, len(icmpData))
	copy(icmpReq, icmpData)
	// Recalculate checksum (set to 0 first)
	icmpReq[2] = 0
	icmpReq[3] = 0
	checksum := icmpChecksum(icmpReq)
	icmpReq[2] = byte(checksum >> 8)
	icmpReq[3] = byte(checksum)

	// Set deadline for receive
	icmpConn.SetDeadline(time.Now().Add(5 * time.Second))

	// Send our ICMP request
	if _, err := icmpConn.Write(icmpReq); err != nil {
		log.Printf("WS tunnel proxy: failed to send ICMP: %v", err)
		return
	}

	// Read response
	reply := make([]byte, 1500)
	n, err := icmpConn.Read(reply)
	if err != nil {
		log.Printf("WS tunnel proxy: failed to receive ICMP reply: %v", err)
		return
	}

	// Parse reply - skip IP header to get ICMP data
	replyIPHdrLen := int(reply[0]&0x0f) * 4
	if n < replyIPHdrLen+8 {
		return
	}
	replyICMP := reply[replyIPHdrLen:n]

	// Verify it's an echo reply (type 0) with matching ID and sequence
	if replyICMP[0] != 0 {
		return
	}
	replyID := uint16(replyICMP[4])<<8 | uint16(replyICMP[5])
	replySeq := uint16(replyICMP[6])<<8 | uint16(replyICMP[7])
	if replyID != icmpID || replySeq != icmpSeq {
		return
	}

	log.Printf("WS tunnel proxy: got ICMP reply from %s (id=%d seq=%d)", dstIP, icmpID, icmpSeq)

	// Build response packet with original src/dst swapped
	respPacket := buildIPPacket(dstIP, origSrc, 1, replyICMP)

	// Send response back through WebSocket (synchronized)
	if err := tc.WriteMessage(websocket.BinaryMessage, respPacket); err != nil {
		log.Printf("WS tunnel proxy: failed to send response: %v", err)
	}
}

// proxyTCP handles TCP packets by establishing real connections and proxying data.
func (a *Agent) proxyTCP(tc *tunnelConn, packet []byte, srcIP, dstIP net.IP, ihl int) {
	if len(packet) < ihl+20 {
		return // TCP header is at least 20 bytes
	}

	tcpHeader := packet[ihl:]
	srcPort := uint16(tcpHeader[0])<<8 | uint16(tcpHeader[1])
	dstPort := uint16(tcpHeader[2])<<8 | uint16(tcpHeader[3])
	seqNum := uint32(tcpHeader[4])<<24 | uint32(tcpHeader[5])<<16 | uint32(tcpHeader[6])<<8 | uint32(tcpHeader[7])
	tcpFlags := tcpHeader[13]
	dataOffset := int(tcpHeader[12]>>4) * 4

	isSYN := tcpFlags&0x02 != 0
	isACK := tcpFlags&0x10 != 0
	isFIN := tcpFlags&0x01 != 0
	isRST := tcpFlags&0x04 != 0

	// Flow key for tracking connections
	flowKey := fmt.Sprintf("%s:%d->%s:%d", srcIP, srcPort, dstIP, dstPort)

	// Handle SYN - new connection
	if isSYN && !isACK {
		log.Printf("WS tunnel proxy TCP SYN: %s", flowKey)

		// Create pending proxy connection BEFORE establishing backend connection
		// This prevents race conditions with ACK packets arriving early
		proxyConn := &tcpProxyConn{
			conn:      nil, // Will be set when connection is established
			wsConn:    tc,
			srcIP:     srcIP,
			srcPort:   srcPort,
			dstIP:     dstIP,
			dstPort:   dstPort,
			localSeq:  randomISN(),
			remoteSeq: seqNum + 1,
			ready:     make(chan struct{}),
			failed:    false,
		}
		a.tunTCPConns.Store(flowKey, proxyConn)

		// Look up workload to translate virtual IP to container IP
		targetAddr := fmt.Sprintf("%s:%d", dstIP, dstPort)

		a.stateMu.RLock()
		wl, exists := a.state.Workloads[dstIP.String()]
		a.stateMu.RUnlock()

		if exists && wl.Owner == a.hwid {
			// This is our workload, get the actual container IP
			containerIP := a.getWorkloadContainerIP(wl.Name)
			if containerIP != "" {
				targetAddr = fmt.Sprintf("%s:%d", containerIP, dstPort)
				log.Printf("WS tunnel proxy: translated %s -> %s", dstIP, containerIP)
			}
		}

		// Establish TCP connection to workload
		tcpConn, err := net.DialTimeout("tcp", targetAddr, 5*time.Second)
		if err != nil {
			log.Printf("WS tunnel proxy: TCP connect to %s failed: %v", targetAddr, err)
			proxyConn.mu.Lock()
			proxyConn.failed = true
			proxyConn.mu.Unlock()
			close(proxyConn.ready)
			a.tunTCPConns.Delete(flowKey)
			// Send RST back
			a.sendTCPResponse(tc, dstIP, srcIP, dstPort, srcPort, 0, seqNum+1, 0x14, nil) // RST+ACK
			return
		}
		// Enable TCP keepalive on the upstream socket so half-open connections
		// (e.g. peer's WS dies mid-flow) eventually get closed by the kernel.
		// Without this, dropped peers leak open file descriptors against the
		// workload until docker-compose restarts it.
		if t, ok := tcpConn.(*net.TCPConn); ok {
			t.SetKeepAlive(true)
			t.SetKeepAlivePeriod(30 * time.Second)
		}

		// Connection established - update proxyConn and signal ready
		proxyConn.mu.Lock()
		proxyConn.conn = tcpConn
		proxyConn.mu.Unlock()
		close(proxyConn.ready)

		// Send SYN-ACK with MSS option to limit segment sizes and prevent fragmentation.
		// MSS = MTU(1280) - IP header(20) - TCP header(20) = 1240
		a.sendTCPSynAck(tc, dstIP, srcIP, dstPort, srcPort, proxyConn.localSeq, proxyConn.remoteSeq, 1240)
		proxyConn.mu.Lock()
		proxyConn.localSeq++
		proxyConn.mu.Unlock()

		// Start goroutine to read from TCP and send back via WebSocket
		go a.tcpProxyReadLoop(flowKey, proxyConn)
		return
	}

	// Get existing connection
	connVal, ok := a.tunTCPConns.Load(flowKey)
	if !ok {
		// Unknown connection, send RST
		if !isRST {
			a.sendTCPResponse(tc, dstIP, srcIP, dstPort, srcPort, 0, seqNum+1, 0x14, nil) // RST+ACK
		}
		return
	}
	proxyConn := connVal.(*tcpProxyConn)

	// Wait for connection to be established if it's still pending
	if proxyConn.ready != nil {
		<-proxyConn.ready
	}

	// Check if connection establishment failed
	proxyConn.mu.Lock()
	if proxyConn.failed || proxyConn.conn == nil {
		proxyConn.mu.Unlock()
		if !isRST {
			a.sendTCPResponse(tc, dstIP, srcIP, dstPort, srcPort, 0, seqNum+1, 0x14, nil) // RST+ACK
		}
		return
	}
	proxyConn.mu.Unlock()

	// Handle FIN
	if isFIN {
		log.Printf("WS tunnel proxy TCP FIN: %s", flowKey)
		proxyConn.mu.Lock()
		proxyConn.remoteSeq = seqNum + 1
		proxyConn.mu.Unlock()

		// Send FIN-ACK and close
		a.sendTCPResponse(tc, dstIP, srcIP, dstPort, srcPort, proxyConn.localSeq, proxyConn.remoteSeq, 0x11, nil) // FIN+ACK
		proxyConn.conn.Close()
		a.tunTCPConns.Delete(flowKey)
		return
	}

	// Handle RST
	if isRST {
		proxyConn.conn.Close()
		a.tunTCPConns.Delete(flowKey)
		return
	}

	// Handle data
	if len(tcpHeader) > dataOffset {
		payload := tcpHeader[dataOffset:]
		if len(payload) > 0 {
			proxyConn.mu.Lock()
			proxyConn.remoteSeq = seqNum + uint32(len(payload))
			proxyConn.mu.Unlock()

			// Write data to TCP connection
			if _, err := proxyConn.conn.Write(payload); err != nil {
				log.Printf("WS tunnel proxy: TCP write failed: %v", err)
				proxyConn.conn.Close()
				a.tunTCPConns.Delete(flowKey)
				return
			}

			// Send ACK
			a.sendTCPResponse(tc, dstIP, srcIP, dstPort, srcPort, proxyConn.localSeq, proxyConn.remoteSeq, 0x10, nil) // ACK
		}
	}
}

// tcpProxyReadLoop reads from the TCP connection and sends data back via WebSocket.
func (a *Agent) tcpProxyReadLoop(flowKey string, proxyConn *tcpProxyConn) {
	// Read buffer - can be large since we'll chunk it for sending
	buf := make([]byte, 32768)
	// Maximum payload size per packet: MTU(1280) - IP header(20) - TCP header(20) = 1240
	const maxSegmentSize = 1240

	for {
		n, err := proxyConn.conn.Read(buf)
		if err != nil {
			// Connection closed
			proxyConn.mu.Lock()
			seq := proxyConn.localSeq
			ack := proxyConn.remoteSeq
			proxyConn.mu.Unlock()

			// Send FIN
			a.sendTCPResponse(proxyConn.wsConn, proxyConn.dstIP, proxyConn.srcIP,
				proxyConn.dstPort, proxyConn.srcPort, seq, ack, 0x11, nil) // FIN+ACK
			a.tunTCPConns.Delete(flowKey)
			return
		}

		if n > 0 {
			// Chunk data into MSS-sized segments to fit within MTU
			data := buf[:n]
			for len(data) > 0 {
				segmentSize := len(data)
				if segmentSize > maxSegmentSize {
					segmentSize = maxSegmentSize
				}

				proxyConn.mu.Lock()
				seq := proxyConn.localSeq
				proxyConn.localSeq += uint32(segmentSize)
				ack := proxyConn.remoteSeq
				proxyConn.mu.Unlock()

				// Send segment with PSH+ACK
				a.sendTCPResponse(proxyConn.wsConn, proxyConn.dstIP, proxyConn.srcIP,
					proxyConn.dstPort, proxyConn.srcPort, seq, ack, 0x18, data[:segmentSize])

				data = data[segmentSize:]
			}
		}
	}
}

// sendTCPResponse constructs and sends a TCP packet back through WebSocket.
func (a *Agent) sendTCPResponse(tc *tunnelConn, srcIP, dstIP net.IP, srcPort, dstPort uint16, seq, ack uint32, flags byte, payload []byte) {
	// Build TCP header (20 bytes minimum)
	tcpLen := 20 + len(payload)
	tcp := make([]byte, tcpLen)

	// Source port
	tcp[0] = byte(srcPort >> 8)
	tcp[1] = byte(srcPort)
	// Dest port
	tcp[2] = byte(dstPort >> 8)
	tcp[3] = byte(dstPort)
	// Sequence number
	tcp[4] = byte(seq >> 24)
	tcp[5] = byte(seq >> 16)
	tcp[6] = byte(seq >> 8)
	tcp[7] = byte(seq)
	// ACK number
	tcp[8] = byte(ack >> 24)
	tcp[9] = byte(ack >> 16)
	tcp[10] = byte(ack >> 8)
	tcp[11] = byte(ack)
	// Data offset (5 = 20 bytes) + reserved
	tcp[12] = 0x50
	// Flags
	tcp[13] = flags
	// Window size
	tcp[14] = 0xFF
	tcp[15] = 0xFF
	// Checksum (calculated below)
	tcp[16] = 0
	tcp[17] = 0
	// Urgent pointer
	tcp[18] = 0
	tcp[19] = 0
	// Payload
	copy(tcp[20:], payload)

	// Calculate TCP checksum with pseudo-header
	tcp[16], tcp[17] = tcpChecksum(srcIP, dstIP, tcp)

	// Build full IP packet
	packet := buildIPPacket(srcIP, dstIP, 6, tcp)

	// Send via WebSocket (synchronized)
	if err := tc.WriteMessage(websocket.BinaryMessage, packet); err != nil {
		log.Printf("WS tunnel proxy: failed to send TCP response: %v", err)
	}
}

// sendTCPSynAck sends a SYN-ACK with MSS option to advertise maximum segment size.
// This prevents the client from sending segments larger than the tunnel MTU can handle.
func (a *Agent) sendTCPSynAck(tc *tunnelConn, srcIP, dstIP net.IP, srcPort, dstPort uint16, seq, ack uint32, mss uint16) {
	// Build TCP header with MSS option (24 bytes total)
	tcp := make([]byte, 24)

	// Source port
	tcp[0] = byte(srcPort >> 8)
	tcp[1] = byte(srcPort)
	// Dest port
	tcp[2] = byte(dstPort >> 8)
	tcp[3] = byte(dstPort)
	// Sequence number
	tcp[4] = byte(seq >> 24)
	tcp[5] = byte(seq >> 16)
	tcp[6] = byte(seq >> 8)
	tcp[7] = byte(seq)
	// ACK number
	tcp[8] = byte(ack >> 24)
	tcp[9] = byte(ack >> 16)
	tcp[10] = byte(ack >> 8)
	tcp[11] = byte(ack)
	// Data offset (6 = 24 bytes header with options) + reserved
	tcp[12] = 0x60
	// Flags: SYN+ACK
	tcp[13] = 0x12
	// Window size
	tcp[14] = 0xFF
	tcp[15] = 0xFF
	// Checksum (calculated below)
	tcp[16] = 0
	tcp[17] = 0
	// Urgent pointer
	tcp[18] = 0
	tcp[19] = 0
	// TCP Options: MSS (Kind=2, Length=4, MSS value)
	tcp[20] = 2 // Kind: MSS
	tcp[21] = 4 // Length: 4 bytes
	tcp[22] = byte(mss >> 8)
	tcp[23] = byte(mss)

	// Calculate TCP checksum with pseudo-header
	tcp[16], tcp[17] = tcpChecksum(srcIP, dstIP, tcp)

	// Build full IP packet
	packet := buildIPPacket(srcIP, dstIP, 6, tcp)

	// Send via WebSocket (synchronized)
	if err := tc.WriteMessage(websocket.BinaryMessage, packet); err != nil {
		log.Printf("WS tunnel proxy: failed to send TCP SYN-ACK: %v", err)
	}
}

// tcpChecksum calculates TCP checksum including pseudo-header.
func tcpChecksum(srcIP, dstIP net.IP, tcpSegment []byte) (byte, byte) {
	// Pseudo-header: srcIP(4) + dstIP(4) + zero(1) + protocol(1) + tcpLen(2)
	pseudoLen := 12 + len(tcpSegment)
	pseudo := make([]byte, pseudoLen)

	copy(pseudo[0:4], srcIP.To4())
	copy(pseudo[4:8], dstIP.To4())
	pseudo[8] = 0
	pseudo[9] = 6 // TCP protocol
	pseudo[10] = byte(len(tcpSegment) >> 8)
	pseudo[11] = byte(len(tcpSegment))
	copy(pseudo[12:], tcpSegment)

	var sum uint32
	for i := 0; i < len(pseudo)-1; i += 2 {
		sum += uint32(pseudo[i])<<8 | uint32(pseudo[i+1])
	}
	if len(pseudo)%2 == 1 {
		sum += uint32(pseudo[len(pseudo)-1]) << 8
	}
	for sum > 0xffff {
		sum = (sum & 0xffff) + (sum >> 16)
	}
	checksum := ^uint16(sum)
	return byte(checksum >> 8), byte(checksum)
}

// proxyUDP handles UDP packets by proxying them directly.
func (a *Agent) proxyUDP(tc *tunnelConn, packet []byte, srcIP, dstIP net.IP, ihl int) {
	if len(packet) < ihl+8 {
		return
	}

	udpHeader := packet[ihl:]
	srcPort := uint16(udpHeader[0])<<8 | uint16(udpHeader[1])
	dstPort := uint16(udpHeader[2])<<8 | uint16(udpHeader[3])
	udpLen := uint16(udpHeader[4])<<8 | uint16(udpHeader[5])

	if int(udpLen) > len(udpHeader) {
		return
	}

	payload := udpHeader[8:udpLen]

	log.Printf("WS tunnel proxy UDP: %s:%d -> %s:%d (%d bytes)", srcIP, srcPort, dstIP, dstPort, len(payload))

	// Send UDP and get response
	addr := fmt.Sprintf("%s:%d", dstIP, dstPort)
	udpConn, err := net.DialTimeout("udp", addr, 5*time.Second)
	if err != nil {
		log.Printf("WS tunnel proxy: UDP dial failed: %v", err)
		return
	}
	defer udpConn.Close()

	udpConn.SetDeadline(time.Now().Add(5 * time.Second))

	if _, err := udpConn.Write(payload); err != nil {
		log.Printf("WS tunnel proxy: UDP write failed: %v", err)
		return
	}

	// Read response
	respBuf := make([]byte, 65535)
	n, err := udpConn.Read(respBuf)
	if err != nil {
		// No response (might be one-way UDP)
		return
	}

	// Build UDP response
	respUDP := make([]byte, 8+n)
	respUDP[0] = byte(dstPort >> 8)
	respUDP[1] = byte(dstPort)
	respUDP[2] = byte(srcPort >> 8)
	respUDP[3] = byte(srcPort)
	respUDP[4] = byte((8 + n) >> 8)
	respUDP[5] = byte(8 + n)
	respUDP[6] = 0 // Checksum (optional for IPv4)
	respUDP[7] = 0
	copy(respUDP[8:], respBuf[:n])

	// Build IP packet and send back (synchronized)
	respPacket := buildIPPacket(dstIP, srcIP, 17, respUDP)
	if err := tc.WriteMessage(websocket.BinaryMessage, respPacket); err != nil {
		log.Printf("WS tunnel proxy: failed to send UDP response: %v", err)
	}
}

// buildIPPacket constructs an IPv4 packet with the given parameters.
func buildIPPacket(srcIP, dstIP net.IP, protocol byte, payload []byte) []byte {
	totalLen := 20 + len(payload)
	packet := make([]byte, totalLen)

	// Version (4) + IHL (5 = 20 bytes)
	packet[0] = 0x45
	// TOS
	packet[1] = 0
	// Total length
	packet[2] = byte(totalLen >> 8)
	packet[3] = byte(totalLen)
	// Identification (random)
	packet[4] = byte(time.Now().UnixNano() >> 8)
	packet[5] = byte(time.Now().UnixNano())
	// Flags + Fragment offset
	packet[6] = 0x40 // Don't fragment
	packet[7] = 0
	// TTL
	packet[8] = 64
	// Protocol
	packet[9] = protocol
	// Checksum (set to 0 for calculation)
	packet[10] = 0
	packet[11] = 0
	// Source IP
	copy(packet[12:16], srcIP.To4())
	// Destination IP
	copy(packet[16:20], dstIP.To4())
	// Payload
	copy(packet[20:], payload)

	// Calculate IP header checksum
	checksum := ipChecksum(packet[:20])
	packet[10] = byte(checksum >> 8)
	packet[11] = byte(checksum)

	return packet
}

// icmpChecksum calculates the ICMP checksum.
func icmpChecksum(data []byte) uint16 {
	var sum uint32
	for i := 0; i < len(data)-1; i += 2 {
		sum += uint32(data[i])<<8 | uint32(data[i+1])
	}
	if len(data)%2 == 1 {
		sum += uint32(data[len(data)-1]) << 8
	}
	for sum > 0xffff {
		sum = (sum & 0xffff) + (sum >> 16)
	}
	return ^uint16(sum)
}

// ipChecksum calculates the IP header checksum.
func ipChecksum(header []byte) uint16 {
	var sum uint32
	for i := 0; i < len(header)-1; i += 2 {
		sum += uint32(header[i])<<8 | uint32(header[i+1])
	}
	for sum > 0xffff {
		sum = (sum & 0xffff) + (sum >> 16)
	}
	return ^uint16(sum)
}
