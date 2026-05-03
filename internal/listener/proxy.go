// HAProxy PROXY protocol v1+v2 preamble parser.
//
// When upstream LB is HAProxy / AWS NLB / Cloudflare Spectrum, the
// client TCP connection appears to pgrouter as coming from the LB IP.
// PROXY protocol prefixes the connection with a small header carrying
// the real client (src_ip, src_port, dst_ip, dst_port) so backends can
// log + ACL against the real client identity.
//
// Spec: https://www.haproxy.org/download/2.9/doc/proxy-protocol.txt
//
// We support both v1 (text) and v2 (binary) variants. Detection is
// based on the first 12 bytes of the connection.
//
// Usage:
//
//	pc, err := ReadProxyHeader(conn)
//	// pc.SourceAddr / pc.DestAddr — real client + advertised dst
//	wrapped := WithProxyAddr(conn, pc.SourceAddr)
//	// pass `wrapped` to PooledHandler etc.

package listener

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// ProxyInfo is the parsed PROXY header. SourceAddr is nil for "UNKNOWN"
// / LOCAL connections — the wrapper leaves RemoteAddr unchanged in
// that case.
type ProxyInfo struct {
	Version    int // 1 or 2
	Family     string
	SourceAddr net.Addr // real client; may be nil for LOCAL/UNKNOWN
	DestAddr   net.Addr // advertised dst; may be nil
	Bytes      int      // header byte count consumed from conn
}

var (
	v2Sig = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}
	v1Sig = []byte("PROXY ")
)

// ErrNoProxyHeader is returned when the connection's first bytes are
// not a PROXY preamble. The conn is left unchanged.
var ErrNoProxyHeader = errors.New("no PROXY protocol header")

// MaxProxyV1Line caps the v1 ASCII line at 107 bytes per the PROXY
// spec (worst case: "PROXY TCP6 <39+39> <5+5> \r\n"). We allow 128 for
// trailing whitespace tolerance. Without this cap, a malicious client
// can send "PROXY " + unbounded bytes without a "\n" and ReadString
// allocates until OOM.
const MaxProxyV1Line = 128

// MaxProxyV2Addr caps the v2 binary address block. Per spec the TLV
// section can extend the addr block; we cap at 1024 to allow modest
// TLVs while preventing a malicious client from making us allocate
// 64 KiB (max uint16) per accepted conn.
const MaxProxyV2Addr = 1024

// ReadProxyHeader peeks the first bytes off conn and, if they match a
// PROXY preamble, consumes + parses it. On success the returned reader
// is the remaining stream (header bytes already drained).
//
// Caller is responsible for swapping `conn` with the returned io.Reader
// when constructing the pgwire backend (a bufio.Reader-wrapping helper
// is in WithProxyAddr).
func ReadProxyHeader(r io.Reader) (ProxyInfo, *bufio.Reader, error) {
	br := bufio.NewReader(r)
	head, err := br.Peek(12)
	if err != nil {
		return ProxyInfo{}, br, fmt.Errorf("peek: %w", err)
	}
	switch {
	case bytes.Equal(head, v2Sig):
		return parseProxyV2(br)
	case bytes.Equal(head[:6], v1Sig):
		return parseProxyV1(br)
	default:
		return ProxyInfo{}, br, ErrNoProxyHeader
	}
}

// parseProxyV1 reads the ASCII line up to \r\n.
//
//	PROXY TCP4 192.168.0.1 192.168.0.11 56324 443\r\n
//	PROXY TCP6 fe80::1 fe80::2 1024 8443\r\n
//	PROXY UNKNOWN\r\n
func parseProxyV1(br *bufio.Reader) (ProxyInfo, *bufio.Reader, error) {
	// Read one byte at a time up to MaxProxyV1Line; refuse to grow
	// beyond the spec'd worst case. ReadString('\n') would allocate
	// unboundedly if the attacker sends "PROXY " + 100 MB no-newline.
	var lineBuf []byte
	for {
		if len(lineBuf) >= MaxProxyV1Line {
			return ProxyInfo{}, br, fmt.Errorf("v1 line exceeds %d bytes (no LF found)", MaxProxyV1Line)
		}
		b, err := br.ReadByte()
		if err != nil {
			return ProxyInfo{}, br, fmt.Errorf("v1 read byte: %w", err)
		}
		lineBuf = append(lineBuf, b)
		if b == '\n' {
			break
		}
	}
	line := strings.TrimRight(string(lineBuf), "\r\n")
	parts := strings.Split(line, " ")
	if len(parts) < 2 {
		return ProxyInfo{}, br, fmt.Errorf("v1 too short: %q", line)
	}
	info := ProxyInfo{Version: 1, Family: parts[1], Bytes: len(line) + 2}
	if parts[1] == "UNKNOWN" {
		return info, br, nil
	}
	if len(parts) != 6 {
		return info, br, fmt.Errorf("v1 want 6 fields, got %d in %q", len(parts), line)
	}
	srcIP, dstIP := parts[2], parts[3]
	srcPort, err := strconv.Atoi(parts[4])
	if err != nil {
		return info, br, fmt.Errorf("v1 src port: %w", err)
	}
	dstPort, err := strconv.Atoi(parts[5])
	if err != nil {
		return info, br, fmt.Errorf("v1 dst port: %w", err)
	}
	info.SourceAddr = &net.TCPAddr{IP: net.ParseIP(srcIP), Port: srcPort}
	info.DestAddr = &net.TCPAddr{IP: net.ParseIP(dstIP), Port: dstPort}
	return info, br, nil
}

// parseProxyV2 parses the binary preamble.
//
//	signature (12 bytes) — v2Sig
//	version+command (1)  — 0x21 = v2 + PROXY; 0x20 = v2 + LOCAL
//	family+proto (1)
//	addr len (2, big-endian)
//	addr block (varies; we only fully parse INET / INET6 over TCP)
func parseProxyV2(br *bufio.Reader) (ProxyInfo, *bufio.Reader, error) {
	hdr := make([]byte, 16)
	if _, err := io.ReadFull(br, hdr); err != nil {
		return ProxyInfo{}, br, fmt.Errorf("v2 header: %w", err)
	}
	verCmd := hdr[12]
	famProto := hdr[13]
	addrLen := int(binary.BigEndian.Uint16(hdr[14:16]))
	// Refuse silly-large addr blocks. Real TCP4/TCP6 fits in ≤36 bytes;
	// TLV extensions cap at MaxProxyV2Addr. uint16 alone allows up to
	// 65535 bytes per accepted conn — easy memory-amplification.
	if addrLen > MaxProxyV2Addr {
		return ProxyInfo{}, br, fmt.Errorf("v2 addr block too large: %d > %d", addrLen, MaxProxyV2Addr)
	}
	addrBuf := make([]byte, addrLen)
	if _, err := io.ReadFull(br, addrBuf); err != nil {
		return ProxyInfo{}, br, fmt.Errorf("v2 addr block: %w", err)
	}
	info := ProxyInfo{Version: 2, Bytes: 16 + addrLen}
	if verCmd>>4 != 2 {
		return info, br, fmt.Errorf("v2 bad version nibble: %x", verCmd>>4)
	}
	cmd := verCmd & 0x0F
	if cmd == 0 {
		// LOCAL — keep original addr.
		info.Family = "LOCAL"
		return info, br, nil
	}
	if cmd != 1 {
		return info, br, fmt.Errorf("v2 unknown command %x", cmd)
	}
	switch famProto {
	case 0x11: // TCP over IPv4
		if len(addrBuf) < 12 {
			return info, br, fmt.Errorf("v2 ipv4 addr block too short: %d", len(addrBuf))
		}
		src := net.IP(addrBuf[0:4])
		dst := net.IP(addrBuf[4:8])
		srcPort := binary.BigEndian.Uint16(addrBuf[8:10])
		dstPort := binary.BigEndian.Uint16(addrBuf[10:12])
		info.Family = "TCP4"
		info.SourceAddr = &net.TCPAddr{IP: src, Port: int(srcPort)}
		info.DestAddr = &net.TCPAddr{IP: dst, Port: int(dstPort)}
	case 0x21: // TCP over IPv6
		if len(addrBuf) < 36 {
			return info, br, fmt.Errorf("v2 ipv6 addr block too short: %d", len(addrBuf))
		}
		src := net.IP(addrBuf[0:16])
		dst := net.IP(addrBuf[16:32])
		srcPort := binary.BigEndian.Uint16(addrBuf[32:34])
		dstPort := binary.BigEndian.Uint16(addrBuf[34:36])
		info.Family = "TCP6"
		info.SourceAddr = &net.TCPAddr{IP: src, Port: int(srcPort)}
		info.DestAddr = &net.TCPAddr{IP: dst, Port: int(dstPort)}
	default:
		info.Family = fmt.Sprintf("UNKNOWN(%x)", famProto)
	}
	return info, br, nil
}

// proxyConn wraps a net.Conn whose first PROXY-header bytes have
// already been consumed. RemoteAddr returns the override addr (real
// client) instead of the underlying TCP peer (LB).
//
// IMPORTANT: this type DOES NOT embed net.Conn. Embedding would let
// callers type-assert the wrapped value to *net.TCPConn / syscall.Conn
// and drain raw bytes from the underlying socket, bypassing the bufio
// reader that holds the post-PROXY bytes still in flight. All net.Conn
// methods are forwarded explicitly instead.
type proxyConn struct {
	conn   net.Conn
	remote net.Addr
	r      io.Reader // br ahead of the original conn; reads pull from here
}

// Read pulls from the buffered reader (which already has the post-header
// bytes drained) before falling through to the underlying conn.
func (p *proxyConn) Read(b []byte) (int, error) { return p.r.Read(b) }

// RemoteAddr returns the parsed real client address.
func (p *proxyConn) RemoteAddr() net.Addr { return p.remote }

// All other net.Conn methods forward to the underlying socket.
func (p *proxyConn) Write(b []byte) (int, error)        { return p.conn.Write(b) }
func (p *proxyConn) Close() error                       { return p.conn.Close() }
func (p *proxyConn) LocalAddr() net.Addr                { return p.conn.LocalAddr() }
func (p *proxyConn) SetReadDeadline(t time.Time) error  { return p.conn.SetReadDeadline(t) }
func (p *proxyConn) SetWriteDeadline(t time.Time) error { return p.conn.SetWriteDeadline(t) }
func (p *proxyConn) SetDeadline(t time.Time) error      { return p.conn.SetDeadline(t) }

// WithProxyAddr wraps conn so RemoteAddr returns the PROXY-parsed
// source address. `r` is the bufio.Reader returned by ReadProxyHeader
// (it holds any bytes the bufio peek read past the header).
func WithProxyAddr(conn net.Conn, remote net.Addr, r io.Reader) net.Conn {
	if remote == nil {
		return conn
	}
	return &proxyConn{conn: conn, remote: remote, r: r}
}
