package listener

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// Attacker sends "PROXY " + a huge body with no LF. Before the cap,
// ReadString allocated unboundedly → OOM. Now it must fail fast with
// an explicit "exceeds N bytes" error.
func TestReadProxyHeaderV1RejectsOversizeLine(t *testing.T) {
	junk := bytes.Repeat([]byte("A"), MaxProxyV1Line+1024) // no '\n'
	payload := append([]byte("PROXY "), junk...)
	_, _, err := ReadProxyHeader(bytes.NewReader(payload))
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
}

// Attacker sends v2 header advertising a 60 KiB addr block — must
// refuse without allocating.
func TestReadProxyHeaderV2RejectsOversizeAddrBlock(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(v2Sig)
	buf.WriteByte(0x21) // v2 + PROXY
	buf.WriteByte(0x11) // TCP4
	// addrLen = MaxProxyV2Addr + 1
	lenBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(lenBuf, uint16(MaxProxyV2Addr+1))
	buf.Write(lenBuf)
	// Don't even bother writing the addr block — parser must reject
	// before the io.ReadFull call.
	_, _, err := ReadProxyHeader(&buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "too large")
}

func TestReadProxyHeaderV1TCP4(t *testing.T) {
	hdr := []byte("PROXY TCP4 192.0.2.1 198.51.100.1 5432 6432\r\n")
	body := []byte("HELLO")
	info, br, err := ReadProxyHeader(bytes.NewReader(append(hdr, body...)))
	require.NoError(t, err)
	require.Equal(t, 1, info.Version)
	require.Equal(t, "TCP4", info.Family)
	src := info.SourceAddr.(*net.TCPAddr)
	require.Equal(t, "192.0.2.1", src.IP.String())
	require.Equal(t, 5432, src.Port)

	rest, err := io.ReadAll(br)
	require.NoError(t, err)
	require.Equal(t, body, rest)
}

func TestReadProxyHeaderV1Unknown(t *testing.T) {
	hdr := []byte("PROXY UNKNOWN\r\n")
	info, _, err := ReadProxyHeader(bytes.NewReader(append(hdr, []byte("rest")...)))
	require.NoError(t, err)
	require.Equal(t, "UNKNOWN", info.Family)
	require.Nil(t, info.SourceAddr)
}

func TestReadProxyHeaderV2TCP4(t *testing.T) {
	// Construct v2 header.
	src := []byte{192, 0, 2, 1}
	dst := []byte{198, 51, 100, 1}
	srcPort := uint16(5432)
	dstPort := uint16(6432)

	addrBlock := bytes.Buffer{}
	addrBlock.Write(src)
	addrBlock.Write(dst)
	_ = binary.Write(&addrBlock, binary.BigEndian, srcPort)
	_ = binary.Write(&addrBlock, binary.BigEndian, dstPort)

	hdr := bytes.Buffer{}
	hdr.Write(v2Sig)
	hdr.WriteByte(0x21) // version 2, PROXY command
	hdr.WriteByte(0x11) // TCP over IPv4
	_ = binary.Write(&hdr, binary.BigEndian, uint16(addrBlock.Len()))
	hdr.Write(addrBlock.Bytes())
	hdr.WriteString("body")

	info, br, err := ReadProxyHeader(&hdr)
	require.NoError(t, err)
	require.Equal(t, 2, info.Version)
	require.Equal(t, "TCP4", info.Family)
	srcAddr := info.SourceAddr.(*net.TCPAddr)
	require.Equal(t, "192.0.2.1", srcAddr.IP.String())
	require.Equal(t, 5432, srcAddr.Port)
	rest, err := io.ReadAll(br)
	require.NoError(t, err)
	require.Equal(t, []byte("body"), rest)
}

func TestReadProxyHeaderV2LOCAL(t *testing.T) {
	hdr := bytes.Buffer{}
	hdr.Write(v2Sig)
	hdr.WriteByte(0x20) // version 2, LOCAL command
	hdr.WriteByte(0x00) // unspec
	_ = binary.Write(&hdr, binary.BigEndian, uint16(0))
	hdr.WriteString("body")

	info, _, err := ReadProxyHeader(&hdr)
	require.NoError(t, err)
	require.Equal(t, "LOCAL", info.Family)
	require.Nil(t, info.SourceAddr)
}

func TestReadProxyHeaderNoHeader(t *testing.T) {
	body := []byte("just-pgwire-bytes-here")
	_, br, err := ReadProxyHeader(bytes.NewReader(body))
	require.ErrorIs(t, err, ErrNoProxyHeader)
	// br still contains the original bytes intact.
	rest, err := io.ReadAll(br)
	require.NoError(t, err)
	require.Equal(t, body, rest)
}

func TestWithProxyAddrOverridesRemote(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	want := &net.TCPAddr{IP: net.ParseIP("203.0.113.7"), Port: 12345}
	wrapped := WithProxyAddr(c1, want, bytes.NewReader(nil))
	require.Equal(t, want, wrapped.RemoteAddr())
}
