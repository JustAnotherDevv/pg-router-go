// Package rawconn provides a zero-pgproto3 wire reader for the
// client→backend hot path. It reads Postgres frontend messages as raw
// bytes, bypassing pgproto3's decode/re-encode. This eliminates per-
// message struct allocations and encoding overhead on the hot path.
//
// Only boring messages (Execute, Sync, Flush) benefit from raw
// passthrough — they have no body or a trivially small body that
// doesn't need inspection. Interesting messages (Query, Parse) still
// need SQL extraction for GUC/pin/classification, but we do that
// with minimal byte scanning instead of full pgproto3 decode.
package rawconn

import (
	"encoding/binary"
	"io"
	"net"
	"sync"

	"github.com/JustAnotherDevv/pgrouter/internal/util"
)

// HeaderSize is the Postgres wire protocol message header size
// (1 type byte + 4 length bytes).
const HeaderSize = 5

// Tag constants for frontend messages.
const (
	TagQuery     = 'Q' // Simple query
	TagParse     = 'P' // Parse (extended protocol)
	TagBind      = 'B' // Bind
	TagDescribe  = 'D' // Describe
	TagExecute   = 'E' // Execute
	TagSync      = 'S' // Sync
	TagFlush     = 'H' // Flush
	TagClose     = 'C' // Close
	TagTerminate = 'X' // Terminate
	TagCopyData  = 'd' // CopyData
	TagCopyDone  = 'c' // CopyDone
	TagCopyFail  = 'f' // CopyFail
)

// BoringTags is a 256-byte lookup table indexed by tag byte.
// boringTags[tag] == true means the message can be forwarded raw
// without any inspection or modification.
var boringTags [256]bool

// DrainTriggerTags is a 256-byte lookup table indexed by tag byte.
// drainTriggerTags[tag] == true means receiving this message should
// trigger a backend drain (read responses until RFQ).
var drainTriggerTags [256]bool

func init() {
	// Boring: forward raw, no inspection needed.
	boring := []byte{
		TagExecute,  // 'E' — 5-byte no-body
		TagSync,     // 'H' — 5-byte no-body
		TagFlush,    // 'f' — 5-byte no-body (note: same as CopyFail!)
		TagCopyData, // 'd' — body is COPY data, forward raw
		TagCopyDone, // 'c' — 5-byte no-body
	}
	for _, b := range boring {
		boringTags[b] = true
	}

	// Drain triggers: message types that cause backend responses.
	drainTrigger := []byte{
		TagQuery,    // 'Q' — backend responds immediately
		TagSync,     // 'H' — backend responds after batch
		TagCopyDone, // 'c' — end of COPY
		TagCopyFail, // 'f' — end of COPY (error)
	}
	for _, b := range drainTrigger {
		drainTriggerTags[b] = true
	}
}

// IsBoring returns true if the tag byte represents a message that can
// be forwarded raw without inspection.
func IsBoring(tag byte) bool {
	return boringTags[tag]
}

// IsDrainTrigger returns true if receiving this message should trigger
// a backend drain loop.
func IsDrainTrigger(tag byte) bool {
	return drainTriggerTags[tag]
}

// IsTerminate returns true if the tag byte is Terminate ('X').
func IsTerminate(tag byte) bool {
	return tag == TagTerminate
}

// IsSync returns true if the tag byte is Sync ('H').
func IsSync(tag byte) bool {
	return tag == TagSync
}

// NeedsBackend returns true if the tag byte represents a message that
// requires a real backend connection to handle.
func NeedsBackend(tag byte) bool {
	switch tag {
	case TagQuery, TagParse, TagBind, TagExecute, TagDescribe,
		TagClose, TagCopyData, TagCopyDone, TagCopyFail, TagFlush:
		return true
	}
	return false
}

// msgBufPool reuses message buffers for the raw read path.
// Typical pgbench messages are <256 bytes; the pool grows as needed.
var msgBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

// GetMsgBuf returns a buffer from the pool with at least minCap capacity.
func GetMsgBuf(minCap int) *[]byte {
	bp := msgBufPool.Get().(*[]byte)
	if cap(*bp) < minCap {
		nb := make([]byte, minCap)
		return &nb
	}
	*bp = (*bp)[:0]
	return bp
}

// PutMsgBuf returns a buffer to the pool. Oversized buffers (>64KB)
// are dropped to avoid pinning large CopyData buffers.
func PutMsgBuf(bp *[]byte) {
	if bp == nil {
		return
	}
	if cap(*bp) > 64*1024 {
		return
	}
	*bp = (*bp)[:0]
	msgBufPool.Put(bp)
}

// RawConn reads Postgres frontend messages as raw bytes from a
// net.Conn, bypassing pgproto3's decode/re-encode. The caller
// receives (tag, rawBytes) pairs and decides what to do with them.
//
// For zero-copy: the returned raw slice is valid until the next
// ReadMessage call. Callers that need to retain the bytes beyond
// the next read must copy them.
type RawConn struct {
	conn   net.Conn
	reader *bufioReader
}

// New wraps conn for raw message reading.
func New(conn net.Conn) *RawConn {
	return &RawConn{
		conn:   conn,
		reader: newBufioReader(conn),
	}
}

// ReadMessage reads one frontend message from the client.
// Returns the tag byte and the complete raw message (header + body).
// The returned slice is valid until the next ReadMessage call.
//
// Returns io.EOF on clean client disconnect.
func (rc *RawConn) ReadMessage() (tag byte, raw []byte, err error) {
	hdr, err := rc.reader.Peek(HeaderSize)
	if err != nil {
		return 0, nil, err
	}
	tag = hdr[0]
	length := int(binary.BigEndian.Uint32(hdr[1:5]))
	bodyLen := length - 4
	if bodyLen < 0 {
		return 0, nil, io.ErrUnexpectedEOF
	}

	msgLen := HeaderSize + bodyLen

	// Fast path: entire message is in the bufio buffer (common for
	// small messages like Execute/Sync = 5 bytes).
	if rc.reader.Available() >= bodyLen {
		// Consume header + body in one shot
		msg, err := rc.reader.Peek(msgLen)
		if err != nil {
			return 0, nil, err
		}
		rc.reader.Discard(msgLen)
		return tag, msg, nil
	}

	// Slow path: message spans buffer boundary. Read header into
	// stack buf, body into a pooled buffer.
	rc.reader.Discard(HeaderSize)
	bp := GetMsgBuf(msgLen)
	raw = (*bp)[:msgLen]
	copy(raw, hdr)
	if bodyLen > 0 {
		if _, err := io.ReadFull(rc.conn, raw[HeaderSize:]); err != nil {
			PutMsgBuf(bp)
			return 0, nil, err
		}
	}
	return tag, raw, nil
}

// ExtractQuerySQL extracts the SQL string from a raw Query message.
// Query format: 'Q' + 4-byte length + null-terminated SQL string.
// Returns the SQL without the trailing null.
func ExtractQuerySQL(raw []byte) string {
	if len(raw) < HeaderSize+1 {
		return ""
	}
	// Skip header (5 bytes), the rest is the null-terminated SQL.
	body := raw[HeaderSize:]
	for i, b := range body {
		if b == 0 {
			return string(body[:i])
		}
	}
	// No null found — whole body is SQL (malformed, but be lenient).
	return string(body)
}

// QueryFirstKeywordRaw returns the first keyword of a raw Query message
// without allocating a string. Returns "" if the message is too short.
// Only checks keywords relevant to the hot path: SET, DISCARD, RESET.
func QueryFirstKeywordRaw(raw []byte) string {
	if len(raw) < HeaderSize+1 {
		return ""
	}
	body := raw[HeaderSize:]
	// Skip leading whitespace.
	i := 0
	for i < len(body) && (body[i] == ' ' || body[i] == '\t' || body[i] == '\n' || body[i] == '\r') {
		i++
	}
	if i >= len(body) || body[i] == 0 {
		return ""
	}
	start := i
	for i < len(body) && body[i] != 0 && isIdentByte(body[i]) {
		i++
	}
	keyword := body[start:i]
	if len(keyword) == 0 {
		return ""
	}
	// Fast case-insensitive check for SET/DISCARD/RESET.
	return matchKeyword(keyword)
}

func isIdentByte(b byte) bool { return util.IsIdentByte(b) }

// matchKeyword checks if the raw keyword bytes match SET/DISCARD/RESET.
func matchKeyword(kw []byte) string {
	switch len(kw) {
	case 3:
		if eqFoldByte(kw[0], 's') && eqFoldByte(kw[1], 'e') && eqFoldByte(kw[2], 't') {
			return "SET"
		}
	case 5:
		if eqFoldByte(kw[0], 'r') && eqFoldByte(kw[1], 'e') && eqFoldByte(kw[2], 's') && eqFoldByte(kw[3], 'e') && eqFoldByte(kw[4], 't') {
			return "RESET"
		}
	case 7:
		if eqFoldByte(kw[0], 'd') && eqFoldByte(kw[1], 'i') && eqFoldByte(kw[2], 's') && eqFoldByte(kw[3], 'c') && eqFoldByte(kw[4], 'a') && eqFoldByte(kw[5], 'r') && eqFoldByte(kw[6], 'd') {
			return "DISCARD"
		}
	}
	return ""
}

func eqFoldByte(a, b byte) bool { return util.EqFold(a, b) }

// ExtractParseFields extracts the statement name and query string
// from a raw Parse message. Parse format:
//
//	'P' + 4-byte length + null-terminated name + null-terminated query
//	+ 2-byte num_param_oids + [4-byte oid each]
//
// Returns (name, query). Either may be empty if the message is
// too short.
func ExtractParseFields(raw []byte) (name, query string) {
	if len(raw) < HeaderSize+2 {
		return "", ""
	}
	body := raw[HeaderSize:]

	// Find first null (end of name).
	nameEnd := -1
	for i, b := range body {
		if b == 0 {
			nameEnd = i
			break
		}
	}
	if nameEnd < 0 {
		return string(body), ""
	}
	name = string(body[:nameEnd])

	// Find second null (end of query).
	queryStart := nameEnd + 1
	queryEnd := -1
	for i := queryStart; i < len(body); i++ {
		if body[i] == 0 {
			queryEnd = i
			break
		}
	}
	if queryEnd < 0 {
		if queryStart < len(body) {
			query = string(body[queryStart:])
		}
		return name, query
	}
	query = string(body[queryStart:queryEnd])
	return name, query
}

// ExtractParseParamOIDs returns the parameter OID slice from a raw
// Parse message. Returns nil if the message is too short.
func ExtractParseParamOIDs(raw []byte) []uint32 {
	if len(raw) < HeaderSize+2 {
		return nil
	}
	body := raw[HeaderSize:]

	i := 0
	for i < len(body) && body[i] != 0 {
		i++
	}
	if i >= len(body) {
		return nil
	}
	i++ // skip null

	for i < len(body) && body[i] != 0 {
		i++
	}
	if i >= len(body) {
		return nil
	}
	i++ // skip null

	// Read 2-byte num_param_oids.
	if i+2 > len(body) {
		return nil
	}
	numOIDs := int(binary.BigEndian.Uint16(body[i:]))
	i += 2

	// Read OIDs.
	if i+numOIDs*4 > len(body) {
		return nil
	}
	oids := make([]uint32, numOIDs)
	for j := 0; j < numOIDs; j++ {
		oids[j] = binary.BigEndian.Uint32(body[i:])
		i += 4
	}
	return oids
}

// bufioReader is a minimal buffered reader optimized for the raw
// message read pattern: small, frequent Peeks followed by Discards.
// We don't need the full bufio.Reader API — just Peek, Discard,
// and Available.
type bufioReader struct {
	r   io.Reader
	buf []byte
	rp  int // read position
	wp  int // write position
}

func newBufioReader(r io.Reader) *bufioReader {
	return &bufioReader{
		r:   r,
		buf: make([]byte, 64*1024), // 64KB buffer
	}
}

// Available returns the number of unread bytes in the buffer.
func (b *bufioReader) Available() int {
	return b.wp - b.rp
}

// Peek returns the next n bytes without advancing. The returned slice
// is valid until the next Discard or Peek call.
func (b *bufioReader) Peek(n int) ([]byte, error) {
	for b.wp-b.rp < n {
		// Compact if we've consumed bytes at the front.
		if b.rp > 0 {
			unread := b.wp - b.rp
			copy(b.buf, b.buf[b.rp:b.wp])
			b.rp = 0
			b.wp = unread
		}
		// Grow if buffer too small.
		if len(b.buf) < n {
			newBuf := make([]byte, max(len(b.buf)*2, n))
			b.wp = copy(newBuf, b.buf[b.rp:b.wp])
			b.rp = 0
			b.buf = newBuf
		}
		// Read more data from underlying reader.
		nn, err := b.r.Read(b.buf[b.wp:])
		b.wp += nn
		if err != nil {
			if b.wp-b.rp >= n {
				return b.buf[b.rp : b.rp+n], nil
			}
			return nil, err
		}
	}
	return b.buf[b.rp : b.rp+n], nil
}

// Discard advances the read position by n bytes. Must call Peek
// first to ensure the bytes are available.
func (b *bufioReader) Discard(n int) {
	b.rp += n
}
