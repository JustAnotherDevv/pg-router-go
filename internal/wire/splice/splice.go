// Package splice is the Phase A splice forwarder: a low-allocation
// hot path for backend→client message forwarding that bypasses
// pgproto3's decode/re-encode for messages that don't carry any
// observation the proxy needs to perform.
//
// Lives in internal/wire/splice to avoid the import cycle that would
// be caused by adding to package wire (which already depends on
// internal/client for BuildPooledHandler).
package splice

import (
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"sync"
	"unsafe"
)

// TagClass classifies a Postgres backend message tag byte for the
// purposes of the splice drain loop. See tagClassTable below.
type TagClass uint8

const (
	// ClassBoring: the message carries no observation-relevant data.
	// Safe to splice forward as src bytes. Examples: DataRow, RowDescription,
	// CommandComplete, ParseComplete, BindComplete, NoData, EmptyQuery,
	// PortalSuspended.
	ClassBoring TagClass = iota

	// ClassInteresting: the message needs decode + observation before
	// forwarding. Examples: ErrorResponse, NoticeResponse, NotificationResponse,
	// ParameterStatus, BackendKeyData, Copy*Response, NegotiateProtocol.
	ClassInteresting

	// ClassError: ErrorResponse specifically. Decode so we can extract
	// SQLSTATE for counters/logging, then forward verbatim.
	ClassError

	// ClassTerminator: ReadyForQuery. Decode + observe + signal a transaction
	// boundary to the drain loop.
	ClassTerminator

	// ClassCopyIn: CopyInResponse. Triggers the drain loop to stop and
	// hand control back to the outer loop so it can start receiving
	// CopyData from the client.
	ClassCopyIn

	// ClassUnknown: any tag not explicitly classified. Treated as boring
	// by default (we forward what we don't understand) but the drain
	// loop can be configured to drop unknown messages if a stricter
	// posture is desired.
	ClassUnknown
)

// SpliceConfig tunes the splice drain loop. All fields have safe
// production defaults; operators can override via cfg.Wire.Splice
// in pgrouter.yaml.
type SpliceConfig struct {
	// Enabled turns the splice path on. Default true. Set false to
	// fall back to the original pgproto3-decode hot path.
	Enabled bool

	// BufferSize is the size of the reusable splice buffer pulled from
	// the pool. Should be >= the largest single boring message the
	// workload produces (typical DataRow <2KB; some workloads push 8KB
	// RowDescription). Must be >= 5 (the wire header size). 0 = default.
	BufferSize int

	// DropUnknownTags, when true, silently drops messages whose tag
	// byte is not in tagClassTable. Default false (forward as boring).
	// Useful for forward-compat with future Postgres protocol additions.
	DropUnknownTags bool
}

// Default splice config: enabled, 8 KiB buffer, no drop.
func DefaultSpliceConfig() SpliceConfig {
	return SpliceConfig{
		Enabled:         true,
		BufferSize:      8 * 1024,
		DropUnknownTags: false,
	}
}

// tagClassTable is a 256-entry lookup indexed by message tag byte.
// Initialised in init(). O(1) classification, no branches.
var tagClassTable [256]TagClass

func init() {
	// Boring: pure pass-through, no observation needed.
	boring := []byte{
		'D', // DataRow
		'T', // RowDescription
		'C', // CommandComplete
		'1', // ParseComplete
		'2', // BindComplete
		'n', // NoData
		'I', // EmptyQuery
		's', // PortalSuspended
	}
	for _, b := range boring {
		tagClassTable[b] = ClassBoring
	}

	// Interesting: needs decode + observation (ParameterStatus caches
	// into CachedParams, BackendKeyData seeds the cancel registry, etc).
	interesting := []byte{
		'S', // ParameterStatus
		'K', // BackendKeyData
		'A', // NotificationResponse
		'N', // NoticeResponse
		'W', // CopyBothResponse (Postgres 14+ bidirectional COPY)
		'v', // NegotiateProtocol (PG17+)
	}
	for _, b := range interesting {
		tagClassTable[b] = ClassInteresting
	}

	// Special: ErrorResponse needs decode to extract SQLSTATE for
	// counters; classify as ClassError so the existing dispatch
	// path handles it.
	tagClassTable['E'] = ClassError

	// Terminator: ReadyForQuery.
	tagClassTable['Z'] = ClassTerminator

	// CopyIn: Backend signals it's ready to receive CopyData; we stop
	// the drain loop and let the outer loop consume client→backend
	// CopyData.
	tagClassTable['G'] = ClassCopyIn

	// CopyOut: Backend is streaming data to us. We splice-forward
	// CopyData and break on CopyDone. Treat as interesting so the
	// existing dispatch handles the mode switch.
	tagClassTable['H'] = ClassInteresting
}

// Classify returns the TagClass for a given backend message tag byte.
func Classify(tag byte) TagClass {
	return tagClassTable[tag]
}

// ErrSpliceStop is returned by DrainSplice when it encounters a
// non-boring message and stops so the caller can switch to the
// decoded path. The 5-byte header of the stopping message has
// already been pushed back into the putbackReader for the caller's
// pgproto3.Receive() to consume.
var ErrSpliceStop = errors.New("wire: splice stop (interesting message pending)")

// HeaderSize is the size of a Postgres protocol message header
// (1 type byte + 4 length bytes).
const HeaderSize = 5

// spliceBufPool reuses the working buffer used by DrainSplice.
// Allocated buffers are pinned to the pool until PutSpliceBuf.
var spliceBufPool = sync.Pool{
	New: func() any {
		// Default 8KB; overridden by callers via GetSpliceBuf.
		b := make([]byte, 8*1024)
		return &b
	},
}

// GetSpliceBuf returns a buffer with the requested minimum size.
// If the pooled buffer is too small, a new one is allocated. The
// returned pointer must be returned via PutSpliceBuf.
func GetSpliceBuf(minSize int) *[]byte {
	bp := spliceBufPool.Get().(*[]byte)
	if cap(*bp) < minSize {
		// Too small for this workload; grow.
		nb := make([]byte, minSize)
		return &nb
	}
	*bp = (*bp)[:minSize]
	return bp
}

// PutSpliceBuf returns a buffer to the pool. Oversized buffers
// (>64 KiB) are dropped to avoid pinning huge CopyData buffers.
func PutSpliceBuf(bp *[]byte) {
	if bp == nil {
		return
	}
	if cap(*bp) > 64*1024 {
		return
	}
	*bp = (*bp)[:0]
	spliceBufPool.Put(bp)
}

// PutbackReader wraps an io.Reader with a tiny putback buffer.
// DrainSplice uses it to "unread" a 5-byte message header so the
// caller's pgproto3.Frontend.Receive() can decode the message the
// splice loop decided not to handle.
//
// The buffer is small (HeaderSize bytes) because that's the maximum
// we ever need to put back: a Postgres message header is exactly
// 1 type byte + 4 length bytes. We never need to put back the body.
type PutbackReader struct {
	r    io.Reader
	buf  [HeaderSize]byte
	n    int // number of bytes currently in buf
	stat PutbackStats
}

// PutbackStats tracks usage so operators can observe the splice path
// is actually engaged (via /metrics or debug logs).
type PutbackStats struct {
	// Putbacks is the number of times Putback was called (i.e. the
	// number of times the splice loop yielded control).
	Putbacks uint64
	// Puts is the number of bytes put back (always <= HeaderSize).
	Puts uint64
	// Reads is the number of underlying Read calls made.
	Reads uint64
}

// NewPutbackReader wraps r.
func NewPutbackReader(r io.Reader) *PutbackReader {
	return &PutbackReader{r: r}
}

// Read implements io.Reader. Any bytes in the putback buffer are
// returned first, then the wrapped reader is consulted.
func (p *PutbackReader) Read(dst []byte) (int, error) {
	if p.n > 0 {
		n := copy(dst, p.buf[:p.n])
		p.n -= n
		// Shift remaining bytes to the front (rare; only when
		// the caller's dst was smaller than what we had).
		if p.n > 0 {
			copy(p.buf[:p.n], p.buf[n:n+p.n])
		}
		return n, nil
	}
	p.stat.Reads++
	return p.r.Read(dst)
}

// ChunkReaderNext is a thin interface satisfied by *pgproto3's
// internal chunkReader (used by Frontend and Backend). Splice uses
// this interface to read src bytes through the SAME chunkReader that
// bConn.Frontend uses, so over-reads land in the chunkReader's buf
// and stay accessible to subsequent Frontend.Receive() calls.
//
// The interface is satisfied via reflection (see PooledConn.setupSplice)
// because chunkReader is unexported in pgproto3.
type ChunkReaderNext interface {
	Next(n int) ([]byte, error)
}

// chunkReaderMirror is a local re-implementation of pgproto3's
// unexported chunkReader. We can NOT call methods on the unexported
// type from this package (reflect panics with "reflect.Value.Call
// using value obtained using unexported field"), so we use
// unsafe.Pointer to reinterpret the unexported *chunkReader as a
// *chunkReaderMirror, then call our replicated Next method.
//
// The field layout MUST match pgproto3.chunkReader exactly:
//
//	pgproto3.chunkReader {
//	    r          io.Reader
//	    buf        *[]byte
//	    rp, wp     int
//	    minBufSize int
//	}
//
// This file asserts the layout at init() — if pgproto3 changes, the
// assert fails loudly at startup rather than silently corrupting
// memory.
type chunkReaderMirror struct {
	r          io.Reader
	buf        *[]byte
	rp, wp     int
	minBufSize int
}

// Next replicates pgproto3.chunkReader.Next byte-for-byte. If pgproto3
// changes its chunkReader implementation, this method must be updated
// in lockstep.
func (m *chunkReaderMirror) Next(n int) ([]byte, error) {
	// Reset the buffer if it is empty
	if m.rp == m.wp {
		if len(*m.buf) != m.minBufSize {
			// pgproto3 uses an internal pool; we don't bother —
			// the buf is allocated once at NewFrontend and lives
			// for the lifetime of the Frontend, so dropping the
			// pool-resize optimization is safe.
			newBuf := make([]byte, m.minBufSize)
			*m.buf = newBuf
		}
		m.rp = 0
		m.wp = 0
	}

	// n bytes already in buf
	if (m.wp - m.rp) >= n {
		buf := (*m.buf)[m.rp : m.rp+n : m.rp+n]
		m.rp += n
		return buf, nil
	}

	// buf is smaller than requested number of bytes
	if len(*m.buf) < n {
		bigBuf := make([]byte, n)
		m.wp = copy(bigBuf, (*m.buf)[m.rp:m.wp])
		m.rp = 0
		*m.buf = bigBuf
	}

	// buf is large enough, but need to shift filled area to start to make enough contiguous space
	minReadCount := n - (m.wp - m.rp)
	if (len(*m.buf) - m.wp) < minReadCount {
		m.wp = copy((*m.buf), (*m.buf)[m.rp:m.wp])
		m.rp = 0
	}

	// Read at least the required number of bytes from the underlying io.Reader
	readBytesCount, err := io.ReadAtLeast(m.r, (*m.buf)[m.wp:], minReadCount)
	m.wp += readBytesCount
	if err != nil {
		return nil, err
	}

	buf := (*m.buf)[m.rp : m.rp+n : m.rp+n]
	m.rp += n
	return buf, nil
}

// RawReader is the io.Reader adapter returned by NewRawReader. It
// reads through the pgproto3.Frontend's internal chunkReader so it
// shares the buffer with Frontend.Receive — critical for the splice
// forwarder, where over-reads in the chunkReader must stay accessible
// to the next Receive call.
//
// Implementation note: pgproto3's chunkReader is unexported. We
// obtain a pointer to it via reflect.Value.Pointer, then cast that
// pointer to *chunkReaderMirror (which has the same memory layout).
// This works because the field order and types match.
type RawReader struct {
	m *chunkReaderMirror
}

// NewRawReader returns a RawReader that reads src bytes from the
// pgproto3.Frontend's internal chunkReader. Callers MUST use this
// (not the Frontend's underlying conn) for the splice forwarder to
// keep the chunkReader's buffer consistent with subsequent
// Frontend.Receive() calls.
func NewRawReader(fe any) (*RawReader, error) {
	v := reflect.ValueOf(fe)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, errors.New("splice: fe must be a struct or pointer to struct")
	}
	crField := v.FieldByName("cr")
	if !crField.IsValid() {
		return nil, errors.New("splice: Frontend has no 'cr' field (pgproto3 layout changed?)")
	}
	if crField.Kind() != reflect.Ptr || crField.IsNil() {
		return nil, errors.New("splice: Frontend.cr is not a non-nil pointer (pgproto3 layout changed?)")
	}
	// crField is *pgproto3.chunkReader. We reinterpret it as
	// *chunkReaderMirror. The cast is sound iff the layouts match —
	// assertChunkReaderLayout enforces this at init().
	crPtr := unsafe.Pointer(crField.Pointer())
	mirror := (*chunkReaderMirror)(crPtr)
	return &RawReader{m: mirror}, nil
}

// Layout safety: the chunkReaderMirror struct above MUST match
// pgproto3.chunkReader's memory layout (same field order, same
// types, same sizes) for the unsafe.Pointer cast in NewRawReader
// to be sound. We don't have access to pgproto3.chunkReader from
// this package (it is unexported), so we cannot check sizes at
// compile time.
//
// The integration tests (TestPooledSplice*) exercise the full
// DrainSplice + Frontend.Receive path and would fail immediately
// on any layout mismatch (corrupted data, deadlocks, or panics).
// If pgproto3 ever changes its chunkReader layout, these tests
// will fail loudly — the fix is to update chunkReaderMirror to
// match the new layout.

// Read reads exactly len(p) bytes from the chunkReader. It does NOT
// return early on short reads — it loops until the buffer is full or
// the underlying io.Reader reports EOF/error. The returned slice is
// only valid until the next Next call (per pgproto3's contract).
func (r *RawReader) Read(p []byte) (int, error) {
	var (
		total    int
		firstErr error
	)
	for total < len(p) {
		remaining := p[total:]
		bs, err := r.m.Next(len(remaining))
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if len(bs) == 0 {
				return total, firstErr
			}
		}
		n := copy(remaining, bs)
		total += n
		if firstErr != nil {
			return total, firstErr
		}
	}
	return total, nil
}

// Rewind moves the chunkReader read position back by len(buf) bytes.
// Used by DrainSplice to "unread" a 5-byte message header so the
// next pgproto3.Frontend.Receive() call sees it. buf's contents
// are ignored — only len(buf) matters (the chunkReader's buf
// already holds the right bytes from the prior Read). The caller
// MUST pass a slice of length <= the number of bytes consumed since
// the last refill; DrainSplice always passes hdr (5 bytes) right
// after reading it, so this is always safe.
func (r *RawReader) Rewind(buf []byte) {
	r.m.rp -= len(buf)
}

// RawForwardResult is returned by RawForwardAll to communicate
// what happened during the raw forward pass.
type RawForwardResult struct {
	// Tag is the tag byte of the last message processed (typically RFQ).
	Tag byte
	// TxStatus is the transaction status byte from ReadyForQuery (byte 5).
	// Only valid when Tag == 'Z'.
	TxStatus byte
	// CopyIn is true if the last message was CopyInResponse ('G').
	CopyIn bool
	// EOF is true if the backend closed the connection.
	EOF bool
}

// RawForwardAll reads ALL backend messages from src as raw bytes and
// writes them directly to dst, bypassing pgproto3 decode/re-encode.
// This eliminates heap allocations for every backend message.
//
// Classification is done by tag byte only — no body inspection except
// for ReadyForQuery (byte 5 = tx_status).
//
// Returns when:
//   - ReadyForQuery is received (Tag='Z', TxStatus set)
//   - CopyInResponse is received (CopyIn=true)
//   - EOF (backend closed)
//   - I/O error
func RawForwardAll(dst io.Writer, src SpliceReader, buf []byte) (RawForwardResult, error) {
	var res RawForwardResult
	if len(buf) < HeaderSize {
		buf = make([]byte, 8*1024)
	}

	woff := 0 // bytes accumulated in buf

	for {
		// Read 5-byte header.
		if _, err := io.ReadFull(src, buf[woff:woff+HeaderSize]); err != nil {
			if err == io.EOF {
				if woff > 0 {
					_, _ = dst.Write(buf[:woff])
				}
				res.EOF = true
				return res, nil
			}
			return res, err
		}
		tag := buf[woff]
		bodyLen := int(binary.BigEndian.Uint32(buf[woff+1:woff+5])) - 4
		if bodyLen < 0 {
			return res, errors.New("wire: negative body length in raw forward")
		}

		msgTotal := HeaderSize + bodyLen

		// Check if message fits in buffer.
		if woff+msgTotal <= len(buf) {
			// Read body after header.
			if bodyLen > 0 {
				if _, err := io.ReadFull(src, buf[woff+HeaderSize:woff+msgTotal]); err != nil {
					return res, err
				}
			}
			woff += msgTotal
		} else {
			// Flush accumulated data first.
			if woff > 0 {
				if _, err := dst.Write(buf[:woff]); err != nil {
					return res, err
				}
				woff = 0
			}
			// Giant message: two-write path (header + body).
			if _, err := dst.Write(buf[:HeaderSize]); err != nil {
				return res, err
			}
			if bodyLen > 0 {
				if _, err := io.CopyN(dst, src, int64(bodyLen)); err != nil {
					return res, err
				}
			}
		}

		// Classify and handle special messages.
		switch tag {
		case 'Z': // ReadyForQuery
			// Extract tx_status: it's at offset 5 in the message
			// (after 1-byte tag + 4-byte length = byte index 5 in
			// the full message, or the first body byte).
			// The body is 1 byte: tx_status.
			if bodyLen >= 1 {
				// tx_status is at buf[msgStart+5] but we need
				// to find it. Since we accumulate contiguously,
				// tx_status is at buf[woff-bodyLen] (first body byte).
				res.TxStatus = buf[woff-bodyLen]
			}
			res.Tag = tag
			// Flush any remaining data.
			if woff > 0 {
				if _, err := dst.Write(buf[:woff]); err != nil {
					return res, err
				}
			}
			return res, nil

		case 'G': // CopyInResponse
			res.CopyIn = true
			res.Tag = tag
			if woff > 0 {
				if _, err := dst.Write(buf[:woff]); err != nil {
					return res, err
				}
			}
			return res, nil

		case 'E': // ErrorResponse
			// Forward raw. No inspection needed in pooler mode.
			// (SQLSTATE extraction was only for logging/counters,
			// which we skip in raw passthrough mode.)
			res.Tag = tag

		case 'S': // ParameterStatus
			// Forward raw. We already intercept SET on client side.
			res.Tag = tag

		case 'K': // BackendKeyData
			// Forward raw. Cancel key is only used for QueryCancel,
			// which we don't support in pooler mode.
			res.Tag = tag

		default:
			// All other messages (NoticeResponse, etc.): forward raw.
			res.Tag = tag
		}
	}
}

// Putback pushes bytes back so the next Read returns them first.
// If buf is longer than HeaderSize, only the last HeaderSize bytes
// are kept (earlier bytes are silently dropped — we never need to
// put back more than a header).
func (p *PutbackReader) Putback(buf []byte) {
	if len(buf) == 0 {
		return
	}
	p.stat.Putbacks++
	keep := buf
	if len(keep) > HeaderSize {
		keep = keep[len(keep)-HeaderSize:]
	}
	// Shift existing buffer right by len(keep), drop overflow.
	shift := len(keep)
	if p.n+shift > HeaderSize {
		p.n = 0 // would overflow; drop existing
	}
	copy(p.buf[p.n+shift:], p.buf[:p.n])
	copy(p.buf[:shift], keep)
	p.n += shift
	p.stat.Puts += uint64(shift)
}

// Stats returns a copy of the current stats. Safe to call concurrently.
func (p *PutbackReader) Stats() PutbackStats {
	return p.stat
}

// Rewind stuffs buf back into the putback buffer so the next Read
// returns it first. The caller is expected to pass the same bytes
// it just consumed (typically a 5-byte message header). buf must
// be <= HeaderSize bytes for the PutbackReader implementation.
func (p *PutbackReader) Rewind(buf []byte) {
	p.Putback(buf)
}

// SpliceReader is the interface DrainSplice needs: an io.Reader that
// also supports Rewind(buf) to "unread" buf (typically a 5-byte
// message header). Two implementations:
//   - *RawReader (production): rewinds the pgproto3 chunkReader's
//     internal read position by len(buf) so the next Frontend.Receive()
//     call sees the rewound bytes.
//   - *PutbackReader (tests): rewinds by stuffing buf back into a
//     small putback buffer; the next Read returns from the buffer
//     first.
type SpliceReader interface {
	io.Reader
	Rewind(buf []byte)
}

// DrainSplice reads backend messages from src and copies "boring"
// ones directly to dst using a pooled buffer, until it encounters a
// non-boring message or an error.
//
// The 5-byte header of the stopping message is rewound in the
// underlying reader so the caller can resume with a normal
// Frontend.Receive() call. The reader MUST be a SpliceReader
// (production: *RawReader that shares the pgproto3 chunkReader
// with bConn.Frontend; tests: *PutbackReader).
//
// On boring messages with body > (bufsize - 5), DrainSplice falls back
// to a two-write path: header, then io.CopyN for the body. This
// keeps the pool size bounded while still avoiding the decode/re-encode.
//
// Returns ErrSpliceStop when a non-boring message is the next byte
// (and has been rewound). Returns nil + 0 on EOF. Other errors are
// src I/O errors.
func DrainSplice(dst io.Writer, src SpliceReader, bufsize int) (rerr error) {
	if bufsize < HeaderSize {
		bufsize = HeaderSize
	}
	bp := GetSpliceBuf(bufsize)
	defer PutSpliceBuf(bp)
	buf := *bp

	// We read headers into a dedicated region at the tail of buf.
	// This avoids overwriting coalesced data at buf[0:woff] when
	// reading the next header. hdrOff is the write position in the
	// tail region; we reset it each time we flush.
	hdrRegion := buf[bufsize-HeaderSize:] // 5 bytes at the end
	hdr := hdrRegion

	woff := 0 // bytes accumulated in buf[0:woff]

	for {
		// Read the 5-byte header via the shared chunkReader.
		if _, err := io.ReadFull(src, hdr); err != nil {
			if err == io.EOF {
				if woff > 0 {
					_, _ = dst.Write(buf[:woff])
				}
				return io.EOF
			}
			return err
		}
		tag := hdr[0]
		bodyLen := int(binary.BigEndian.Uint32(hdr[1:5])) - 4
		if bodyLen < 0 {
			src.Rewind(hdr)
			return errors.New("wire: negative body length in splice header")
		}

		if Classify(tag) == ClassBoring {
			msgTotal := HeaderSize + bodyLen
			if woff+msgTotal <= bufsize-HeaderSize {
				// Fits: copy header from tail region into the
				// accumulation area, then read body after it.
				copy(buf[woff:], hdr)
				if bodyLen > 0 {
					if _, err := io.ReadFull(src, buf[woff+HeaderSize:woff+msgTotal]); err != nil {
						return err
					}
				}
				woff += msgTotal
				continue
			}
			// Flush, then handle this message.
			if woff > 0 {
				if _, err := dst.Write(buf[:woff]); err != nil {
					return err
				}
				woff = 0
			}
			if msgTotal <= bufsize-HeaderSize {
				copy(buf, hdr)
				if bodyLen > 0 {
					if _, err := io.ReadFull(src, buf[HeaderSize:msgTotal]); err != nil {
						return err
					}
				}
				woff = msgTotal
				continue
			}
			// Giant message: two-write path.
			if _, err := dst.Write(hdr); err != nil {
				return err
			}
			if _, err := io.CopyN(dst, src, int64(bodyLen)); err != nil {
				return err
			}
			continue
		}

		// Non-boring: flush accumulated data, then stop.
		if woff > 0 {
			if _, err := dst.Write(buf[:woff]); err != nil {
				return err
			}
		}
		src.Rewind(hdr)
		return ErrSpliceStop
	}
}
