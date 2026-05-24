// Package rawfd provides a concrete net.Conn implementation backed by
// a *net.TCPConn, eliminating interface dispatch overhead in the hot
// path. Every Read/Write goes directly to the TCP socket via the Go
// poller with zero intermediate interface indirection.
//
// Byte counting is done inline via atomic counters, replacing the
// CountingConn closure + interface dispatch approach.
package rawfd

import (
	"net"
	"sync/atomic"
	"time"
)

// ByteCounter is a thread-safe byte counter using atomic operations.
// Replaces CountingConn's closure-based counting.
type ByteCounter struct {
	n atomic.Int64
}

// Add increments the counter by n bytes.
func (c *ByteCounter) Add(n int) {
	if c != nil {
		c.n.Add(int64(n))
	}
}

// Value returns the current count.
func (c *ByteCounter) Value() int64 {
	if c == nil {
		return 0
	}
	return c.n.Load()
}

// RawFD wraps a *net.TCPConn with concrete Read/Write methods and
// inline byte counting. This eliminates:
//   - net.Conn interface dispatch (concrete method call instead)
//   - CountingConn closure overhead (atomic counter instead)
//   - CountingConn interface dispatch (direct *net.TCPConn call)
//
// The Go poller integration is preserved — Read/Write use the same
// internal/poll.FD path as net.TCPConn.
type RawFD struct {
	conn     *net.TCPConn
	bytesIn  *ByteCounter
	bytesOut *ByteCounter
}

// New creates a RawFD from a net.Conn. The conn is type-asserted to
// *net.TCPConn (which it always is for TCP connections).
func New(conn net.Conn, bytesIn, bytesOut *ByteCounter) *RawFD {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		// Try to unwrap CountingConn or other wrappers
		if cc, ok := conn.(interface{ Unwrap() net.Conn }); ok {
			tc, _ = cc.Unwrap().(*net.TCPConn)
		}
	}
	if tc == nil {
		panic("rawfd: cannot extract *net.TCPConn")
	}
	return &RawFD{conn: tc, bytesIn: bytesIn, bytesOut: bytesOut}
}

// Read reads from the TCP socket. The call goes directly to
// *net.TCPConn.Read → netFD.Read → poll.FD.Read with zero
// intermediate interface dispatch.
func (r *RawFD) Read(buf []byte) (int, error) {
	n, err := r.conn.Read(buf)
	if n > 0 {
		r.bytesIn.Add(n)
	}
	return n, err
}

// Write writes to the TCP socket. Same concrete dispatch chain as Read.
func (r *RawFD) Write(buf []byte) (int, error) {
	n, err := r.conn.Write(buf)
	if n > 0 {
		r.bytesOut.Add(n)
	}
	return n, err
}

// ReadFull reads exactly len(buf) bytes. Inline implementation avoids
// io.ReadFull's interface dispatch through io.Reader.
func (r *RawFD) ReadFull(buf []byte) (int, error) {
	off := 0
	for off < len(buf) {
		n, err := r.conn.Read(buf[off:])
		off += n
		if err != nil {
			return off, err
		}
	}
	return off, nil
}

func (r *RawFD) SetReadDeadline(t time.Time) error  { return r.conn.SetReadDeadline(t) }
func (r *RawFD) SetWriteDeadline(t time.Time) error { return r.conn.SetWriteDeadline(t) }
func (r *RawFD) SetDeadline(t time.Time) error      { return r.conn.SetDeadline(t) }
func (r *RawFD) Close() error                        { return r.conn.Close() }
func (r *RawFD) RemoteAddr() net.Addr                { return r.conn.RemoteAddr() }
func (r *RawFD) LocalAddr() net.Addr                 { return r.conn.LocalAddr() }
