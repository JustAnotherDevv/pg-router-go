// CountingConn wraps a net.Conn and fires callbacks on each Read /
// Write with the byte count. Used by pgrouter to attribute per-tenant
// bandwidth to Prometheus counters labelled {database, user}.
//
// Use:
//
//	wrapped := util.NewCountingConn(conn,
//	    func(n int) { stats.OnBytesIn(db, user, n) },
//	    func(n int) { stats.OnBytesOut(db, user, n) },
//	)
//	defer wrapped.Close()

package util

import "net"

// CountingConn is a net.Conn that reports Read + Write byte counts
// via callbacks. nil callbacks are skipped (so callers can wire just
// one direction if they want).
type CountingConn struct {
	net.Conn
	onIn  func(n int)
	onOut func(n int)
}

// NewCountingConn wraps conn.
func NewCountingConn(conn net.Conn, onIn, onOut func(n int)) *CountingConn {
	return &CountingConn{Conn: conn, onIn: onIn, onOut: onOut}
}

// Read forwards to the underlying conn and reports bytes read.
func (c *CountingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 && c.onIn != nil {
		c.onIn(n)
	}
	return n, err
}

// Write forwards to the underlying conn and reports bytes written.
func (c *CountingConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if n > 0 && c.onOut != nil {
		c.onOut(n)
	}
	return n, err
}
