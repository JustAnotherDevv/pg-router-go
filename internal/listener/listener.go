// Package listener accepts client TCP connections and dispatches one
// goroutine per connection. PoC scope: TCP only; future tasks add Unix
// sockets, TLS, and PROXY protocol.
package listener

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

// readerConn drains a pre-peeked bufio reader before falling through to
// the underlying conn. Used when PROXY parsing peeked bytes that
// weren't a PROXY header — those bytes have to feed pgwire startup.
//
// Does NOT embed net.Conn — same rationale as proxyConn: a downstream
// type-assertion to the underlying TCPConn would drain raw bytes and
// skip the bufio reader holding the peeked preamble bytes.
type readerConn struct {
	conn net.Conn
	r    io.Reader
}

func (rc *readerConn) Read(b []byte) (int, error)         { return rc.r.Read(b) }
func (rc *readerConn) Write(b []byte) (int, error)        { return rc.conn.Write(b) }
func (rc *readerConn) Close() error                       { return rc.conn.Close() }
func (rc *readerConn) LocalAddr() net.Addr                { return rc.conn.LocalAddr() }
func (rc *readerConn) RemoteAddr() net.Addr               { return rc.conn.RemoteAddr() }
func (rc *readerConn) SetReadDeadline(t time.Time) error  { return rc.conn.SetReadDeadline(t) }
func (rc *readerConn) SetWriteDeadline(t time.Time) error { return rc.conn.SetWriteDeadline(t) }
func (rc *readerConn) SetDeadline(t time.Time) error      { return rc.conn.SetDeadline(t) }

// Handler is called once per accepted connection. The implementation owns
// the connection's lifetime; it MUST Close the conn before returning.
// ctx is cancelled when the listener is shutting down — handlers should
// honor cancellation for graceful shutdown.
type Handler func(ctx context.Context, conn net.Conn)

// Listener wraps a net.Listener with per-connection goroutine dispatch +
// graceful shutdown semantics.
type Listener struct {
	addr   string
	ln     net.Listener
	log    *slog.Logger
	wg     sync.WaitGroup

	// proxyProtocol, when true, parses a PROXY v1/v2 preamble off every
	// accepted conn and rewrites RemoteAddr to the real client. For use
	// behind HAProxy / AWS NLB / Cloudflare Spectrum.
	proxyProtocol bool
}

// EnableProxyProtocol switches on PROXY v1/v2 preamble parsing on every
// accepted conn. Must be called before Serve.
func (l *Listener) EnableProxyProtocol() *Listener {
	l.proxyProtocol = true
	return l
}

// New creates a Listener bound to addr (e.g. ":6432"). It does not start
// accepting until Serve is called.
func New(addr string, log *slog.Logger) (*Listener, error) {
	if log == nil {
		log = slog.Default()
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	return &Listener{
		addr: addr,
		ln:   ln,
		log:  log,
	}, nil
}

// Addr returns the bound address (useful when port :0 was requested).
func (l *Listener) Addr() net.Addr {
	return l.ln.Addr()
}

// Serve accepts connections in a loop, dispatching each to handler.
// Returns when ctx is cancelled or the listener fails irrecoverably.
// On return, Serve waits for all in-flight handlers to finish.
func (l *Listener) Serve(ctx context.Context, handler Handler) error {
	l.log.Info("listener started", "addr", l.ln.Addr().String())

	// Close listener on ctx cancel to unblock Accept.
	go func() {
		<-ctx.Done()
		_ = l.ln.Close()
	}()

	for {
		conn, err := l.ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				l.log.Info("listener shutting down", "reason", ctx.Err())
				break
			}
			// transient errors: log + continue
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				l.log.Warn("accept timeout", "err", err)
				continue
			}
			// otherwise treat as fatal
			l.wg.Wait()
			return fmt.Errorf("accept: %w", err)
		}

		l.wg.Add(1)
		go func(c net.Conn) {
			defer l.wg.Done()
			if l.proxyProtocol {
				// Tight deadline: PROXY preambles are ≤108 bytes (v1) or
				// ≤16+~36 (v2) — a healthy LB ships them in one TCP
				// segment. 1s is generous; slow-reads tying up
				// max_client_conn goroutines for 5s each is a DoS path.
				_ = c.SetReadDeadline(time.Now().Add(1 * time.Second))
				info, br, err := ReadProxyHeader(c)
				_ = c.SetReadDeadline(time.Time{})
				if err == nil && info.SourceAddr != nil {
					c = WithProxyAddr(c, info.SourceAddr, br)
				} else if err == ErrNoProxyHeader {
					// Strict mode (PROXY required) would reject here;
					// we currently accept bare connections so misconfig
					// during rollout doesn't drop traffic.
					c = &readerConn{conn: c, r: br}
				} else if err != nil {
					l.log.Warn("PROXY header parse failed; closing",
						"err", err, "remote", c.RemoteAddr().String())
					_ = c.Close()
					return
				} else {
					c = &readerConn{conn: c, r: br}
				}
			}
			l.log.Debug("client accepted", "remote", c.RemoteAddr().String())
			handler(ctx, c)
			l.log.Debug("client done", "remote", c.RemoteAddr().String())
		}(conn)
	}

	l.log.Info("waiting for in-flight handlers")
	l.wg.Wait()
	l.log.Info("listener stopped")
	return nil
}

// Close stops the listener immediately. Prefer cancelling ctx in Serve.
func (l *Listener) Close() error {
	return l.ln.Close()
}
