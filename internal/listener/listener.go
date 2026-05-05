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

	"github.com/JustAnotherDevv/pgrouter/internal/stats"
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

	// proxyProtocolStrict, when true alongside proxyProtocol, rejects
	// bare connections (no preamble) instead of falling through to
	// the readerConn wrap. Bare-conn count flows to the
	// pgrouter_proxy_proto_missing_total counter regardless.
	proxyProtocolStrict bool
}

// EnableProxyProtocol switches on PROXY v1/v2 preamble parsing on every
// accepted conn. Must be called before Serve.
//
// `strict`=true rejects connections that don't present a PROXY
// preamble; false logs the miss + accepts the bare conn (for rollout
// safety).
func (l *Listener) EnableProxyProtocol(strict bool) *Listener {
	l.proxyProtocol = true
	l.proxyProtocolStrict = strict
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
				wrapped, ok := l.parsePROXYOrWrap(c)
				if !ok {
					return // parse failed; conn already closed.
				}
				c = wrapped
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

// parsePROXYOrWrap handles the 4-way PROXY-preamble outcome:
//
//	1. Valid PROXY header with real source addr → return proxyConn.
//	2. Valid PROXY header without source (LOCAL/UNKNOWN) → return readerConn.
//	3. No PROXY header (bare client during rollout) → return readerConn.
//	4. Parse error → log + close + return ok=false.
//
// ok=false means the caller MUST return without invoking the handler;
// the conn has already been closed.
//
// Tight deadline: PROXY preambles are ≤108 bytes (v1) or ≤16+~36 (v2);
// a healthy LB ships them in one TCP segment. 1s is generous;
// 5s gave slow-read attackers ~max_client_conn × 5s goroutine pin.
func (l *Listener) parsePROXYOrWrap(c net.Conn) (net.Conn, bool) {
	_ = c.SetReadDeadline(time.Now().Add(1 * time.Second))
	info, br, err := ReadProxyHeader(c)
	_ = c.SetReadDeadline(time.Time{})
	switch {
	case err == nil && info.SourceAddr != nil:
		return WithProxyAddr(c, info.SourceAddr, br), true
	case errors.Is(err, ErrNoProxyHeader):
		// Bare client (no preamble). Strict mode rejects + tracks; lax
		// mode tracks + wraps the conn so rollout doesn't drop traffic.
		stats.OnProxyProtoMissing()
		if l.proxyProtocolStrict {
			l.log.Warn("PROXY strict mode: closing bare connection",
				"remote", c.RemoteAddr().String())
			_ = c.Close()
			return nil, false
		}
		return &readerConn{conn: c, r: br}, true
	case err == nil:
		// LOCAL/UNKNOWN PROXY command — keep underlying addr.
		return &readerConn{conn: c, r: br}, true
	default:
		l.log.Warn("PROXY header parse failed; closing",
			"err", err, "remote", c.RemoteAddr().String())
		_ = c.Close()
		return nil, false
	}
}
