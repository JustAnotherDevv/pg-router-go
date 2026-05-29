// Package listener accepts client TCP connections and dispatches one
// goroutine per connection. PoC scope: TCP only; future tasks add Unix
// sockets, TLS, and PROXY protocol.
package listener

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
)

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
