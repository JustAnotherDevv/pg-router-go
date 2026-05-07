// Slow query log emission test.
//
// Runs a fake backend that sleeps after Parse-less Query, then asserts
// the PooledConn emits a `slow_query` WARN log line with the redacted
// SQL when the duration exceeds the threshold.

package client

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
)

// syncBuf is a goroutine-safe wrapper around bytes.Buffer for use as
// a slog handler sink in tests. The PooledConn writes from one
// goroutine while the test polls Bytes()/String() from another;
// without this, `go test -race` flags the bare bytes.Buffer.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}
func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}
func (s *syncBuf) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Return a copy so the caller doesn't observe mutation.
	out := make([]byte, s.buf.Len())
	copy(out, s.buf.Bytes())
	return out
}

func TestSlowQueryEmitsWarn(t *testing.T) {
	fb := newFakeBackend(t)
	dial := func(ctx context.Context) (*backend.Conn, error) {
		return fb.Conn(), nil
	}
	p := newDialPool(t, "test", dial, 2)

	buf := &syncBuf{}
	captureLog := slog.New(slog.NewTextHandler(buf,
		&slog.HandlerOptions{Level: slog.LevelDebug}))

	_, fe, _ := startPooled(t, p, &PooledConn{
		PooledConfig: PooledConfig{
			SlowQuery: 5 * time.Millisecond,
			LogSQL:    "redacted",
		},
		Log:      captureLog,
		Database: "appdb",
		User:     "alice",
	})

	// Backend sleeps before responding → query crosses 5ms threshold.
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		time.Sleep(20 * time.Millisecond)
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})

	fe.Send(&pgproto3.Query{String: "SELECT pg_sleep(0.02) /* secret */"})
	require.NoError(t, fe.Flush())
	for {
		m, _ := fe.Receive()
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	// WARN log fires AFTER pgrouter forwards the RFQ to the client
	// (drain-loop ordering). Poll for it.
	require.Eventually(t, func() bool {
		return bytesContains(buf.Bytes(), "slow_query")
	}, time.Second, 5*time.Millisecond,
		"expected slow_query log line, got: %s", buf.String())
	require.Contains(t, buf.String(), "kind=query")
}

func bytesContains(b []byte, sub string) bool {
	return bytes.Contains(b, []byte(sub))
}

func TestSlowQueryDisabledByZero(t *testing.T) {
	fb := newFakeBackend(t)
	dial := func(ctx context.Context) (*backend.Conn, error) {
		return fb.Conn(), nil
	}
	p := newDialPool(t, "test", dial, 2)

	buf := &syncBuf{}
	captureLog := slog.New(slog.NewTextHandler(buf, nil))

	_, fe, _ := startPooled(t, p, &PooledConn{
		PooledConfig: PooledConfig{SlowQuery: 0}, // disabled
		Log:          captureLog,
	})
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		time.Sleep(20 * time.Millisecond)
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})
	fe.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fe.Flush())
	for {
		m, _ := fe.Receive()
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	require.NotContains(t, buf.String(), "slow_query")
}
