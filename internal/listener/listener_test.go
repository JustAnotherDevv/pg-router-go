package listener

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JustAnotherDevv/pg-router-go/internal/testutil"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return testutil.Discard
}

// TestListenerAcceptsAndDispatches verifies the listener spawns a
// goroutine per connection and calls the handler.
func TestListenerAcceptsAndDispatches(t *testing.T) {
	ln, err := New("127.0.0.1:0", testLogger())
	require.NoError(t, err)

	var seen atomic.Int32
	handler := func(ctx context.Context, conn net.Conn) {
		defer conn.Close()
		seen.Add(1)
		// echo one read
		buf := make([]byte, 64)
		n, _ := conn.Read(buf)
		_, _ = conn.Write(buf[:n])
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- ln.Serve(ctx, handler) }()

	// Connect 3 clients.
	for i := 0; i < 3; i++ {
		c, err := net.Dial("tcp", ln.Addr().String())
		require.NoError(t, err)
		_, err = c.Write([]byte("ping"))
		require.NoError(t, err)
		buf := make([]byte, 4)
		_, err = io.ReadFull(c, buf)
		require.NoError(t, err)
		require.Equal(t, "ping", string(buf))
		_ = c.Close()
	}

	// Stop.
	cancel()
	select {
	case err := <-serveErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("listener did not shut down")
	}

	require.EqualValues(t, 3, seen.Load(), "handler should have been called for each connection")
}

// TestListenerGracefulShutdown verifies that in-flight handlers complete
// before Serve returns.
func TestListenerGracefulShutdown(t *testing.T) {
	ln, err := New("127.0.0.1:0", testLogger())
	require.NoError(t, err)

	done := make(chan struct{})
	handler := func(ctx context.Context, conn net.Conn) {
		defer conn.Close()
		// Block until ctx is cancelled.
		<-ctx.Done()
		close(done)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- ln.Serve(ctx, handler) }()

	c, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer c.Close()

	// Give the listener time to enter handler.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit on ctx cancel")
	}
	select {
	case err := <-serveErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("listener did not shut down after handler exited")
	}
}

// TestListenerInvalidAddr verifies bad addresses fail fast.
func TestListenerInvalidAddr(t *testing.T) {
	_, err := New("not-a-real-addr", testLogger())
	require.Error(t, err)
}
