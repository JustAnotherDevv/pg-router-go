// Verifies Conn.Close is idempotent + safe to call from multiple
// goroutines simultaneously. Janitor eviction + Serve error paths
// can both call Close on the same conn; without sync.Once the
// second SetWriteDeadline → Send(Terminate) → Flush sequence on
// an already-closed socket races and on some platforms panics.

package backend

import (
	"net"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestConnCloseIdempotent(t *testing.T) {
	// net.Pipe gives us a fake socket pair we can close + reclose
	// without needing a live PG.
	cl, sv := net.Pipe()
	defer sv.Close()
	c := &Conn{
		NetConn:  cl,
		Frontend: pgproto3.NewFrontend(cl, cl),
	}

	// First close returns the underlying close result; further closes
	// MUST return the same error without panicking or re-sending
	// Terminate.
	err1 := c.Close()
	require.NoError(t, err1)
	err2 := c.Close()
	require.NoError(t, err2)
	err3 := c.Close()
	require.NoError(t, err3)
}

func TestConnCloseConcurrent(t *testing.T) {
	cl, sv := net.Pipe()
	defer sv.Close()
	c := &Conn{
		NetConn:  cl,
		Frontend: pgproto3.NewFrontend(cl, cl),
	}

	// 16 goroutines race on Close. With sync.Once exactly one
	// SetWriteDeadline + Send + Flush + NetConn.Close fires;
	// without it, we'd see panics or double-close races (visible
	// under -race in CI).
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Close()
		}()
	}
	wg.Wait()
}
