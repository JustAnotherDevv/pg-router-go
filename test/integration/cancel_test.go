// Live CancelRequest routing through pgrouter to real Postgres.
//
// Flow:
//   1. Open a connection through pgrouter.
//   2. Capture its BackendKeyData (pid, secret) — pgrouter advertises a
//      synthetic pid/secret allocated from its cancel.Tracker.
//   3. Spawn a goroutine running `SELECT pg_sleep(10)`.
//   4. From a SECOND TCP connection to pgrouter's port, send the
//      CancelRequest packet with the captured (pid, secret).
//   5. Assert the sleep query returns with a "canceling statement due
//      to user request" / SQLSTATE 57014 error within a few seconds.

//go:build integration

package integration

import (
	"context"
	"encoding/binary"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestCancelRoutesToBackend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := pgconn.Connect(ctx, Dsn())
	require.NoError(t, err)
	defer conn.Close(context.Background())

	pid := conn.PID()
	secretBytes := conn.SecretKey()
	require.NotZero(t, pid)
	require.GreaterOrEqual(t, len(secretBytes), 4, "need at least 4-byte secret")
	secret := binary.BigEndian.Uint32(secretBytes[:4])

	// Find pgrouter host:port from DSN.
	cfg, err := pgconn.ParseConfig(Dsn())
	require.NoError(t, err)
	host := cfg.Host
	port := int(cfg.Port)

	// Issue a long-running query in a goroutine.
	done := make(chan error, 1)
	go func() {
		_, err := conn.Exec(context.Background(), "SELECT pg_sleep(10)").ReadAll()
		done <- err
	}()

	// Give the query a moment to start.
	time.Sleep(300 * time.Millisecond)

	require.NoError(t, sendRawCancel(host, port, pid, secret))

	select {
	case e := <-done:
		require.Error(t, e, "long query should have been cancelled")
		msg := strings.ToLower(e.Error())
		require.True(t,
			strings.Contains(msg, "canceling statement") ||
				strings.Contains(msg, "57014") ||
				strings.Contains(msg, "user request"),
			"expected cancellation error, got: %v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("query was not cancelled within 5s")
	}
}

func TestCancelWithUnknownPidIsDropped(t *testing.T) {
	cfg, err := pgconn.ParseConfig(Dsn())
	require.NoError(t, err)
	// Send a bogus cancel — pgrouter's tracker has no entry, so it
	// should drop silently (no panic, no error packet to client; the
	// TCP connection from us is just closed).
	require.NoError(t, sendRawCancel(cfg.Host, int(cfg.Port), 0xDEADBEEF, 0xCAFEF00D))
}

// sendRawCancel opens a fresh TCP conn to (host, port), sends the
// 16-byte CancelRequest, and closes. Wire format (PG docs):
//
//	int32 length      = 16
//	int32 protocol    = 80877102 (0x04d2162e)
//	int32 pid
//	int32 secret_key
func sendRawCancel(host string, port int, pid, secret uint32) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()

	pkt := []byte{
		0, 0, 0, 16, // length
		0x04, 0xd2, 0x16, 0x2e, // 80877102
		byte(pid >> 24), byte(pid >> 16), byte(pid >> 8), byte(pid),
		byte(secret >> 24), byte(secret >> 16), byte(secret >> 8), byte(secret),
	}
	_ = c.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err = c.Write(pkt)
	return err
}
