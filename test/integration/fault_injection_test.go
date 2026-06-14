// Fault-injection integration tests for pgrouter.
//
// Focus areas:
//   - slow query timeout
//   - pool exhaustion / query_wait timeout
//   - backend restart recovery
//   - active backend connection drop

//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func adminConnect(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, AdminDsn())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func TestFaultSlowQueryTimeout(t *testing.T) {
	conn := pgxConnect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	start := time.Now()
	_, err := conn.Exec(ctx, "SELECT pg_sleep(6)")
	require.Error(t, err)
	require.Less(t, time.Since(start), 9*time.Second)

	msg := strings.ToLower(err.Error())
	require.True(t,
		strings.Contains(msg, "query_timeout") ||
			strings.Contains(msg, "57014") ||
			strings.Contains(msg, "canceling statement"),
		"expected timeout-style error, got: %v", err)
}

func TestFaultPoolExhaustionReturnsWaitTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const held = 10 // matches integration harness default_pool_size
	var heldConns []*pgx.Conn
	var heldTxs []pgx.Tx

	for i := 0; i < held; i++ {
		c, err := pgx.Connect(ctx, Dsn())
		require.NoError(t, err)
		heldConns = append(heldConns, c)

		tx, err := c.Begin(ctx)
		require.NoError(t, err)
		heldTxs = append(heldTxs, tx)

		var one int
		require.NoError(t, tx.QueryRow(ctx, "SELECT 1").Scan(&one))
	}

	t.Cleanup(func() {
		for _, tx := range heldTxs {
			_ = tx.Rollback(context.Background())
		}
		for _, c := range heldConns {
			_ = c.Close(context.Background())
		}
	})

	start := time.Now()
	waiter, err := pgx.Connect(ctx, Dsn())
	if err == nil {
		defer waiter.Close(context.Background())
		_, err = waiter.Exec(ctx, "SELECT 1")
	}
	require.Error(t, err)

	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 4*time.Second)
	require.Less(t, elapsed, 9*time.Second)

	msg := strings.ToLower(err.Error())
	require.True(t,
		strings.Contains(msg, "acquire timeout") ||
			strings.Contains(msg, "wait") ||
			strings.Contains(msg, "timeout"),
		"expected wait-timeout style error, got: %v", err)
}

func TestFaultBackendRestartRecovers(t *testing.T) {
	faultHarnessReady(t)

	conn := pgxConnect(t)
	var one int
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT 1").Scan(&one))
	require.Equal(t, 1, one)

	dockerCompose(t, "restart", "postgres")
	require.NoError(t, waitForAdminReadyTimeout(30*time.Second))

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		c, err := pgx.Connect(ctx, Dsn())
		if err != nil {
			return false
		}
		defer c.Close(context.Background())

		var n int
		return c.QueryRow(ctx, "SELECT 1").Scan(&n) == nil && n == 1
	}, 30*time.Second, 500*time.Millisecond, "pgrouter did not recover after backend restart")
}

func TestFaultBackendConnectionDropDuringQuery(t *testing.T) {
	admin := adminConnect(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	victim, err := pgx.Connect(ctx, Dsn())
	require.NoError(t, err)
	defer victim.Close(context.Background())

	errCh := make(chan error, 1)
	go func() {
		_, err := victim.Exec(context.Background(), "SELECT pg_sleep(20)")
		errCh <- err
	}()

	var pid int32
	require.Eventually(t, func() bool {
		q := `
SELECT pid
FROM pg_stat_activity
WHERE query = 'SELECT pg_sleep(20)'
  AND state = 'active'
ORDER BY query_start DESC
LIMIT 1`
		err := admin.QueryRow(context.Background(), q).Scan(&pid)
		return err == nil && pid > 0
	}, 5*time.Second, 200*time.Millisecond, "did not find active backend query to terminate")

	var terminated bool
	require.NoError(t, admin.QueryRow(context.Background(),
		"SELECT pg_terminate_backend($1)", pid).Scan(&terminated))
	require.True(t, terminated)

	select {
	case err := <-errCh:
		require.Error(t, err)
		msg := strings.ToLower(err.Error())
		require.True(t,
			strings.Contains(msg, "terminating connection") ||
				strings.Contains(msg, "connection closed") ||
				strings.Contains(msg, "57p01") ||
				strings.Contains(msg, "broken pipe") ||
				strings.Contains(msg, "eof"),
			"expected backend-drop style error, got: %v", err)
	case <-time.After(8 * time.Second):
		t.Fatal("query did not fail after backend termination")
	}

	fresh, err := pgx.Connect(ctx, Dsn())
	require.NoError(t, err)
	defer fresh.Close(context.Background())

	var one int
	require.NoError(t, fresh.QueryRow(ctx, "SELECT 1").Scan(&one))
	require.Equal(t, 1, one)
}
