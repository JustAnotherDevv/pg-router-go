package backend

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// pgAddr returns the test Postgres address. Default :25432 (local dev container).
func pgAddr() string {
	if v := os.Getenv("PGROUTER_TEST_PG_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:25432"
}

// TestDialTrust connects to a trust-auth Postgres and completes the handshake.
// Requires a running Postgres on PGROUTER_TEST_PG_ADDR (default :25432).
func TestDialTrust(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Postgres; skip in -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, DialOptions{
		Addr:     pgAddr(),
		User:     "test",
		Database: "test",
		AppName:  "pgrouter-test",
		Log:      slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	defer c.Close()

	require.NotZero(t, c.PostgresPID, "backend pid set")
	require.NotEmpty(t, c.SecretKey, "secret key set")
	require.NotEmpty(t, c.Params, "ParameterStatus values captured")
	// Standard Postgres always emits server_version.
	require.Contains(t, c.Params, "server_version")
}

// TestDialBadAddr verifies dial error path.
func TestDialBadAddr(t *testing.T) {
	if testing.Short() {
		t.Skip("network test; skip in -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := Dial(ctx, DialOptions{
		Addr:     "127.0.0.1:1",
		User:     "u",
		Database: "d",
		Timeout:  1 * time.Second,
		Log:      slog.New(slog.DiscardHandler),
	})
	require.Error(t, err)
}
