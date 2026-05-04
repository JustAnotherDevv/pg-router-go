package client

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
)

// TestWelcomeUsesUpstreamParamsAfterFirstDial verifies that a client
// connecting through a pool whose upstream has been dialed at least
// once sees the REAL upstream's ParameterStatus (server_version etc.)
// in the welcome, NOT the CannedParams fallback.
func TestWelcomeUsesUpstreamParamsAfterFirstDial(t *testing.T) {
	// Dial returns a backend.Conn whose Params include a distinctive
	// server_version that the welcome MUST surface.
	dial := func(_ context.Context) (*backend.Conn, error) {
		return &backend.Conn{
			Params: map[string]string{
				"server_version":  "16.4 (Real-Upstream-Version)",
				"client_encoding": "UTF8",
				"TimeZone":        "Etc/UTC",
				"is_superuser":    "off",
			},
		}, nil
	}
	p := pool.New("welcome-real", dial, pool.Config{
		DefaultPoolSize: 2,
		QueryWait:       time.Second,
		Log:             slog.New(slog.DiscardHandler),
	})
	defer p.Close()

	// Pre-warm: first acquire populates the cache.
	c, err := p.Acquire(context.Background())
	require.NoError(t, err)
	p.Release(c, false)
	require.NotNil(t, p.CachedParams())

	// Now run a PooledConn welcome and inspect the wire it produced.
	cli, srv := net.Pipe()
	defer cli.Close()
	defer srv.Close()
	be := pgproto3.NewBackend(srv, srv)

	pc := &PooledConn{
		PooledConfig: PooledConfig{
			CannedParams: map[string]string{
				"server_version": "WRONG-fallback-value", // must be overridden
				"DateStyle":      "ISO, MDY",             // canned-only fills gap
			},
		},
		Log:  slog.New(slog.DiscardHandler),
		Pool: p,
	}

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- pc.sendWelcome(context.Background(), be)
	}()

	// Read the welcome on the client side.
	fe := pgproto3.NewFrontend(cli, cli)
	_ = cli.SetReadDeadline(time.Now().Add(time.Second))

	gotParams := map[string]string{}
	var sawAuthOk, sawKey, sawReady bool
	for !sawReady {
		msg, err := fe.Receive()
		require.NoError(t, err)
		switch m := msg.(type) {
		case *pgproto3.AuthenticationOk:
			sawAuthOk = true
		case *pgproto3.ParameterStatus:
			gotParams[m.Name] = m.Value
		case *pgproto3.BackendKeyData:
			sawKey = true
		case *pgproto3.ReadyForQuery:
			sawReady = true
			require.Equal(t, byte('I'), m.TxStatus)
		}
	}
	require.NoError(t, <-doneCh)
	require.True(t, sawAuthOk)
	require.True(t, sawKey)

	require.Equal(t, "16.4 (Real-Upstream-Version)", gotParams["server_version"],
		"server_version MUST come from upstream, not fallback")
	require.Equal(t, "UTF8", gotParams["client_encoding"])
	require.Equal(t, "Etc/UTC", gotParams["TimeZone"])
	require.Equal(t, "ISO, MDY", gotParams["DateStyle"],
		"canned values fill gaps the upstream didn't send")
}

// TestWelcomeFallsBackToCannedWhenPoolEmpty verifies the cold-start
// path: pool has never been dialed AND eager-warm fails (no usable
// dialer) → welcome uses CannedParams.
func TestWelcomeFallsBackToCannedWhenPoolEmpty(t *testing.T) {
	// A dialer that always errors — Pool will fail to warm.
	dialErr := func(_ context.Context) (*backend.Conn, error) {
		return nil, errFailDial
	}
	p := pool.New("welcome-cold", dialErr, pool.Config{
		DefaultPoolSize: 1,
		QueryWait:       50 * time.Millisecond,
		Log:             slog.New(slog.DiscardHandler),
	})
	defer p.Close()

	cli, srv := net.Pipe()
	defer cli.Close()
	defer srv.Close()
	be := pgproto3.NewBackend(srv, srv)

	pc := &PooledConn{
		PooledConfig: PooledConfig{
			CannedParams: map[string]string{
				"server_version":  "16.0 (canned)",
				"client_encoding": "UTF8",
			},
		},
		Log:  slog.New(slog.DiscardHandler),
		Pool: p,
	}

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- pc.sendWelcome(context.Background(), be)
	}()

	fe := pgproto3.NewFrontend(cli, cli)
	_ = cli.SetReadDeadline(time.Now().Add(time.Second))

	gotParams := map[string]string{}
	for {
		msg, err := fe.Receive()
		require.NoError(t, err)
		if ps, ok := msg.(*pgproto3.ParameterStatus); ok {
			gotParams[ps.Name] = ps.Value
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	require.NoError(t, <-doneCh)
	require.Equal(t, "16.0 (canned)", gotParams["server_version"])
}

// TestWelcomeEagerWarmsOnColdStart verifies the cold-start path: pool
// has never been dialed and CannedParams are empty, so welcomeParams
// MUST trigger an eager Acquire+Release that populates the cache.
func TestWelcomeEagerWarmsOnColdStart(t *testing.T) {
	dial := func(_ context.Context) (*backend.Conn, error) {
		return &backend.Conn{
			Params: map[string]string{"server_version": "16.1 (warmed)"},
		}, nil
	}
	p := pool.New("cold-warm", dial, pool.Config{
		DefaultPoolSize: 1,
		QueryWait:       time.Second,
		Log:             slog.New(slog.DiscardHandler),
	})
	defer p.Close()
	require.Nil(t, p.CachedParams(), "fresh pool: nothing cached")

	pc := &PooledConn{
		PooledConfig: PooledConfig{
			CannedParams: map[string]string{}, // forces eager warm
		},
		Log:  slog.New(slog.DiscardHandler),
		Pool: p,
	}
	params := pc.welcomeParams(context.Background())
	require.Equal(t, "16.1 (warmed)", params["server_version"])
	require.NotNil(t, p.CachedParams(), "cache populated by warm")
}

type sentinelErr string

func (e sentinelErr) Error() string { return string(e) }

var errFailDial sentinelErr = "test: dial failure"
