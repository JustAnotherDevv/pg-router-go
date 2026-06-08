package pgrouter

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pg-router-go/internal/config"
	"github.com/JustAnotherDevv/pg-router-go/internal/testutil"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestNewRejectsBadConfig(t *testing.T) {
	_, err := New(nil, nil)
	require.Error(t, err)
}

func TestNewRejectsValidationFailure(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{ListenPort: 99999, MaxClientConn: 1},
	}
	_, err := New(cfg, testutil.Discard)
	require.Error(t, err)
}

func TestServerStartsAcceptingTCP(t *testing.T) {
	port := freePort(t)
	cfg := &config.Config{
		Server: config.ServerConfig{
			ListenAddr:    "127.0.0.1",
			ListenPort:    port,
			MaxClientConn: 10,
		},
		Pool: config.PoolConfig{
			Mode:            config.PoolModeTransaction,
			DefaultPoolSize: 4,
			SkipPreflight:   true, // SB9: no real PG in unit test
		},
		Auth: config.AuthConfig{Type: config.AuthTrust},
		TLS:  config.TLSConfig{ClientMode: config.SSLDisable, ServerMode: config.SSLDisable},
		Databases: map[string]config.DatabaseConfig{
			"appdb": {Host: "127.0.0.1", Port: 5432, DBName: "appdb"},
		},
		Metrics: config.MetricsConfig{Listen: ":0", Path: "/metrics"},
	}
	srv, err := New(cfg, testutil.Discard)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, srv.Start(ctx))
	defer srv.Stop(2 * time.Second)

	// Verify the listener is up.
	deadline := time.Now().Add(2 * time.Second)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server did not accept on %s", addr)
}
