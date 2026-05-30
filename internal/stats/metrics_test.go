package stats

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// resetReg replaces the package-global registry so tests can call New()
// repeatedly without colliding on Already-registered.
func resetReg(t *testing.T) {
	t.Helper()
	orig := Reg
	Reg = prometheus.NewRegistry()
	t.Cleanup(func() { Reg = orig })
}

func TestNewRegistersAllMetrics(t *testing.T) {
	resetReg(t)
	m := New()

	// Sanity-check by incrementing one of each major family.
	m.ClientConnsTotal.Inc()
	m.BackendDialsTotal.Inc()
	m.QueriesTotal.WithLabelValues("appdb", "alice").Inc()
	m.AuthAttempts.Inc()
	m.CancelsReceived.Inc()
	m.PoolAcquireSeconds.WithLabelValues("appdb/alice").Observe(0.01)
	m.PoolWaitersGauge.WithLabelValues("appdb/alice").Set(2)

	// Gather + verify the families show up.
	families, err := Reg.Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, mf := range families {
		names[mf.GetName()] = true
	}
	wanted := []string{
		"pgrouter_client_connections_total",
		"pgrouter_backend_dials_total",
		"pgrouter_queries_total",
		"pgrouter_auth_attempts_total",
		"pgrouter_cancels_received_total",
		"pgrouter_pool_acquire_seconds",
		"pgrouter_pool_waiters",
		"go_goroutines", // from the runtime collector
	}
	for _, w := range wanted {
		require.True(t, names[w], "missing metric: %s", w)
	}
}

func TestServeMetricsEndpoint(t *testing.T) {
	resetReg(t)
	m := New()
	m.ClientConnsTotal.Inc()
	m.QueriesTotal.WithLabelValues("appdb", "alice").Add(7)

	// Pick a free port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- ServeMetrics(ctx, addr, "/metrics", slog.New(slog.DiscardHandler))
	}()

	// Poll until the server is up.
	deadline := time.Now().Add(2 * time.Second)
	var body string
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err == nil {
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			body = string(b)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.Contains(t, body, "pgrouter_client_connections_total 1")
	require.Contains(t, body, `pgrouter_queries_total{database="appdb",user="alice"} 7`)

	cancel()
	select {
	case err := <-doneCh:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("metrics server did not shut down")
	}
}

func TestHealthzReturns200(t *testing.T) {
	resetReg(t)
	_ = New()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ServeMetrics(ctx, addr, "/metrics", slog.New(slog.DiscardHandler))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			defer resp.Body.Close()
			require.Equal(t, 200, resp.StatusCode)
			b, _ := io.ReadAll(resp.Body)
			require.Equal(t, "ok", strings.TrimSpace(string(b)))
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("healthz never came up")
}
