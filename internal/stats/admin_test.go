package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// startTestServer spins up ServeMetricsAndAdmin on a free port and
// returns the base URL + a cleanup func.
func startTestServer(t *testing.T, opts AdminServerOptions) (string, func()) {
	t.Helper()
	resetReg(t)
	_ = New()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	_ = ln.Close()
	opts.Addr = addr

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = ServeMetricsAndAdmin(ctx, opts, slog.New(slog.DiscardHandler))
		close(done)
	}()
	// Poll until up.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := http.Get("http://" + addr + "/healthz"); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return "http://" + addr, func() { cancel(); <-done }
}

func TestAdminVersionEndpoint(t *testing.T) {
	Build.Version = "0.5.0-test"
	Build.Commit = "abc123"
	base, stop := startTestServer(t, AdminServerOptions{})
	defer stop()
	resp, err := http.Get(base + "/api/v1/version")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	var v VersionInfo
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&v))
	require.Equal(t, "0.5.0-test", v.Version)
	require.Equal(t, "abc123", v.Commit)
}

func TestAdminPoolsEndpoint(t *testing.T) {
	admin := &AdminAPI{
		Pools: func() ([]PoolSnapshot, error) {
			return []PoolSnapshot{
				{Name: "appdb/alice", DB: "appdb", User: "alice", Idle: 3, Active: 1, Waiters: 0},
			}, nil
		},
	}
	base, stop := startTestServer(t, AdminServerOptions{Admin: admin})
	defer stop()
	resp, err := http.Get(base + "/api/v1/pools")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	var pools []PoolSnapshot
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&pools))
	require.Len(t, pools, 1)
	require.Equal(t, "alice", pools[0].User)
}

func TestAdminStatsEndpoint(t *testing.T) {
	admin := &AdminAPI{
		Stats: func() (StatsSnapshot, error) {
			return StatsSnapshot{UptimeSeconds: 42, QueriesTotal: 100}, nil
		},
	}
	base, stop := startTestServer(t, AdminServerOptions{Admin: admin})
	defer stop()
	resp, err := http.Get(base + "/api/v1/stats")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	var s StatsSnapshot
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&s))
	require.Equal(t, 42.0, s.UptimeSeconds)
	require.Equal(t, 100.0, s.QueriesTotal)
}

func TestAdminDrainRequiresPost(t *testing.T) {
	admin := &AdminAPI{
		Drain: func(d time.Duration) error { return nil },
	}
	base, stop := startTestServer(t, AdminServerOptions{Admin: admin})
	defer stop()
	resp, err := http.Get(base + "/api/v1/drain")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 405, resp.StatusCode)
}

func TestAdminDrainOK(t *testing.T) {
	called := false
	admin := &AdminAPI{
		Drain: func(d time.Duration) error {
			called = true
			require.Equal(t, 10*time.Second, d)
			return nil
		},
	}
	base, stop := startTestServer(t, AdminServerOptions{Admin: admin})
	defer stop()
	resp, err := http.Post(base+"/api/v1/drain", "application/json",
		bytes.NewBufferString(`{"deadline_seconds":10}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	require.True(t, called)
}

func TestAdminReloadOK(t *testing.T) {
	called := false
	admin := &AdminAPI{
		Reload: func() error { called = true; return nil },
	}
	base, stop := startTestServer(t, AdminServerOptions{Admin: admin})
	defer stop()
	resp, err := http.Post(base+"/api/v1/reload", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	require.True(t, called)
}

func TestAdminTokenRejectsUnauth(t *testing.T) {
	admin := &AdminAPI{
		Drain: func(d time.Duration) error { return nil },
	}
	base, stop := startTestServer(t, AdminServerOptions{
		Admin: admin, AuthToken: "secret-pw",
	})
	defer stop()
	// No header → 401.
	resp, err := http.Post(base+"/api/v1/drain", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 401, resp.StatusCode)
}

func TestAdminTokenAcceptsCorrect(t *testing.T) {
	admin := &AdminAPI{
		Drain: func(d time.Duration) error { return nil },
	}
	base, stop := startTestServer(t, AdminServerOptions{
		Admin: admin, AuthToken: "secret-pw",
	})
	defer stop()
	req, _ := http.NewRequest("POST", base+"/api/v1/drain", nil)
	req.Header.Set("Authorization", "Bearer secret-pw")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
}

func TestAdminNilHandlerReturns501(t *testing.T) {
	base, stop := startTestServer(t, AdminServerOptions{Admin: nil})
	defer stop()
	resp, err := http.Get(base + "/api/v1/pools")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 501, resp.StatusCode)
	_, _ = io.Copy(io.Discard, resp.Body)
}
