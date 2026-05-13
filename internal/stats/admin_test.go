package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
	"github.com/stretchr/testify/require"
)

// requireGet calls http.Get(url), asserts the status code, and returns
// the response body for streaming decode. Body close is registered via
// t.Cleanup. Saves the 4-line `resp,err := Get; NoError; defer Close;
// Equal(status)` motif at every admin endpoint call site.
func requireGet(t *testing.T, url string, wantStatus int) io.Reader {
	t.Helper()
	resp, err := http.Get(url) //nolint:gosec // test URL
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, wantStatus, resp.StatusCode)
	return resp.Body
}

// requirePost is the Post equivalent of requireGet.
func requirePost(t *testing.T, url, contentType string, body io.Reader, wantStatus int) io.Reader {
	t.Helper()
	resp, err := http.Post(url, contentType, body) //nolint:gosec // test URL
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, wantStatus, resp.StatusCode)
	return resp.Body
}

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
		_ = ServeMetricsAndAdmin(ctx, opts, testutil.Discard)
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
	var v VersionInfo
	require.NoError(t, json.NewDecoder(requireGet(t, base + "/api/v1/version", 200)).Decode(&v))
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
	var pools []PoolSnapshot
	require.NoError(t, json.NewDecoder(requireGet(t, base + "/api/v1/pools", 200)).Decode(&pools))
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
	var s StatsSnapshot
	require.NoError(t, json.NewDecoder(requireGet(t, base + "/api/v1/stats", 200)).Decode(&s))
	require.Equal(t, 42.0, s.UptimeSeconds)
	require.Equal(t, 100.0, s.QueriesTotal)
}

func TestAdminDrainRequiresPost(t *testing.T) {
	admin := &AdminAPI{
		Drain: func(d time.Duration) error { return nil },
	}
	base, stop := startTestServer(t, AdminServerOptions{Admin: admin})
	defer stop()
	_ = requireGet(t, base + "/api/v1/drain", 405)
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
	_ = requirePost(t, base+"/api/v1/drain", "application/json", bytes.NewBufferString(`{"deadline_seconds":10}`), 200)
	require.True(t, called)
}

func TestAdminReloadOK(t *testing.T) {
	called := false
	admin := &AdminAPI{
		Reload: func() error { called = true; return nil },
	}
	base, stop := startTestServer(t, AdminServerOptions{Admin: admin})
	defer stop()
	_ = requirePost(t, base+"/api/v1/reload", "application/json", nil, 200)
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
	_ = requirePost(t, base+"/api/v1/drain", "application/json", nil, 401)
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
	_, _ = io.Copy(io.Discard, requireGet(t, base+"/api/v1/pools", 501))
}
