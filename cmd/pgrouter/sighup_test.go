// Phase A test for SIGHUP reload: the reloader must NOT shut down the
// process; it must re-read the config and log a diff.
//
// We test the goroutine in isolation (no real signal needed — the
// channel is just a chan os.Signal and we send syscall.SIGHUP into it
// directly).

package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/config"
	"github.com/JustAnotherDevv/pgrouter/internal/stats"
)

func newTestRegistry() *prometheus.Registry {
	return prometheus.NewRegistry()
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	require.NoError(t, err)
	return string(b)
}

// getCounter scrapes the package Reg and returns the float value of the
// named metric matching `labels` (subset-match: all label pairs must be
// present). Returns 0 if no series matches.
func getCounter(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := stats.Reg.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), labels) {
				if c := m.GetCounter(); c != nil {
					return c.GetValue()
				}
			}
		}
	}
	return 0
}

func labelsMatch(have []*dto.LabelPair, want map[string]string) bool {
	got := make(map[string]string, len(have))
	for _, lp := range have {
		got[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// minimalValidConfig writes a tiny config to disk that passes Load+Validate.
func minimalValidConfig(t *testing.T, defaultPoolSize int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pgrouter.yaml")
	body := "" +
		"server:\n" +
		"  listen_addr: 127.0.0.1\n" +
		"  listen_port: 6432\n" +
		"pool:\n" +
		"  mode: transaction\n" +
		"  default_pool_size: " + itoaSmall(defaultPoolSize) + "\n" +
		"auth:\n" +
		"  type: trust\n" +
		"databases:\n" +
		"  appdb:\n" +
		"    host: 127.0.0.1\n" +
		"    port: 5432\n"
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return path
}

// itoaSmall avoids strconv just so this test file's imports stay tight.
func itoaSmall(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestSighupReloaderRereadsAndDoesNotExitOnSignal(t *testing.T) {
	// Use a fresh registry so the global metric reads are deterministic.
	resetReg := stats.Reg
	stats.Reg = newTestRegistry()
	t.Cleanup(func() { stats.Reg = resetReg })
	_ = stats.New() // wires stats.Active

	path := minimalValidConfig(t, 10)
	cfg, err := config.Load(path)
	require.NoError(t, err)
	require.Equal(t, 10, cfg.Pool.DefaultPoolSize)

	hupCh := make(chan os.Signal, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		runSighupReloader(ctx, hupCh, path, cfg,
			slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	// First reload: same content. Should bump the "ok" metric.
	hupCh <- syscall.SIGHUP
	// Tiny sleep for the goroutine to drain the channel + finish a Load.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, float64(1), getCounter(t, "pgrouter_sighup_reloads_total", map[string]string{"outcome": "ok"}))

	// Mutate the file: pool size goes from 10 → 25.
	require.NoError(t, os.WriteFile(path,
		[]byte(strings.Replace(readFile(t, path), "default_pool_size: 10",
			"default_pool_size: 25", 1)), 0o644))

	hupCh <- syscall.SIGHUP
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, float64(2), getCounter(t, "pgrouter_sighup_reloads_total", map[string]string{"outcome": "ok"}))

	// Corrupt the file: empty databases map is a validation error.
	require.NoError(t, os.WriteFile(path, []byte("server:\n  listen_port: 6432\n"), 0o644))
	hupCh <- syscall.SIGHUP
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, float64(1), getCounter(t, "pgrouter_sighup_reloads_total", map[string]string{"outcome": "fail"}))

	// Cancel → reloader exits cleanly.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reloader did not exit on ctx cancel")
	}
}
