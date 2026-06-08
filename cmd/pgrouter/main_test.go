package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/JustAnotherDevv/pg-router-go/internal/config"
	"github.com/stretchr/testify/require"
)

func TestVersionSubcommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := realMain([]string{"version"}, &stdout, &stderr)
	require.Equal(t, 0, code)
	require.Contains(t, stdout.String(), "pgrouter")
	require.Contains(t, stdout.String(), version)
}

func TestHelpSubcommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := realMain([]string{"--help"}, &stdout, &stderr)
	require.Equal(t, 0, code)
	require.Contains(t, stdout.String(), "pgrouter run")
	require.Contains(t, stdout.String(), "pgrouter validate")
}

func TestNoArgsShowsUsageExit2(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := realMain(nil, &stdout, &stderr)
	require.Equal(t, 2, code)
	require.Contains(t, stderr.String(), "pgrouter run")
}

func TestUnknownSubcommandExit2(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := realMain([]string{"frobnicate"}, &stdout, &stderr)
	require.Equal(t, 2, code)
	require.Contains(t, stderr.String(), "unknown subcommand")
}

func TestValidateValidConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ok.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
databases:
  appdb:
    host: 127.0.0.1
`), 0o600))

	var stdout, stderr bytes.Buffer
	code := realMain([]string{"validate", path}, &stdout, &stderr)
	require.Equal(t, 0, code, "stderr: %s", stderr.String())
	require.Contains(t, stdout.String(), "OK:")
	require.Contains(t, stdout.String(), "appdb")
}

func TestValidateInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
server:
  listen_port: 99999
`), 0o600))

	var stdout, stderr bytes.Buffer
	code := realMain([]string{"validate", path}, &stdout, &stderr)
	require.Equal(t, 1, code)
	require.Contains(t, stderr.String(), "FAIL:")
}

func TestValidateMissingFile(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := realMain([]string{"validate", "/no/such/file.yaml"}, &stdout, &stderr)
	require.Equal(t, 1, code)
	require.Contains(t, stderr.String(), "FAIL:")
}

func TestValidateUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := realMain([]string{"validate"}, &stdout, &stderr)
	require.Equal(t, 2, code)
	require.Contains(t, stderr.String(), "usage:")
}

func TestLegacyConfigIsValid(t *testing.T) {
	path, cleanup, err := writeLegacyConfig(":16432", "localhost:15432", "json", "warn")
	require.NoError(t, err)
	defer cleanup()

	cfg, err := config.Load(path)
	require.NoError(t, err)
	require.Equal(t, "0.0.0.0", cfg.Server.ListenAddr)
	require.Equal(t, 16432, cfg.Server.ListenPort)
	require.True(t, cfg.Pool.SkipPreflight)
	require.Equal(t, "127.0.0.1:0", cfg.Metrics.Listen)
	require.Equal(t, "localhost", cfg.Databases["postgres"].Host)
	require.Equal(t, 15432, cfg.Databases["postgres"].Port)
}

// TestSampleConfigsAreValid is run from the package directory; the
// shipped samples must validate cleanly.
func TestSampleConfigsAreValid(t *testing.T) {
	samples := []string{
		"../../examples/configs/basic.yaml",
		"../../examples/configs/session-mode.yaml",
		"../../examples/configs/multi-pool.yaml",
	}
	for _, s := range samples {
		t.Run(filepath.Base(s), func(t *testing.T) {
			if _, err := os.Stat(s); err != nil {
				t.Skipf("sample not present at %s: %v", s, err)
			}
			var stdout, stderr bytes.Buffer
			code := realMain([]string{"validate", s}, &stdout, &stderr)
			require.Equal(t, 0, code, "stderr: %s", stderr.String())
		})
	}
}
