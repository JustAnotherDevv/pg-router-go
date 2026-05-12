package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "pgrouter.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nope.yaml"))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConfigNotFound))
}

func TestLoadValidMinimal(t *testing.T) {
	p := writeTemp(t, `
databases:
  appdb:
    host: 127.0.0.1
`)
	cfg, err := Load(p)
	require.NoError(t, err)

	// Defaults applied.
	require.Equal(t, "0.0.0.0", cfg.Server.ListenAddr)
	require.Equal(t, 6432, cfg.Server.ListenPort)
	require.Equal(t, 1000, cfg.Server.MaxClientConn)
	require.Equal(t, PoolModeTransaction, cfg.Pool.Mode)
	require.Equal(t, 20, cfg.Pool.DefaultPoolSize)
	require.Equal(t, 600*time.Second, cfg.Pool.ServerIdle)
	require.Equal(t, AuthTrust, cfg.Auth.Type)
	require.Equal(t, SSLDisable, cfg.TLS.ClientMode)
	require.Equal(t, ":9090", cfg.Metrics.Listen)
	require.Equal(t, "/metrics", cfg.Metrics.Path)

	// Per-database defaults.
	db := cfg.Databases["appdb"]
	require.Equal(t, 5432, db.Port)
	require.Equal(t, "appdb", db.DBName)
	require.Equal(t, "127.0.0.1", db.Host)
}

func TestLoadFullConfig(t *testing.T) {
	cfg, err := Load(filepath.Join("testdata", "full_config.yaml"))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", cfg.Server.ListenAddr)
	require.Equal(t, 16432, cfg.Server.ListenPort)
	require.Equal(t, 500, cfg.Server.MaxClientConn)
	require.Equal(t, 30*time.Second, cfg.Server.ClientLogin)

	require.Equal(t, PoolModeSession, cfg.Pool.Mode)
	require.Equal(t, 10, cfg.Pool.DefaultPoolSize)
	require.Equal(t, 2, cfg.Pool.MinPoolSize)
	require.Equal(t, 3, cfg.Pool.ReservePoolSize)
	require.Equal(t, 60*time.Second, cfg.Pool.QueryWaitTimeout)
	require.Equal(t, 1800*time.Second, cfg.Pool.ServerLifetime)

	require.Equal(t, AuthSCRAM, cfg.Auth.Type)
	require.Equal(t, "/etc/pgrouter/userlist.txt", cfg.Auth.UserlistFile)

	require.Equal(t, SSLRequire, cfg.TLS.ClientMode)
	require.Equal(t, SSLVerifyFull, cfg.TLS.ServerMode)
	require.Equal(t, "/etc/ssl/certs/ca-bundle.pem", cfg.TLS.ServerCAFile)

	require.Equal(t, "127.0.0.1:9100", cfg.Metrics.Listen)
	require.Equal(t, "debug", cfg.Logging.Level)
	require.Equal(t, "json", cfg.Logging.Format)

	require.Equal(t, "db.internal", cfg.Databases["appdb"].Host)
	require.Equal(t, 5433, cfg.Databases["appdb"].Port)
	require.Equal(t, "production", cfg.Databases["appdb"].DBName)
	require.Equal(t, PoolModeSession, cfg.Databases["reporting"].PoolMode)
	require.Equal(t, 5, cfg.Databases["reporting"].PoolSize)

	require.Equal(t, 30, cfg.Users["app_user"].PoolSize)
	require.Equal(t, PoolModeSession, cfg.Users["reporter"].PoolMode)
}

func TestLoadRejectsUnknownField(t *testing.T) {
	p := writeTemp(t, `
server:
  listen_port: 6432
  totally_made_up: yes
databases:
  appdb:
    host: 127.0.0.1
`)
	_, err := Load(p)
	require.Error(t, err)
	require.Contains(t, err.Error(), "totally_made_up")
}

func TestLoadRejectsEmptyDatabases(t *testing.T) {
	p := writeTemp(t, `
server:
  listen_port: 6432
`)
	_, err := Load(p)
	require.Error(t, err)
	require.Contains(t, err.Error(), "databases")
	require.Contains(t, err.Error(), "at least one database")
}

// baseValidCfg returns a minimum-valid Config: trust auth, transaction
// pool, SSL disabled, one trivial database. Tests mutate one field to
// exercise specific Validate paths.
func baseValidCfg() *Config {
	return &Config{
		Server: ServerConfig{ListenPort: 6432, MaxClientConn: 100},
		Pool:   PoolConfig{Mode: PoolModeTransaction, DefaultPoolSize: 10},
		Auth:   AuthConfig{Type: AuthTrust},
		TLS:    TLSConfig{ClientMode: SSLDisable, ServerMode: SSLDisable},
		Databases: map[string]DatabaseConfig{
			"appdb": {Host: "127.0.0.1", Port: 5432},
		},
	}
}

// TestValidationCases drives the Validate path through a single table.
// Each entry mutates baseValidCfg() and asserts (a) Validate errors,
// and (b) each `wantContains` substring appears in the error.
func TestValidationCases(t *testing.T) {
	cases := []struct {
		name         string
		mutate       func(*Config)
		wantContains []string
	}{
		{
			name: "SCRAM requires userlist or auth_query",
			mutate: func(c *Config) {
				c.Auth.Type = AuthSCRAM
			},
			wantContains: []string{"userlist_file", "auth_query"},
		},
		{
			name: "peer requires unix_socket_dir",
			mutate: func(c *Config) {
				c.Auth.Type = AuthPeer
			},
			wantContains: []string{"unix_socket_dir"},
		},
		{
			name: "replica requires host and port",
			mutate: func(c *Config) {
				db := c.Databases["appdb"]
				db.Replicas = []ReplicaConfig{{Host: "", Port: 70000}}
				c.Databases["appdb"] = db
			},
			wantContains: []string{"replicas[0].host", "replicas[0].port"},
		},
		{
			name: "TLS require needs client cert",
			mutate: func(c *Config) {
				c.TLS.ClientMode = SSLRequire
			},
			wantContains: []string{"client_cert_file"},
		},
		{
			name: "server verify-ca needs CA file",
			mutate: func(c *Config) {
				c.TLS.ServerMode = SSLVerifyCA
			},
			wantContains: []string{"server_ca_file"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseValidCfg()
			tc.mutate(cfg)
			err := Validate(cfg)
			require.Error(t, err)
			msg := err.Error()
			for _, want := range tc.wantContains {
				require.Contains(t, msg, want)
			}
		})
	}
}

func TestValidationCollectsMultipleErrors(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{ListenPort: 999999, MaxClientConn: 0},
		Pool:   PoolConfig{Mode: "weird", DefaultPoolSize: 0, MinPoolSize: 999},
		Auth:   AuthConfig{Type: "ldap"},
		TLS:    TLSConfig{ClientMode: "bogus", ServerMode: SSLDisable},
		Databases: map[string]DatabaseConfig{
			"x": {Host: "", Port: 70000, PoolMode: "weird", PoolSize: -1},
		},
	}
	err := Validate(cfg)
	require.Error(t, err)
	var ve ValidationErrors
	require.ErrorAs(t, err, &ve)
	require.Greater(t, len(ve), 5, "expected many errors, got %d", len(ve))

	// Spot-check a couple of error paths.
	msg := err.Error()
	require.Contains(t, msg, "server.listen_port")
	require.Contains(t, msg, "pool.mode")
	require.Contains(t, msg, "auth.type")
	require.Contains(t, msg, "databases.x.host")
}

func TestValidationReplicaDefaultsApply(t *testing.T) {
	cfg := &Config{
		Databases: map[string]DatabaseConfig{
			"appdb": {Host: "127.0.0.1",
				Replicas: []ReplicaConfig{{Host: "replica.local"}}},
		},
	}
	applyDefaults(cfg)
	require.Equal(t, 5432, cfg.Databases["appdb"].Replicas[0].Port)
	require.Equal(t, 1, cfg.Databases["appdb"].Replicas[0].Weight)
}

func TestValidationPeerOKWithUnixSocketDir(t *testing.T) {
	cfg := baseValidCfg()
	cfg.Server.UnixSocketDir = "/var/run/pgrouter"
	cfg.Auth.Type = AuthPeer
	require.NoError(t, Validate(cfg))
}

func TestEmptyFilePassesThroughDefaults(t *testing.T) {
	p := writeTemp(t, "")
	_, err := Load(p)
	require.Error(t, err)
	// Empty file applies defaults but fails validation (no databases).
	require.Contains(t, err.Error(), "databases")
}

func TestErrorMessagesIncludeFieldPath(t *testing.T) {
	p := writeTemp(t, `
pool:
  mode: bogus
databases:
  appdb:
    host: 127.0.0.1
`)
	_, err := Load(p)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "pool.mode"))
	require.True(t, strings.Contains(err.Error(), "bogus"))
}
