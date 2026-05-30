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
	p := writeTemp(t, `
server:
  listen_addr: 127.0.0.1
  listen_port: 16432
  max_client_conn: 500
  client_login_timeout: 30s

pool:
  mode: session
  default_pool_size: 10
  min_pool_size: 2
  reserve_pool_size: 3
  query_wait_timeout: 60s
  server_idle_timeout: 300s
  server_lifetime: 1800s

auth:
  type: scram-sha-256
  userlist_file: /etc/pgrouter/userlist.txt

tls:
  client_mode: require
  client_cert_file: /etc/pgrouter/tls/cert.pem
  client_key_file: /etc/pgrouter/tls/key.pem
  server_mode: verify-full
  server_ca_file: /etc/ssl/certs/ca-bundle.pem

metrics:
  listen: 127.0.0.1:9100
  path: /metrics

logging:
  level: debug
  format: json

databases:
  appdb:
    host: db.internal
    port: 5433
    dbname: production
  reporting:
    host: warehouse.internal
    pool_mode: session
    pool_size: 5

users:
  app_user:
    pool_size: 30
  reporter:
    pool_mode: session
`)
	cfg, err := Load(p)
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

func TestValidationSCRAMRequiresUserlistOrAuthQuery(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{ListenPort: 6432, MaxClientConn: 100},
		Pool:   PoolConfig{Mode: PoolModeTransaction, DefaultPoolSize: 10},
		Auth:   AuthConfig{Type: AuthSCRAM},
		TLS:    TLSConfig{ClientMode: SSLDisable, ServerMode: SSLDisable},
		Databases: map[string]DatabaseConfig{
			"appdb": {Host: "127.0.0.1", Port: 5432},
		},
	}
	err := Validate(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "userlist_file")
	require.Contains(t, err.Error(), "auth_query")
}

func TestValidationPeerRequiresUnixSocketDir(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{ListenPort: 6432, MaxClientConn: 100},
		Pool:   PoolConfig{Mode: PoolModeTransaction, DefaultPoolSize: 10},
		Auth:   AuthConfig{Type: AuthPeer},
		TLS:    TLSConfig{ClientMode: SSLDisable, ServerMode: SSLDisable},
		Databases: map[string]DatabaseConfig{
			"appdb": {Host: "127.0.0.1", Port: 5432},
		},
	}
	err := Validate(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unix_socket_dir")
}

func TestValidationPeerOKWithUnixSocketDir(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{ListenPort: 6432, MaxClientConn: 100,
			UnixSocketDir: "/var/run/pgrouter"},
		Pool: PoolConfig{Mode: PoolModeTransaction, DefaultPoolSize: 10},
		Auth: AuthConfig{Type: AuthPeer},
		TLS:  TLSConfig{ClientMode: SSLDisable, ServerMode: SSLDisable},
		Databases: map[string]DatabaseConfig{
			"appdb": {Host: "127.0.0.1", Port: 5432},
		},
	}
	require.NoError(t, Validate(cfg))
}

func TestValidationTLSRequireNeedsCert(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{ListenPort: 6432, MaxClientConn: 100},
		Pool:   PoolConfig{Mode: PoolModeTransaction, DefaultPoolSize: 10},
		Auth:   AuthConfig{Type: AuthTrust},
		TLS:    TLSConfig{ClientMode: SSLRequire, ServerMode: SSLDisable},
		Databases: map[string]DatabaseConfig{
			"appdb": {Host: "127.0.0.1", Port: 5432},
		},
	}
	err := Validate(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "client_cert_file")
}

func TestValidationServerVerifyCAneedsCAFile(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{ListenPort: 6432, MaxClientConn: 100},
		Pool:   PoolConfig{Mode: PoolModeTransaction, DefaultPoolSize: 10},
		Auth:   AuthConfig{Type: AuthTrust},
		TLS:    TLSConfig{ClientMode: SSLDisable, ServerMode: SSLVerifyCA},
		Databases: map[string]DatabaseConfig{
			"appdb": {Host: "127.0.0.1", Port: 5432},
		},
	}
	err := Validate(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "server_ca_file")
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
