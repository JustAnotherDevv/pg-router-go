package config

import "time"

// Config is the top-level pgrouter configuration.
//
// MVP scope: matches the §20.9 settings list culled to what's
// implementable through MVP M.15. PgBouncer-compat ini conversion is
// post-MVP (v1.0); for now we accept YAML only.
//
// YAML tags drive `gopkg.in/yaml.v3` (un)marshalling.
type Config struct {
	Server    ServerConfig                `yaml:"server"`
	Pool      PoolConfig                  `yaml:"pool"`
	Auth      AuthConfig                  `yaml:"auth"`
	TLS       TLSConfig                   `yaml:"tls,omitempty"`
	Metrics   MetricsConfig               `yaml:"metrics,omitempty"`
	Logging   LoggingConfig               `yaml:"logging,omitempty"`
	Databases map[string]DatabaseConfig   `yaml:"databases"`
	Users     map[string]UserConfig       `yaml:"users,omitempty"`
}

// ServerConfig controls the listening side.
type ServerConfig struct {
	ListenAddr     string        `yaml:"listen_addr"`      // default "0.0.0.0"
	ListenPort     int           `yaml:"listen_port"`      // default 6432
	UnixSocketDir  string        `yaml:"unix_socket_dir,omitempty"`
	UnixSocketMode string        `yaml:"unix_socket_mode,omitempty"` // "0777"

	// ProxyProtocol enables HAProxy PROXY v1/v2 preamble parsing on
	// every accepted TCP conn. Required when pgrouter sits behind an
	// L4 load balancer that prefixes the real client addr.
	ProxyProtocol bool `yaml:"proxy_protocol,omitempty"`

	MaxClientConn  int           `yaml:"max_client_conn"`            // default 1000
	ClientIdle     time.Duration `yaml:"client_idle_timeout"`        // 0 = disabled
	ClientLogin    time.Duration `yaml:"client_login_timeout"`       // default 60s
	IdleTx         time.Duration `yaml:"idle_transaction_timeout"`   // 0 = disabled
}

// PoolConfig controls how the (db, user) pools behave.
type PoolConfig struct {
	Mode              PoolMode      `yaml:"mode"`                // session | transaction | statement
	DefaultPoolSize   int           `yaml:"default_pool_size"`   // default 20
	MinPoolSize       int           `yaml:"min_pool_size"`       // default 0
	ReservePoolSize   int           `yaml:"reserve_pool_size"`   // default 0
	ReservePoolTimeout time.Duration `yaml:"reserve_pool_timeout"` // default 5s
	MaxDBConn         int           `yaml:"max_db_connections"`  // default 0 (unlimited)
	MaxUserConn       int           `yaml:"max_user_connections"`// default 0
	QueryTimeout      time.Duration `yaml:"query_timeout"`       // 0 = no timeout
	QueryWaitTimeout  time.Duration `yaml:"query_wait_timeout"`  // default 120s
	ServerIdle        time.Duration `yaml:"server_idle_timeout"` // default 600s
	ServerLifetime    time.Duration `yaml:"server_lifetime"`     // default 3600s
	ServerConnectTimeout time.Duration `yaml:"server_connect_timeout"` // default 15s
	ServerCheckQuery  string        `yaml:"server_check_query"`  // default ";"
	ServerCheckDelay  time.Duration `yaml:"server_check_delay"`  // default 30s
	ServerResetQuery  string        `yaml:"server_reset_query"`  // default "DISCARD ALL"
}

// PoolMode is the connection-reuse policy.
type PoolMode string

const (
	PoolModeSession     PoolMode = "session"
	PoolModeTransaction PoolMode = "transaction"
	PoolModeStatement   PoolMode = "statement"
)

// AuthConfig controls how clients authenticate.
type AuthConfig struct {
	Type         AuthType `yaml:"type"`                    // default "trust"
	UserlistFile string   `yaml:"userlist_file,omitempty"` // pgbouncer-compat userlist.txt
	AuthQuery    string   `yaml:"auth_query,omitempty"`    // SQL to fetch creds
	AuthUser     string   `yaml:"auth_user,omitempty"`     // user for AuthQuery

	// HBAFile is the path to a pg_hba.conf-style file consulted when
	// type == "hba". Each connecting (db, user, ip) is matched against
	// the rules top-to-bottom; the rule's method is then applied via
	// the standard handlers (trust/md5/scram-sha-256/peer/cert/reject).
	HBAFile string `yaml:"hba_file,omitempty"`
}

// AuthType is the client auth mechanism.
type AuthType string

const (
	AuthTrust AuthType = "trust"
	AuthSCRAM AuthType = "scram-sha-256"
	AuthMD5   AuthType = "md5"
	AuthPeer  AuthType = "peer" // Unix-socket SO_PEERCRED
	AuthHBA   AuthType = "hba"  // post-MVP
	AuthCert  AuthType = "cert" // post-MVP
)

// TLSConfig configures client- and server-facing TLS.
type TLSConfig struct {
	ClientMode     SSLMode `yaml:"client_mode"`            // default "disable"
	ClientCertFile string  `yaml:"client_cert_file,omitempty"`
	ClientKeyFile  string  `yaml:"client_key_file,omitempty"`
	ClientCAFile   string  `yaml:"client_ca_file,omitempty"`
	ClientProtocols []string `yaml:"client_protocols,omitempty"` // e.g. ["TLSv1.2", "TLSv1.3"]

	ServerMode     SSLMode `yaml:"server_mode"` // default "disable"
	ServerCertFile string  `yaml:"server_cert_file,omitempty"`
	ServerKeyFile  string  `yaml:"server_key_file,omitempty"`
	ServerCAFile   string  `yaml:"server_ca_file,omitempty"`
}

// SSLMode mirrors libpq sslmode semantics.
type SSLMode string

const (
	SSLDisable    SSLMode = "disable"
	SSLAllow      SSLMode = "allow"
	SSLPrefer     SSLMode = "prefer"
	SSLRequire    SSLMode = "require"
	SSLVerifyCA   SSLMode = "verify-ca"
	SSLVerifyFull SSLMode = "verify-full"
)

// MetricsConfig exposes the Prometheus endpoint + admin HTTP API.
type MetricsConfig struct {
	Listen string `yaml:"listen"` // host:port (default ":9090")
	Path   string `yaml:"path"`   // default "/metrics"

	// AdminToken gates state-changing /api/v1 POSTs (drain, reload).
	// Empty = open (dev mode); production must set this.
	AdminToken string `yaml:"admin_token,omitempty"`
}

// LoggingConfig configures the slog handler.
type LoggingConfig struct {
	Level  string `yaml:"level"`  // debug | info | warn | error (default info)
	Format string `yaml:"format"` // text | json (default text)

	// LogSQL controls per-query SQL logging:
	//   off      — never log query text (only req_id + tags)
	//   redacted — replace literals with `?` before logging (default)
	//   full     — log the raw SQL (dev only; emits a warning at boot)
	LogSQL string `yaml:"log_sql"`

	// SlowQuery is the duration above which a per-query WARN line is
	// emitted (with redacted SQL). 0 disables.
	SlowQuery time.Duration `yaml:"slow_query"`

	// AuditFile, if non-empty, opens an append-only JSON-lines log
	// that captures every per-query event (ts, req_id, db, user,
	// app, kind, duration_ms, sql). SQL is rendered through LogSQL
	// mode. Intended for compliance / billing pipelines.
	AuditFile string `yaml:"audit_file,omitempty"`
}

// LogSQLMode normalises LoggingConfig.LogSQL into one of the three
// canonical strings. Empty / unrecognised falls back to "redacted".
type LogSQLMode string

const (
	LogSQLOff      LogSQLMode = "off"
	LogSQLRedacted LogSQLMode = "redacted"
	LogSQLFull     LogSQLMode = "full"
)

// NormalizeLogSQL maps a YAML string to a canonical LogSQLMode.
func NormalizeLogSQL(v string) LogSQLMode {
	switch v {
	case "off", "none", "false":
		return LogSQLOff
	case "full", "raw":
		return LogSQLFull
	default:
		return LogSQLRedacted
	}
}

// DatabaseConfig is a per-database upstream definition.
//
// `name` is the YAML map key (the database alias clients see). `dbname`
// is the actual Postgres database on the upstream (may differ for
// renaming).
type DatabaseConfig struct {
	Host             string   `yaml:"host"`     // required
	Port             int      `yaml:"port"`     // default 5432
	DBName           string   `yaml:"dbname"`   // defaults to map key
	User             string   `yaml:"user,omitempty"`             // optional fixed upstream user
	Password         string   `yaml:"password,omitempty"`         // optional fixed password
	PoolMode         PoolMode `yaml:"pool_mode,omitempty"`        // override Pool.Mode
	PoolSize         int      `yaml:"pool_size,omitempty"`        // override Pool.DefaultPoolSize
	ServerResetQuery string   `yaml:"server_reset_query,omitempty"` // override Pool.ServerResetQuery

	// MaxQPS, if > 0, caps the per-tenant Query/Parse rate via a token
	// bucket. Burst = MaxQPS (capacity equals refill-per-second).
	MaxQPS float64 `yaml:"max_qps,omitempty"`
}

// UserConfig is a per-user override.
type UserConfig struct {
	PoolMode PoolMode `yaml:"pool_mode,omitempty"`
	PoolSize int      `yaml:"pool_size,omitempty"`
	MaxConn  int      `yaml:"max_user_connections,omitempty"`
	MaxQPS   float64  `yaml:"max_qps,omitempty"` // per-user QPS cap
}
