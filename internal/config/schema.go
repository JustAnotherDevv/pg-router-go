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
	Wire      WireConfig                  `yaml:"wire,omitempty"`
	Databases map[string]DatabaseConfig   `yaml:"databases"`
	Users     map[string]UserConfig       `yaml:"users,omitempty"`
}

// WireConfig tunes the low-level wire forwarding path. Phase A's
// splice forwarder lives here; future hot-path knobs (e.g. buffered
// backend writer from Phase B) will be added alongside.
type WireConfig struct {
	// Splice enables the splice forwarder for backend→client drain.
	// Default true. Set false to fall back to the original
	// pgproto3-decode hot path (useful for bisecting regressions).
	Splice *bool `yaml:"splice,omitempty"`

	// SpliceBufferSize is the size of the pooled splice working
	// buffer, in bytes. Single boring messages with body > (size - 5)
	// fall back to a two-write path; everything else is a single
	// write. Must be >= 5. Default 8192 (8 KiB) — matches typical
	// kernel socket buffer + handles any single Postgres message.
	SpliceBufferSize int `yaml:"splice_buffer_size,omitempty"`

	// SpliceDropUnknown, when true, drops backend messages whose tag
	// byte isn't in the known tag table instead of splicing them
	// forward. Default false (forward as boring). Use only if you're
	// debugging a protocol mismatch and don't want unknown tags
	// reaching the client.
	SpliceDropUnknown bool `yaml:"splice_drop_unknown,omitempty"`

	// PreparedCache enables the cross-backend prepared-statement
	// cache (per-client name→server-name rewrite + per-backend LRU
	// of pgr_<hash> statements). Default true. Set false to disable
	// the cache entirely — Parse/Bind/Close pass through with the
	// client's original statement names, the per-client PrepareCache
	// is never built, and the per-backend cache is left nil.
	//
	// Disable when the workload is dominated by unnamed extended
	// statements, simple Query, or one-shot Parse/Bind pairs where
	// the cache-hit rate is low and the per-Parse hash + map lookup
	// overhead shows up as a net regression.
	PreparedCache *bool `yaml:"prepared_cache,omitempty"`

	// RawPassthrough bypasses pgproto3 for client→backend message
	// reading. Raw bytes are read from the client socket and forwarded
	// directly to the backend — no struct allocation, no decode/re-encode.
	// Only Query/Parse have their SQL extracted for GUC/pin/classification.
	// Backend→client splice continues to work independently.
	//
	// Trade-off: prepared-cache interception is disabled in raw mode.
	// Default false. Set true for maximum throughput on simple workloads.
	RawPassthrough *bool `yaml:"raw_passthrough,omitempty"`

	// SkipResetQuery, when true, skips the DISCARD ALL (or custom
	// server_reset_query) on backend release. Saves 1 write + 2 read
	// syscalls + 4 heap allocs per transaction. Safe when clients
	// don't use session-level state (SET, LISTEN, temp tables, etc.)
	// and the workload is purely transactional. Default false.
	SkipResetQuery *bool `yaml:"skip_reset_query,omitempty"`


}

// ServerConfig controls the listening side.
type ServerConfig struct {
	ListenAddr     string        `yaml:"listen_addr"`      // default "0.0.0.0"
	ListenPort     int           `yaml:"listen_port"`      // default 6432
	UnixSocketDir  string        `yaml:"unix_socket_dir,omitempty"`
	UnixSocketMode string        `yaml:"unix_socket_mode,omitempty"` // "0777"

	// SingleThread pins Go to a single OS thread (runtime.GOMAXPROCS=1)
	// for low-latency proxy workloads. Eliminates goroutine scheduling
	// overhead and CPU cache thrashing from thread migration. Safe for
	// connection poolers where the hot path is I/O-bound (Read/Write
	// syscalls yield the goroutine). Disable if the workload includes
	// CPU-heavy query processing (e.g. complex SQL parsing, TLS-heavy
	// auth). Default false.
	SingleThread *bool `yaml:"single_thread,omitempty"`

	// GOGC overrides the Go garbage collector's GOGC value at startup.
	// GOGC controls heap growth ratio before GC triggers: GOGC=100
	// (default) means GC runs when heap doubles. Higher values reduce
	// GC frequency at the cost of more memory. For poolers with low
	// live heap (~1-2MB), GOGC=200 or higher is safe and reduces GC
	// CPU overhead significantly. Set to "off" to disable GC entirely
	// (dangerous for long-running processes with unbounded alloc).
	// Default: Go runtime default (100).
	GOGC string `yaml:"gogc,omitempty"`

	// SocketRecvBuf overrides SO_RCVBUF on accepted client connections.
	// Larger buffers reduce read syscall frequency under high throughput.
	// Default 0 = kernel default (typically 128-256KB).
	SocketRecvBuf int `yaml:"socket_recv_buf,omitempty"`

	// SocketSendBuf overrides SO_SNDBUF on accepted client connections.
	// Larger buffers reduce write syscall frequency under high throughput.
	// Default 0 = kernel default (typically 128-256KB).
	SocketSendBuf int `yaml:"socket_send_buf,omitempty"`

	// ProxyProtocol enables HAProxy PROXY v1/v2 preamble parsing on
	// every accepted TCP conn. Required when pgrouter sits behind an
	// L4 load balancer that prefixes the real client addr.
	ProxyProtocol bool `yaml:"proxy_protocol,omitempty"`

	// ProxyProtocolStrict, when true alongside ProxyProtocol, rejects
	// accepted connections that don't present a PROXY preamble. Use in
	// production once the LB is verified speaking PROXY. Default false
	// keeps existing rollout behavior (bare conns accepted) so a LB
	// misconfig doesn't drop traffic during the rollout window.
	// Connections rejected this way are counted in
	// pgrouter_proxy_proto_missing_total.
	ProxyProtocolStrict bool `yaml:"proxy_protocol_strict,omitempty"`

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

	// SkipPreflight disables the boot-time dial against each configured
	// database. Default false (preflight runs; all-fail aborts boot)
	// catches typos before clients see latency. Set true for staged
	// rollouts where backends warm later than the pooler.
	SkipPreflight bool `yaml:"skip_preflight,omitempty"`
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

	// Replicas is the list of read replicas backing this database.
	// pgrouter classifies SQL into read vs write (see internal/client
	// classifier), routes reads to the least-laggy replica, writes to
	// the primary. Empty list = primary-only (back-compat).
	Replicas []ReplicaConfig `yaml:"replicas,omitempty"`

	// MaxReplicaLagBytes is the cap on replication lag (WAL bytes
	// behind primary) above which a replica is excluded from the
	// read-routing pool. 0 = unbounded (don't skip).
	MaxReplicaLagBytes int64 `yaml:"max_replica_lag_bytes,omitempty"`

	// StickyReadWindow is how long after a write on this database a
	// follow-up SELECT from the same client is pinned to the primary
	// (read-your-own-writes). 0 = disabled.
	StickyReadWindow time.Duration `yaml:"sticky_read_window,omitempty"`
}

// ReplicaConfig is one read replica entry under databases.<name>.replicas.
type ReplicaConfig struct {
	Host   string `yaml:"host"`
	Port   int    `yaml:"port"`             // default 5432
	Weight int    `yaml:"weight,omitempty"` // default 1; relative read-routing weight
}

// UserConfig is a per-user override.
type UserConfig struct {
	PoolMode PoolMode `yaml:"pool_mode,omitempty"`
	PoolSize int      `yaml:"pool_size,omitempty"`
	MaxConn  int      `yaml:"max_user_connections,omitempty"`
	MaxQPS   float64  `yaml:"max_qps,omitempty"` // per-user QPS cap
}
