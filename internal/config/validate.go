package config

import (
	"fmt"
	"strings"
)

// ValidationError records a single problem in a config file. Path is a
// dot-separated locator, e.g. "databases.appdb.host".
type ValidationError struct {
	Path    string
	Message string
}

func (e ValidationError) Error() string {
	if e.Path == "" {
		return e.Message
	}
	return fmt.Sprintf("[%s]: %s", e.Path, e.Message)
}

// ValidationErrors aggregates many ValidationError entries into one error.
type ValidationErrors []ValidationError

func (es ValidationErrors) Error() string {
	if len(es) == 0 {
		return "no errors"
	}
	if len(es) == 1 {
		return es[0].Error()
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d config errors:\n", len(es))
	for _, e := range es {
		fmt.Fprintf(&b, "  - %s\n", e.Error())
	}
	return strings.TrimRight(b.String(), "\n")
}

// add appends one error to the slice and returns the new slice.
func (es ValidationErrors) add(path, format string, args ...any) ValidationErrors {
	return append(es, ValidationError{Path: path, Message: fmt.Sprintf(format, args...)})
}

// validPoolMode is the canonical set of accepted PoolMode strings.
var validPoolMode = map[PoolMode]struct{}{
	PoolModeSession: {}, PoolModeTransaction: {}, PoolModeStatement: {},
}

// Validate runs full structural validation. Returns nil if everything
// checks out, otherwise a ValidationErrors aggregating every problem.
//
// WIN5 refactor: the per-field "if x < 1 { add } / if x < 0 { add }"
// patterns repeated 15+ times. They now go through e.port, e.minInt,
// e.poolMode helpers. Each numeric guard is one line at the call site.
func Validate(cfg *Config) error {
	e := &validator{}

	// Server.
	e.port("server.listen_port", cfg.Server.ListenPort)
	e.minInt("server.max_client_conn", cfg.Server.MaxClientConn, 1)

	// Pool.
	e.poolMode("pool.mode", cfg.Pool.Mode, true)
	e.minInt("pool.default_pool_size", cfg.Pool.DefaultPoolSize, 1)
	e.minInt("pool.min_pool_size", cfg.Pool.MinPoolSize, 0)
	e.minInt("pool.reserve_pool_size", cfg.Pool.ReservePoolSize, 0)
	if cfg.Pool.MinPoolSize > cfg.Pool.DefaultPoolSize {
		e.add("pool.min_pool_size",
			"must not exceed default_pool_size (%d > %d)",
			cfg.Pool.MinPoolSize, cfg.Pool.DefaultPoolSize)
	}

	// Auth.
	switch cfg.Auth.Type {
	case AuthTrust, AuthSCRAM, AuthMD5, AuthPeer, AuthCert, AuthHBA:
	default:
		e.add("auth.type", "unknown auth type %q", cfg.Auth.Type)
	}
	if cfg.Auth.Type == AuthHBA && cfg.Auth.HBAFile == "" {
		e.add("auth.hba_file", "required when auth.type=hba")
	}
	if cfg.Auth.Type == AuthCert {
		switch cfg.TLS.ClientMode {
		case SSLVerifyCA, SSLVerifyFull:
		default:
			e.add("auth.type",
				"cert auth requires tls.client_mode=verify-ca or verify-full (got %q)",
				cfg.TLS.ClientMode)
		}
	}
	if cfg.Auth.Type == AuthSCRAM || cfg.Auth.Type == AuthMD5 {
		if cfg.Auth.UserlistFile == "" && cfg.Auth.AuthQuery == "" {
			e.add("auth", "%s requires either auth.userlist_file or auth.auth_query", cfg.Auth.Type)
		}
	}
	if cfg.Auth.Type == AuthPeer && cfg.Server.UnixSocketDir == "" {
		e.add("auth.type", "peer auth requires server.unix_socket_dir to be set")
	}
	if cfg.Auth.AuthQuery != "" && cfg.Auth.AuthUser == "" {
		e.add("auth.auth_user", "required when auth.auth_query is set")
	}

	// TLS.
	if err := validateSSLMode(cfg.TLS.ClientMode); err != nil {
		e.add("tls.client_mode", "%s", err)
	}
	if err := validateSSLMode(cfg.TLS.ServerMode); err != nil {
		e.add("tls.server_mode", "%s", err)
	}
	switch cfg.TLS.ClientMode {
	case SSLRequire, SSLVerifyCA, SSLVerifyFull:
		if cfg.TLS.ClientCertFile == "" || cfg.TLS.ClientKeyFile == "" {
			e.add("tls", "client_mode=%s requires client_cert_file + client_key_file",
				cfg.TLS.ClientMode)
		}
	}
	switch cfg.TLS.ServerMode {
	case SSLVerifyCA, SSLVerifyFull:
		if cfg.TLS.ServerCAFile == "" {
			e.add("tls.server_ca_file", "required when server_mode=%s", cfg.TLS.ServerMode)
		}
	}

	// Databases.
	if len(cfg.Databases) == 0 {
		e.add("databases", "at least one database must be defined")
	}
	for name, db := range cfg.Databases {
		path := "databases." + name
		e.required(path+".host", db.Host)
		e.port(path+".port", db.Port)
		e.poolMode(path+".pool_mode", db.PoolMode, false)
		e.minInt(path+".pool_size", db.PoolSize, 0)
		e.minInt64(path+".max_replica_lag_bytes", db.MaxReplicaLagBytes, 0)
		for i, r := range db.Replicas {
			rp := fmt.Sprintf("%s.replicas[%d]", path, i)
			e.required(rp+".host", r.Host)
			e.port(rp+".port", r.Port)
			e.minInt(rp+".weight", r.Weight, 0)
		}
	}

	// Users.
	for name, u := range cfg.Users {
		path := "users." + name
		e.poolMode(path+".pool_mode", u.PoolMode, false)
		e.minInt(path+".pool_size", u.PoolSize, 0)
	}

	if len(e.errs) > 0 {
		return e.errs
	}
	return nil
}

// validator collects errors and exposes typed-guard helpers so each
// field-level check is one line at the call site.
type validator struct{ errs ValidationErrors }

func (e *validator) add(path, format string, args ...any) {
	e.errs = e.errs.add(path, format, args...)
}

func (e *validator) port(path string, v int) {
	if v < 1 || v > 65535 {
		e.add(path, "must be in [1, 65535], got %d", v)
	}
}

func (e *validator) minInt(path string, v, min int) {
	if v < min {
		e.add(path, "must be >= %d, got %d", min, v)
	}
}

func (e *validator) minInt64(path string, v, min int64) {
	if v < min {
		e.add(path, "must be >= %d, got %d", min, v)
	}
}

func (e *validator) required(path, v string) {
	if v == "" {
		e.add(path, "required")
	}
}

// poolMode validates against {session|transaction|statement}. When
// required=false the empty value is accepted (per-db/user overrides).
func (e *validator) poolMode(path string, m PoolMode, required bool) {
	if m == "" {
		if required {
			e.add(path, "must be one of session|transaction|statement, got %q", m)
		}
		return
	}
	if _, ok := validPoolMode[m]; !ok {
		e.add(path, "must be one of session|transaction|statement, got %q", m)
	}
}

func validateSSLMode(m SSLMode) error {
	switch m {
	case SSLDisable, SSLAllow, SSLPrefer, SSLRequire, SSLVerifyCA, SSLVerifyFull:
		return nil
	default:
		return fmt.Errorf("unknown sslmode %q (want disable|allow|prefer|require|verify-ca|verify-full)", m)
	}
}
