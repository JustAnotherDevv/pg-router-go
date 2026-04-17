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

// Validate runs full structural validation. Returns nil if everything
// checks out, otherwise a ValidationErrors aggregating every problem.
func Validate(cfg *Config) error {
	var errs ValidationErrors

	// Server.
	if cfg.Server.ListenPort < 1 || cfg.Server.ListenPort > 65535 {
		errs = errs.add("server.listen_port",
			"must be in [1, 65535], got %d", cfg.Server.ListenPort)
	}
	if cfg.Server.MaxClientConn < 1 {
		errs = errs.add("server.max_client_conn",
			"must be >= 1, got %d", cfg.Server.MaxClientConn)
	}

	// Pool.
	switch cfg.Pool.Mode {
	case PoolModeSession, PoolModeTransaction, PoolModeStatement:
	default:
		errs = errs.add("pool.mode",
			"must be one of session|transaction|statement, got %q", cfg.Pool.Mode)
	}
	if cfg.Pool.DefaultPoolSize < 1 {
		errs = errs.add("pool.default_pool_size",
			"must be >= 1, got %d", cfg.Pool.DefaultPoolSize)
	}
	if cfg.Pool.MinPoolSize < 0 {
		errs = errs.add("pool.min_pool_size",
			"must be >= 0, got %d", cfg.Pool.MinPoolSize)
	}
	if cfg.Pool.MinPoolSize > cfg.Pool.DefaultPoolSize {
		errs = errs.add("pool.min_pool_size",
			"must not exceed default_pool_size (%d > %d)",
			cfg.Pool.MinPoolSize, cfg.Pool.DefaultPoolSize)
	}
	if cfg.Pool.ReservePoolSize < 0 {
		errs = errs.add("pool.reserve_pool_size",
			"must be >= 0, got %d", cfg.Pool.ReservePoolSize)
	}

	// Auth.
	switch cfg.Auth.Type {
	case AuthTrust, AuthSCRAM, AuthMD5:
		// supported in MVP
	case AuthHBA, AuthCert:
		errs = errs.add("auth.type",
			"%q is post-MVP; supported MVP types: trust, scram-sha-256, md5", cfg.Auth.Type)
	default:
		errs = errs.add("auth.type",
			"unknown auth type %q", cfg.Auth.Type)
	}
	if cfg.Auth.Type == AuthSCRAM || cfg.Auth.Type == AuthMD5 {
		if cfg.Auth.UserlistFile == "" && cfg.Auth.AuthQuery == "" {
			errs = errs.add("auth",
				"%s requires either auth.userlist_file or auth.auth_query", cfg.Auth.Type)
		}
	}
	if cfg.Auth.AuthQuery != "" && cfg.Auth.AuthUser == "" {
		errs = errs.add("auth.auth_user",
			"required when auth.auth_query is set")
	}

	// TLS.
	if err := validateSSLMode(cfg.TLS.ClientMode); err != nil {
		errs = errs.add("tls.client_mode", "%s", err)
	}
	if err := validateSSLMode(cfg.TLS.ServerMode); err != nil {
		errs = errs.add("tls.server_mode", "%s", err)
	}
	switch cfg.TLS.ClientMode {
	case SSLRequire, SSLVerifyCA, SSLVerifyFull:
		if cfg.TLS.ClientCertFile == "" || cfg.TLS.ClientKeyFile == "" {
			errs = errs.add("tls",
				"client_mode=%s requires client_cert_file + client_key_file",
				cfg.TLS.ClientMode)
		}
	}
	switch cfg.TLS.ServerMode {
	case SSLVerifyCA, SSLVerifyFull:
		if cfg.TLS.ServerCAFile == "" {
			errs = errs.add("tls.server_ca_file",
				"required when server_mode=%s", cfg.TLS.ServerMode)
		}
	}

	// Databases.
	if len(cfg.Databases) == 0 {
		errs = errs.add("databases",
			"at least one database must be defined")
	}
	for name, db := range cfg.Databases {
		path := fmt.Sprintf("databases.%s", name)
		if db.Host == "" {
			errs = errs.add(path+".host", "required")
		}
		if db.Port < 1 || db.Port > 65535 {
			errs = errs.add(path+".port",
				"must be in [1, 65535], got %d", db.Port)
		}
		if db.PoolMode != "" {
			switch db.PoolMode {
			case PoolModeSession, PoolModeTransaction, PoolModeStatement:
			default:
				errs = errs.add(path+".pool_mode",
					"must be one of session|transaction|statement, got %q", db.PoolMode)
			}
		}
		if db.PoolSize < 0 {
			errs = errs.add(path+".pool_size",
				"must be >= 0, got %d", db.PoolSize)
		}
	}

	// Users.
	for name, u := range cfg.Users {
		path := fmt.Sprintf("users.%s", name)
		if u.PoolMode != "" {
			switch u.PoolMode {
			case PoolModeSession, PoolModeTransaction, PoolModeStatement:
			default:
				errs = errs.add(path+".pool_mode",
					"must be one of session|transaction|statement, got %q", u.PoolMode)
			}
		}
		if u.PoolSize < 0 {
			errs = errs.add(path+".pool_size",
				"must be >= 0, got %d", u.PoolSize)
		}
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}

func validateSSLMode(m SSLMode) error {
	switch m {
	case SSLDisable, SSLAllow, SSLPrefer, SSLRequire, SSLVerifyCA, SSLVerifyFull:
		return nil
	default:
		return fmt.Errorf("unknown sslmode %q (want disable|allow|prefer|require|verify-ca|verify-full)", m)
	}
}
