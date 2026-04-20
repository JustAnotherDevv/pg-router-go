package config

import "time"

// applyDefaults fills in zero-value fields with sensible defaults so an
// empty `pgrouter.yaml` still produces a working pooler.
//
// Anything that's already set by the user is left untouched. The rules
// match PgBouncer's defaults where reasonable (Go zero values diverge:
// 0 means "unset" rather than "0 timeout" for the durations we care about).
func applyDefaults(cfg *Config) {
	// Server.
	if cfg.Server.ListenAddr == "" {
		cfg.Server.ListenAddr = "0.0.0.0"
	}
	if cfg.Server.ListenPort == 0 {
		cfg.Server.ListenPort = 6432
	}
	if cfg.Server.MaxClientConn == 0 {
		cfg.Server.MaxClientConn = 1000
	}
	if cfg.Server.ClientLogin == 0 {
		cfg.Server.ClientLogin = 60 * time.Second
	}

	// Pool.
	if cfg.Pool.Mode == "" {
		cfg.Pool.Mode = PoolModeTransaction // MVP default
	}
	if cfg.Pool.DefaultPoolSize == 0 {
		cfg.Pool.DefaultPoolSize = 20
	}
	if cfg.Pool.ReservePoolTimeout == 0 {
		cfg.Pool.ReservePoolTimeout = 5 * time.Second
	}
	if cfg.Pool.QueryWaitTimeout == 0 {
		cfg.Pool.QueryWaitTimeout = 120 * time.Second
	}
	if cfg.Pool.ServerIdle == 0 {
		cfg.Pool.ServerIdle = 600 * time.Second
	}
	if cfg.Pool.ServerLifetime == 0 {
		cfg.Pool.ServerLifetime = 3600 * time.Second
	}
	if cfg.Pool.ServerConnectTimeout == 0 {
		cfg.Pool.ServerConnectTimeout = 15 * time.Second
	}
	if cfg.Pool.ServerCheckQuery == "" {
		cfg.Pool.ServerCheckQuery = ";"
	}
	if cfg.Pool.ServerCheckDelay == 0 {
		cfg.Pool.ServerCheckDelay = 30 * time.Second
	}
	if cfg.Pool.ServerResetQuery == "" {
		cfg.Pool.ServerResetQuery = "DISCARD ALL"
	}

	// Auth.
	if cfg.Auth.Type == "" {
		cfg.Auth.Type = AuthTrust
	}

	// TLS.
	if cfg.TLS.ClientMode == "" {
		cfg.TLS.ClientMode = SSLDisable
	}
	if cfg.TLS.ServerMode == "" {
		cfg.TLS.ServerMode = SSLDisable
	}

	// Metrics.
	if cfg.Metrics.Listen == "" {
		cfg.Metrics.Listen = ":9090"
	}
	if cfg.Metrics.Path == "" {
		cfg.Metrics.Path = "/metrics"
	}

	// Logging.
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}
	if cfg.Logging.Format == "" {
		cfg.Logging.Format = "text"
	}
	if cfg.Logging.LogSQL == "" {
		cfg.Logging.LogSQL = string(LogSQLRedacted)
	}

	// Per-database fills.
	for name, db := range cfg.Databases {
		if db.Port == 0 {
			db.Port = 5432
		}
		if db.DBName == "" {
			db.DBName = name
		}
		cfg.Databases[name] = db
	}
}
