// Package config defines the pgrouter YAML schema, loader, validator,
// defaults, and hot-reload primitives.
//
// MVP scope:
//   - M.3.1: YAML schema + loader
//   - M.3.2: validation + sensible defaults
//   - M.3.3: `pgrouter validate` + `pgrouter run --config` CLI subcommands
//   - M.3.4: sample configs
//
// Post-MVP: PgBouncer .ini compatibility layer, hot reload via SIGHUP.
package config
