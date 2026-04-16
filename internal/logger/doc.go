// Package logger wraps slog with pgrouter-specific concerns: structured
// fields, optional slow-query log, optional audit log.
//
// MVP scope (M.13):
//   - slog handler factory (text + json modes)
//   - trace-id middleware
//
// Post-MVP: slow query log, audit log.
package logger
