// Package client owns the client-facing half of each connection path:
// startup parsing, the client-side state machine, transaction-boundary
// detection, per-client GUC tracking, and per-client prepared statement
// tracking.
//
// MVP scope:
//   - M.6: ClientConn lifecycle + state machine
//   - M.10: per-client GUC map (varcache equivalent)
//   - M.11: per-client prepared statement tracking
//
// PoC carryover: see conn.go for the M.1 forwarder loop.
package client
