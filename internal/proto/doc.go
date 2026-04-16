// Package proto is the wire-protocol layer: a thin wrapper around
// github.com/jackc/pgx/v5/pgproto3 that gives higher-level packages an
// API they can call without knowing about pgproto3 internals.
//
// MVP scope (M.2):
//   - message type enum
//   - Reader / Writer types backed by pgproto3.Backend / pgproto3.Frontend
//   - typed decoders + encoders per message kind
//   - buffer pool via sync.Pool
//   - Forward / ForwardUntil helpers for the proxy loop
//   - fuzz tests against malformed input
package proto
