// Package backend owns the upstream-Postgres-facing half of each connection
// path: dialing, the backend startup handshake, the per-backend state
// machine, and (in later milestones) state reset (DISCARD ALL),
// idle eviction, prepared-statement cache, and health-check ping.
//
// MVP scope: see internal/backend/conn.go (M.7).
//
// PoC carryover: `backend.Dial` opens a trust-mode upstream connection
// and returns a `Conn` with PostgresPID + Params populated.
package backend
