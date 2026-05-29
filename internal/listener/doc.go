// Package listener accepts TCP (and later Unix-domain socket) client
// connections and dispatches each to a handler goroutine.
//
// MVP scope:
//   - TCP listener (PoC carryover)
//   - Unix-domain socket listener
//   - TLS upgrade handler (M.4)
//   - Optional HAProxy PROXY protocol header (v1.0)
package listener
