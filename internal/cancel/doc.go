// Package cancel implements PostgreSQL CancelRequest routing.
//
// MVP scope (M.12):
//   - Generate our own (PID, secret) per client — never leak backend's
//   - Track (PID, secret) -> backend conn mapping
//   - On CancelRequest, dial the right backend's cancel side-channel
package cancel
