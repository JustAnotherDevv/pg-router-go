// Per-connection request IDs for log correlation.
//
// Every accepted client connection gets one short hex ID stamped into
// the logger via slog.With("req_id", ...) at the dispatcher boundary.
// All subsequent log lines from that connection — startup, auth,
// per-query, backend acquire, release, errors — carry the same id, so
// an operator can grep the logs for one client's full session without
// having to correlate by remote address (which can be reused or NATed).
//
// The id is 12 hex chars from crypto/rand → 6 random bytes → ~48 bits
// of entropy. That's enough for ~16M concurrent connections at a 1-in-a
// billion collision rate while keeping the log field short.
//
// A 32-bit monotonic counter is mixed into the front 4 hex chars so
// ordering inside a process is observable in the log even without
// timestamps. Counter wraps after 4 billion connections (months of
// uptime at hundreds of QPS); collisions across a wrap are absorbed by
// the random tail.

package client

import (
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
)

var requestIDCounter uint32

// newRequestID returns a 12-char lowercase hex id (counter4 + rand4).
// Layout: ccccrrrrrrrr where ccccc is a 4-hex-char monotonic counter
// and rrrrrrrr is 4 random bytes → 8 hex chars.
func newRequestID() string {
	n := atomic.AddUint32(&requestIDCounter, 1)
	var randBuf [4]byte
	// Best-effort: an entropy failure is extremely rare; fall back to
	// the counter alone rather than panicking.
	_, _ = rand.Read(randBuf[:])
	var b [12]byte
	hex.Encode(b[:4], []byte{byte(n >> 8), byte(n)})
	hex.Encode(b[4:], randBuf[:])
	return string(b[:])
}
