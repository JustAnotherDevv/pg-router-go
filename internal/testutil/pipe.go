package testutil

import (
	"net"
	"testing"
)

// PipePair returns a synchronous, in-memory net.Conn pair (via net.Pipe)
// and registers t.Cleanup for both ends so neither leaks to GC.
//
// Replaces the common, footgun-prone pattern:
//
//	clt, srv := net.Pipe()
//	defer clt.Close()  // forgot srv — leaks to GC
//
// Callers should NOT add their own defer Close — Cleanup runs in reverse
// registration order at test end. Close is idempotent on net.Pipe ends,
// so an extra explicit Close in a caller is harmless but redundant.
func PipePair(t *testing.T) (clt, srv net.Conn) {
	t.Helper()
	clt, srv = net.Pipe()
	t.Cleanup(func() {
		_ = clt.Close()
		_ = srv.Close()
	})
	return clt, srv
}
