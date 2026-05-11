package testutil

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// TCPListener listens on 127.0.0.1:<ephemeral> and registers
// t.Cleanup(ln.Close). Returns the listener and its address string —
// the address is what most callers use (net.Dial passes it back).
//
// Replaces the 3-line `ln, err := net.Listen("tcp", "127.0.0.1:0");
// require.NoError(t, err); defer ln.Close()` pattern that appears
// 9 times across the test tree.
func TCPListener(t *testing.T) (ln net.Listener, addr string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	return ln, ln.Addr().String()
}
