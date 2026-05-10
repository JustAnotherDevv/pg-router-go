package pool

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
)

// Test-only dialer constructors. Keep both the closure form
// (`okDial`) and the counter form (`countingDial`) so existing tests
// can collapse their inline `dial := func(_ context.Context) ...`
// boilerplate.

// okDial returns a Dialer that always succeeds with a fresh empty
// backend.Conn. Use when the test doesn't care about which conn is
// returned.
func okDial(_ context.Context) (*backend.Conn, error) {
	return &backend.Conn{}, nil
}

// failDial returns a Dialer that always fails with the given error.
func failDial(err error) Dialer {
	return func(_ context.Context) (*backend.Conn, error) { return nil, err }
}

// countingDial returns a Dialer that succeeds with a fresh empty
// backend.Conn while atomically incrementing the supplied counter on
// every call.
func countingDial(n *atomic.Int64) Dialer {
	return func(_ context.Context) (*backend.Conn, error) {
		n.Add(1)
		return &backend.Conn{}, nil
	}
}

// newPool wraps New: stamps Log=testutil.Discard onto the caller's
// Config (caller leaves Log unset) and registers t.Cleanup(p.Close).
// Saves the `Log: testutil.Discard,` Config field + `defer p.Close()`
// line on every pool-test setup.
func newPool(t *testing.T, name string, dial Dialer, cfg Config) *Pool {
	t.Helper()
	cfg.Log = testutil.Discard
	p := New(name, dial, cfg)
	t.Cleanup(p.Close)
	return p
}
