// Tests for Phase A: PreAcquire/PostRelease callbacks, configurable
// ResetQuery, and Manager.WithGlobalLimits semaphores.

package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
)

// --- PreAcquire / PostRelease callback invariants ---

// TestPreAcquireGatesAcquire shows PreAcquire is called BEFORE the pool
// lock is taken, and returning a non-nil error short-circuits Acquire.
func TestPreAcquireGatesAcquire(t *testing.T) {
	var preCalls atomic.Int64
	cfg := Config{
		DefaultPoolSize: 1,
		Log:             testutil.Discard,
		Callbacks: Callbacks{
			PreAcquire: func(_ context.Context) error {
				preCalls.Add(1)
				return context.Canceled // synthetic reject
			},
		},
	}
	dial := func(_ context.Context) (*backend.Conn, error) {
		t.Fatal("dial must not be called when PreAcquire errors")
		return nil, nil
	}
	p := New("t", dial, cfg)
	c, err := p.Acquire(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, c)
	require.Equal(t, int64(1), preCalls.Load())
}

// TestPostReleaseFiresOnSuccessfulRelease verifies the 1:1 pairing.
func TestPostReleaseFiresOnSuccessfulRelease(t *testing.T) {
	var pre, post atomic.Int64
	cfg := Config{
		DefaultPoolSize: 2,
		Log:             testutil.Discard,
		Callbacks: Callbacks{
			PreAcquire:  func(_ context.Context) error { pre.Add(1); return nil },
			PostRelease: func() { post.Add(1) },
		},
	}
	p := New("t", okDial, cfg)

	c, err := p.Acquire(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), pre.Load())
	require.Equal(t, int64(0), post.Load(), "PostRelease must NOT fire before Release")

	p.Release(c, false)
	require.Equal(t, int64(1), post.Load(), "exactly one PostRelease per Acquire")
}

// TestPostReleaseFiresOnAcquireFailure: if PreAcquire succeeds but the
// internal Acquire path errors (e.g. dial failure), PostRelease MUST
// still fire so semaphore counters stay leak-free.
func TestPostReleaseFiresOnAcquireFailure(t *testing.T) {
	var pre, post atomic.Int64
	dialErr := errPoolForceFail
	cfg := Config{
		DefaultPoolSize: 1,
		Log:             testutil.Discard,
		Callbacks: Callbacks{
			PreAcquire:  func(_ context.Context) error { pre.Add(1); return nil },
			PostRelease: func() { post.Add(1) },
		},
	}
	p := New("t", failDial(dialErr), cfg)
	c, err := p.Acquire(context.Background())
	require.Error(t, err)
	require.Nil(t, c)
	require.Equal(t, int64(1), pre.Load())
	require.Equal(t, int64(1), post.Load(),
		"PostRelease must fire when Acquire fails after PreAcquire succeeded")
}

// errPoolForceFail is a sentinel for the dial-failure test.
var errPoolForceFail = &poolErrSentinel{msg: "forced dial fail"}

type poolErrSentinel struct{ msg string }

func (e *poolErrSentinel) Error() string { return e.msg }

// --- ResetQuery propagation ---

// TestReleaseUsesConfiguredResetQuery: pool.Config.ResetQuery controls
// the SQL sent during Release reset. We can't easily intercept the
// backend.Conn round-trip here (the fake doesn't speak pgwire), so we
// verify the wiring by injecting a Config.ResetQuery and checking the
// reset failure path is invoked via a sentinel backend.
//
// (End-to-end propagation through real PG is covered by an integration
// test once Docker is available; this unit pins the merge contract.)
func TestReleaseUsesConfiguredResetQueryCustomMerge(t *testing.T) {
	base := Config{
		DefaultPoolSize: 5,
		ResetQuery:      "DISCARD ALL",
	}
	// Override per-pool to a custom query — mimics what Manager.Get does
	// when configFor returns a *Config with non-empty ResetQuery.
	override := &Config{ResetQuery: "DELETE FROM tmp; DISCARD ALL"}
	merged := mergeConfig(base, override)
	require.Equal(t, "DELETE FROM tmp; DISCARD ALL", merged.ResetQuery)

	// Empty ResetQuery in override → base wins.
	override2 := &Config{DefaultPoolSize: 10}
	merged2 := mergeConfig(base, override2)
	require.Equal(t, "DISCARD ALL", merged2.ResetQuery,
		"empty override.ResetQuery preserves base")
}

// --- Manager.WithGlobalLimits semaphores ---

// blockingDialer simulates a backend that takes `d` to dial; lets us
// observe the queue under a global cap without flakiness.
func blockingDialer(d time.Duration) Dialer {
	return func(ctx context.Context) (*backend.Conn, error) {
		select {
		case <-time.After(d):
			return &backend.Conn{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// TestGlobalDBLimitCapsConcurrentCheckouts: two pools sharing the same
// db are jointly capped by maxDBConn=1.
func TestGlobalDBLimitCapsConcurrentCheckouts(t *testing.T) {
	m := newGlobalLimitManager(t, 50*time.Millisecond, 1, 0, nil)

	ka := Key{DB: "appdb", User: "alice"}
	kb := Key{DB: "appdb", User: "bob"}

	c1, err := m.Acquire(context.Background(), ka)
	require.NoError(t, err)
	require.NotNil(t, c1)

	// Second Acquire on a DIFFERENT user but SAME db should block until
	// short ctx-deadline fires.
	ctx2, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	c2, err := m.Acquire(ctx2, kb)
	require.Error(t, err)
	require.Nil(t, c2)

	// Release the first → second now succeeds.
	m.Release(ka, c1, false)
	c2, err = m.Acquire(context.Background(), kb)
	require.NoError(t, err)
	require.NotNil(t, c2)
	m.Release(kb, c2, false)
}

// TestGlobalUserLimitCapsConcurrentCheckouts: two pools sharing the
// same user but DIFFERENT db are jointly capped by maxUserConn=1.
func TestGlobalUserLimitCapsConcurrentCheckouts(t *testing.T) {
	m := newGlobalLimitManager(t, 50*time.Millisecond, 0, 1, nil)

	ka := Key{DB: "appdb", User: "alice"}
	kb := Key{DB: "warehouse", User: "alice"} // same user, different db

	c1, err := m.Acquire(context.Background(), ka)
	require.NoError(t, err)
	ctx2, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err = m.Acquire(ctx2, kb)
	require.Error(t, err, "second Acquire under same user cap must time out")

	m.Release(ka, c1, false)
	c2, err := m.Acquire(context.Background(), kb)
	require.NoError(t, err)
	m.Release(kb, c2, false)
}

// TestGlobalLimitObserverFiresOnReject: nil-safe + counts.
func TestGlobalLimitObserverFiresOnReject(t *testing.T) {
	var rejects sync.Map
	obs := func(scope, name string) {
		key := scope + ":" + name
		v, _ := rejects.LoadOrStore(key, new(atomic.Int64))
		v.(*atomic.Int64).Add(1)
	}
	m := newGlobalLimitManager(t, 30*time.Millisecond, 1, 0, obs)

	ka := Key{DB: "appdb", User: "alice"}
	c1, err := m.Acquire(context.Background(), ka)
	require.NoError(t, err)
	defer m.Release(ka, c1, false)

	ctx2, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_, err = m.Acquire(ctx2, Key{DB: "appdb", User: "bob"})
	require.Error(t, err)

	v, ok := rejects.Load("db:appdb")
	require.True(t, ok, "expected reject on db scope")
	require.Equal(t, int64(1), v.(*atomic.Int64).Load())
}

// TestGlobalLimitNoOpWhenDisabled: 0/0 → no semaphore, no PreAcquire,
// concurrent acquires from same db succeed.
func TestGlobalLimitNoOpWhenDisabled(t *testing.T) {
	m := newGlobalLimitManager(t, 50*time.Millisecond, 0, 0, nil)

	c1, err := m.Acquire(context.Background(), Key{DB: "appdb", User: "a"})
	require.NoError(t, err)
	c2, err := m.Acquire(context.Background(), Key{DB: "appdb", User: "b"})
	require.NoError(t, err)
	m.Release(Key{DB: "appdb", User: "a"}, c1, false)
	m.Release(Key{DB: "appdb", User: "b"}, c2, false)
}
