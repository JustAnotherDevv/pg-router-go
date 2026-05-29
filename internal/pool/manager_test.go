package pool

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
)

// keyedMockDialer wraps a per-key counter so we can verify per-pool isolation.
type keyedMockDialer struct {
	calls atomic.Int64
}

func (m *keyedMockDialer) For(_ Key) Dialer {
	return func(ctx context.Context) (*backend.Conn, error) {
		m.calls.Add(1)
		return &backend.Conn{}, nil
	}
}

func newTestManager(t *testing.T) (*Manager, *keyedMockDialer) {
	t.Helper()
	km := &keyedMockDialer{}
	m := NewManager(Config{
		DefaultPoolSize: 3,
		QueryWait:       time.Second,
		Log:             slog.New(slog.DiscardHandler),
	}, km.For)
	t.Cleanup(m.Close)
	return m, km
}

func TestManagerLazyCreatesPool(t *testing.T) {
	m, _ := newTestManager(t)
	require.Len(t, m.Pools(), 0)

	p := m.Get(Key{DB: "appdb", User: "alice"})
	require.NotNil(t, p)
	require.Len(t, m.Pools(), 1)

	p2 := m.Get(Key{DB: "appdb", User: "alice"})
	require.Same(t, p, p2, "same key returns same pool")
}

func TestManagerSeparatesByKey(t *testing.T) {
	m, _ := newTestManager(t)
	p1 := m.Get(Key{DB: "appdb", User: "alice"})
	p2 := m.Get(Key{DB: "appdb", User: "bob"})
	p3 := m.Get(Key{DB: "warehouse", User: "alice"})
	require.NotSame(t, p1, p2)
	require.NotSame(t, p1, p3)
	require.NotSame(t, p2, p3)
	require.Len(t, m.Pools(), 3)
}

func TestManagerAcquireRelease(t *testing.T) {
	m, km := newTestManager(t)
	k := Key{DB: "appdb", User: "alice"}
	c, err := m.Acquire(context.Background(), k)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, int64(1), km.calls.Load())

	m.Release(k, c, false)
	require.Equal(t, 1, m.Get(k).Stats().Idle)
}

func TestManagerAllStats(t *testing.T) {
	m, _ := newTestManager(t)
	_, _ = m.Acquire(context.Background(), Key{DB: "a", User: "u1"})
	_, _ = m.Acquire(context.Background(), Key{DB: "a", User: "u2"})

	stats := m.AllStats()
	require.Len(t, stats, 2)
	for _, s := range stats {
		require.Equal(t, 1, s.Active)
	}
}

func TestManagerJanitorEvicts(t *testing.T) {
	km := &keyedMockDialer{}
	m := NewManager(Config{
		DefaultPoolSize: 2,
		ServerIdle:      10 * time.Millisecond,
		Log:             slog.New(slog.DiscardHandler),
	}, km.For)
	defer m.Close()

	k := Key{DB: "a", User: "u"}
	c, _ := m.Acquire(context.Background(), k)
	m.Release(k, c, false)
	require.Equal(t, 1, m.Get(k).Stats().Idle)

	m.Start(15 * time.Millisecond)
	require.Eventually(t, func() bool {
		return m.Get(k).Stats().Idle == 0
	}, time.Second, 5*time.Millisecond)
}

func TestManagerCloseStopsJanitor(t *testing.T) {
	km := &keyedMockDialer{}
	m := NewManager(Config{
		DefaultPoolSize: 1,
		ServerIdle:      time.Hour,
		Log:             slog.New(slog.DiscardHandler),
	}, km.For)
	m.Start(time.Millisecond)
	// Just verify Close doesn't deadlock.
	m.Close()
}
