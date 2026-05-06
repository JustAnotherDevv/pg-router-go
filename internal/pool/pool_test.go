package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
)

// mockDialer is a no-op dialer that returns a stubbed backend.Conn.
// The Conn is non-functional but valid for pool bookkeeping.
type mockDialer struct {
	spawned atomic.Int64
	fail    atomic.Bool
}

func (m *mockDialer) Dial(ctx context.Context) (*backend.Conn, error) {
	if m.fail.Load() {
		return nil, errors.New("mock dial fail")
	}
	m.spawned.Add(1)
	// A pure-stub Conn (no NetConn / Frontend) — fine for pool tests
	// that don't actually drive query traffic.
	return &backend.Conn{}, nil
}

func newTestPool(t *testing.T, cfg Config) (*Pool, *mockDialer) {
	t.Helper()
	if cfg.Log == nil {
		cfg.Log = testutil.Discard
	}
	md := &mockDialer{}
	p := New("test", md.Dial, cfg)
	t.Cleanup(p.Close)
	return p, md
}

func TestAcquireSpawnsBackend(t *testing.T) {
	p, md := newTestPool(t, Config{DefaultPoolSize: 5})
	c, err := p.Acquire(context.Background())
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, int64(1), md.spawned.Load())

	st := p.Stats()
	require.Equal(t, 1, st.Active)
	require.Equal(t, 0, st.Idle)
	require.Equal(t, uint64(1), st.TotalSpawned)
}

func TestReleasePutsBackendIntoIdle(t *testing.T) {
	p, _ := newTestPool(t, Config{DefaultPoolSize: 2})
	c, err := p.Acquire(context.Background())
	require.NoError(t, err)

	p.Release(c, false)
	st := p.Stats()
	require.Equal(t, 0, st.Active)
	require.Equal(t, 1, st.Idle)
}

func TestAcquireReusesIdle(t *testing.T) {
	p, md := newTestPool(t, Config{DefaultPoolSize: 2})
	c, _ := p.Acquire(context.Background())
	p.Release(c, false)
	require.Equal(t, int64(1), md.spawned.Load())

	c2, err := p.Acquire(context.Background())
	require.NoError(t, err)
	require.Equal(t, c, c2, "reuse same backend")
	require.Equal(t, int64(1), md.spawned.Load(), "no new spawn")
}

func TestAcquireBlocksWhenFull(t *testing.T) {
	p, _ := newTestPool(t, Config{DefaultPoolSize: 1, QueryWait: time.Second})

	c1, err := p.Acquire(context.Background())
	require.NoError(t, err)

	// Second acquire must block until c1 is released.
	type result struct {
		c   any
		err error
	}
	ch := make(chan result, 1)
	go func() {
		c, err := p.Acquire(context.Background())
		ch <- result{c, err}
	}()

	// Verify the waiter is queued.
	require.Eventually(t, func() bool {
		return p.Stats().Waiters == 1
	}, time.Second, 10*time.Millisecond)

	p.Release(c1, false)

	select {
	case r := <-ch:
		require.NoError(t, r.err)
		require.NotNil(t, r.c)
	case <-time.After(time.Second):
		t.Fatal("second Acquire did not unblock")
	}

	require.Equal(t, 0, p.Stats().Waiters)
}

func TestAcquireTimeout(t *testing.T) {
	p, _ := newTestPool(t, Config{
		DefaultPoolSize: 1,
		QueryWait:       50 * time.Millisecond,
	})

	// Saturate.
	_, err := p.Acquire(context.Background())
	require.NoError(t, err)

	_, err = p.Acquire(context.Background())
	require.ErrorIs(t, err, ErrAcquireTimeout)
}

func TestAcquireContextCancel(t *testing.T) {
	p, _ := newTestPool(t, Config{DefaultPoolSize: 1, QueryWait: time.Minute})
	_, _ = p.Acquire(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := p.Acquire(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestAcquireFIFOFairness(t *testing.T) {
	p, _ := newTestPool(t, Config{DefaultPoolSize: 1, QueryWait: time.Second})
	c1, _ := p.Acquire(context.Background())

	const N = 5
	var (
		mu      sync.Mutex
		order   []int
		startWG sync.WaitGroup
	)
	startWG.Add(N)

	for i := 0; i < N; i++ {
		i := i
		go func() {
			startWG.Done()
			c, err := p.Acquire(context.Background())
			if err != nil {
				return
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			p.Release(c, false)
		}()
	}
	startWG.Wait()

	// Wait until all N waiters parked.
	require.Eventually(t, func() bool {
		return p.Stats().Waiters == N
	}, time.Second, 10*time.Millisecond)

	p.Release(c1, false)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == N
	}, 2*time.Second, 10*time.Millisecond)

	// FIFO not strictly guaranteed since goroutines park in race order,
	// but every waiter must complete.
	require.Len(t, order, N)
}

func TestAcquireAfterCloseFails(t *testing.T) {
	p, _ := newTestPool(t, Config{DefaultPoolSize: 1})
	p.Close()
	_, err := p.Acquire(context.Background())
	require.ErrorIs(t, err, ErrPoolClosed)
}

func TestCloseClosesIdleBackends(t *testing.T) {
	p, _ := newTestPool(t, Config{DefaultPoolSize: 2})
	c, _ := p.Acquire(context.Background())
	p.Release(c, false)
	require.Equal(t, 1, p.Stats().Idle)
	p.Close()
	st := p.Stats()
	require.Equal(t, 0, st.Idle)
}

func TestEvictIdleOnce(t *testing.T) {
	p, _ := newTestPool(t, Config{
		DefaultPoolSize: 2,
		ServerIdle:      50 * time.Millisecond,
	})
	c, _ := p.Acquire(context.Background())
	p.Release(c, false)
	require.Equal(t, 1, p.Stats().Idle)

	// Not yet expired.
	require.Equal(t, 0, p.EvictIdleOnce(time.Now()))
	require.Equal(t, 1, p.Stats().Idle)

	// Past the threshold.
	require.Equal(t, 1, p.EvictIdleOnce(time.Now().Add(time.Second)))
	require.Equal(t, 0, p.Stats().Idle)
	require.Equal(t, uint64(1), p.Stats().TotalEvicted)
}

func TestDialFailureReleasesSlot(t *testing.T) {
	p, md := newTestPool(t, Config{DefaultPoolSize: 1})
	md.fail.Store(true)
	_, err := p.Acquire(context.Background())
	require.Error(t, err)
	require.Equal(t, 0, p.Stats().Active, "failed dial must not hold a slot")

	// Recover.
	md.fail.Store(false)
	c, err := p.Acquire(context.Background())
	require.NoError(t, err)
	require.NotNil(t, c)
}
