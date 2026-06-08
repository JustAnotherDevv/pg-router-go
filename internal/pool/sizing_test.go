package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pg-router-go/internal/backend"
	"github.com/JustAnotherDevv/pg-router-go/internal/testutil"
)

// --- MinPoolSize / EnsureWarm ---

func TestEnsureWarmSpawnsToFloor(t *testing.T) {
	dialed := atomic.Int64{}
	dial := countingDial(&dialed)
	p := newPool(t, "warm-test", dial, Config{
		DefaultPoolSize: 10,
		MinPoolSize:     3,
	})

	require.Equal(t, 3, p.EnsureWarm(context.Background()))
	require.Equal(t, int64(3), dialed.Load())
	st := p.Stats()
	require.Equal(t, 3, st.Idle)
	require.Equal(t, 0, st.Active)
}

func TestEnsureWarmRespectsExistingBackends(t *testing.T) {
	p := newPool(t, "warm-test", okDial, Config{
		DefaultPoolSize: 10,
		MinPoolSize:     2,
	})
	// Spawn 1 active.
	c, _ := p.Acquire(context.Background())
	require.Equal(t, 1, p.EnsureWarm(context.Background()),
		"only one more needed to reach floor=2")
	p.Release(c, false)
}

func TestEnsureWarmDialFailureStops(t *testing.T) {
	calls := atomic.Int64{}
	dial := func(_ context.Context) (*backend.Conn, error) {
		calls.Add(1)
		return nil, errors.New("nope")
	}
	p := newPool(t, "warm-test", dial, Config{
		DefaultPoolSize: 10,
		MinPoolSize:     5,
	})
	require.Equal(t, 0, p.EnsureWarm(context.Background()))
	require.Equal(t, int64(1), calls.Load(), "dial error stops the warming loop")
}

func TestEvictRespectsMinPoolSize(t *testing.T) {
	p := newPool(t, "floor-test", okDial, Config{
		DefaultPoolSize: 5,
		MinPoolSize:     2,
		ServerIdle:      time.Millisecond,
	})

	// Acquire + release 4 backends â†’ 4 idle.
	conns := make([]*backend.Conn, 4)
	for i := range conns {
		c, _ := p.Acquire(context.Background())
		conns[i] = c
	}
	for _, c := range conns {
		p.Release(c, false)
	}
	require.Equal(t, 4, p.Stats().Idle)

	// Past the idle threshold â†’ eviction kicks in BUT MinPoolSize=2 must be honored.
	n := p.EvictIdleOnce(time.Now().Add(time.Second))
	require.Equal(t, 2, n, "should evict 2, keep 2 to satisfy MinPoolSize")
	require.Equal(t, 2, p.Stats().Idle)
}

func TestEvictLifetimeRecycleBypassesMinPoolSize(t *testing.T) {
	p := newPool(t, "lifetime-test", okDial, Config{
		DefaultPoolSize: 5,
		MinPoolSize:     2,
		ServerLifetime:  time.Millisecond,
	})

	// Acquire 3 distinct backends, THEN release all (otherwise the
	// fast-path idle reuse would only dial one).
	conns := make([]*backend.Conn, 3)
	for i := range conns {
		c, _ := p.Acquire(context.Background())
		conns[i] = c
	}
	for _, c := range conns {
		p.Release(c, false)
	}
	require.Equal(t, 3, p.Stats().Idle)

	// Lifetime recycle is unconditional: all 3 evicted.
	n := p.EvictIdleOnce(time.Now().Add(time.Second))
	require.Equal(t, 3, n)
	require.Equal(t, 0, p.Stats().Idle)
}

// --- ReservePoolSize ---

func TestReservePoolKicksAfterTimeout(t *testing.T) {
	p := newPool(t, "reserve-test", okDial, Config{
		DefaultPoolSize:    1,
		ReservePoolSize:    1,
		ReservePoolTimeout: 30 * time.Millisecond,
		QueryWait:          time.Second,
	})

	// Saturate regular pool.
	c1, err := p.Acquire(context.Background())
	require.NoError(t, err)

	// Second Acquire would block forever without reserve, but reserve
	// kicks in after 30ms.
	start := time.Now()
	c2, err := p.Acquire(context.Background())
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.NotNil(t, c2)
	require.Greater(t, elapsed, 25*time.Millisecond, "should have waited for reserve timeout")
	require.Less(t, elapsed, 200*time.Millisecond)

	st := p.Stats()
	require.Equal(t, 2, st.Active, "regular + reserve both checked out")
	require.Equal(t, uint64(2), st.TotalSpawned)

	p.Release(c1, false)
	p.Release(c2, false)
}

func TestReservePoolCappedAtSize(t *testing.T) {
	p := newPool(t, "reserve-cap", okDial, Config{
		DefaultPoolSize:    1,
		ReservePoolSize:    1,
		ReservePoolTimeout: 20 * time.Millisecond,
		QueryWait:          200 * time.Millisecond,
	})

	// Saturate: regular + reserve.
	c1, _ := p.Acquire(context.Background())
	c2, _ := p.Acquire(context.Background())

	// Third Acquire should time out â€” reserve is also saturated.
	_, err := p.Acquire(context.Background())
	require.ErrorIs(t, err, ErrAcquireTimeout)

	p.Release(c1, false)
	p.Release(c2, false)
}

// --- Drain timeout ---

func TestCloseWithDeadlineWaitsForActive(t *testing.T) {
	p := New("drain-test", okDial, Config{
		DefaultPoolSize: 2,
		Log:             testutil.Discard,
	})

	c, _ := p.Acquire(context.Background())
	require.NotNil(t, c)

	// Release happens after we start CloseWithDeadline.
	go func() {
		time.Sleep(20 * time.Millisecond)
		p.Release(c, false)
	}()

	start := time.Now()
	err := p.CloseWithDeadline(time.Now().Add(time.Second))
	elapsed := time.Since(start)
	require.NoError(t, err, "drained before deadline")
	require.Less(t, elapsed, 100*time.Millisecond, "did not wait full deadline")
}

func TestCloseWithDeadlineTimesOut(t *testing.T) {
	p := New("drain-timeout", okDial, Config{
		DefaultPoolSize: 2,
		Log:             testutil.Discard,
	})
	// Hold an active checkout forever.
	c, _ := p.Acquire(context.Background())
	require.NotNil(t, c)

	err := p.CloseWithDeadline(time.Now().Add(50 * time.Millisecond))
	require.ErrorIs(t, err, ErrDrainTimeout)
}

// --- Callbacks ---

func TestCallbacksFire(t *testing.T) {
	var waitMu sync.Mutex
	var waitCalls []time.Duration
	var dialCalls int
	var evictCalls int

	p := newPool(t, "cb-test", okDial, Config{
		DefaultPoolSize: 2,
		ServerIdle:      time.Millisecond,
		Callbacks: Callbacks{
			OnAcquireWait: func(_ string, d time.Duration) {
				waitMu.Lock()
				waitCalls = append(waitCalls, d)
				waitMu.Unlock()
			},
			OnDial:  func(_ string) { dialCalls++ },
			OnEvict: func(_ string, n int) { evictCalls += n },
		},
	})

	c, _ := p.Acquire(context.Background())
	require.GreaterOrEqual(t, dialCalls, 1)
	require.GreaterOrEqual(t, len(waitCalls), 1)
	p.Release(c, false)
	p.EvictIdleOnce(time.Now().Add(time.Second))
	require.GreaterOrEqual(t, evictCalls, 1)
}

func TestCallbacksOnDialError(t *testing.T) {
	var errors []error
	dial := func(_ context.Context) (*backend.Conn, error) {
		return nil, fakeError("boom")
	}
	p := newPool(t, "cb-err", dial, Config{
		DefaultPoolSize: 1,
		Callbacks: Callbacks{
			OnDialError: func(_ string, e error) { errors = append(errors, e) },
		},
	})
	_, err := p.Acquire(context.Background())
	require.Error(t, err)
	require.Len(t, errors, 1)
}

type fakeError string

func (e fakeError) Error() string { return string(e) }
