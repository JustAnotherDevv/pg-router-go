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

// TestManagerPerPoolConfigOverride: ConfigFor lets one pool get bigger
// limits than the default.
func TestManagerPerPoolConfigOverride(t *testing.T) {
	dial := func(_ context.Context) (*backend.Conn, error) {
		return &backend.Conn{}, nil
	}
	m := NewManager(Config{
		DefaultPoolSize: 5,
		Log:             testutil.Discard,
	}, func(_ Key) Dialer { return dial }).WithConfigFor(func(k Key) *Config {
		if k.DB == "big" {
			return &Config{DefaultPoolSize: 50, MinPoolSize: 5}
		}
		return nil
	})
	defer m.Close()

	pSmall := m.Get(Key{DB: "small", User: "u"})
	pBig := m.Get(Key{DB: "big", User: "u"})

	require.Equal(t, 5, pSmall.cfg.DefaultPoolSize)
	require.Equal(t, 50, pBig.cfg.DefaultPoolSize)
	require.Equal(t, 5, pBig.cfg.MinPoolSize)
}

func TestManagerCloseWithDeadlineDrainsAllPools(t *testing.T) {
	dial := func(_ context.Context) (*backend.Conn, error) {
		return &backend.Conn{}, nil
	}
	m := NewManager(Config{
		DefaultPoolSize: 1,
		Log:             testutil.Discard,
	}, func(_ Key) Dialer { return dial })

	// Spawn 3 pools, each with one active checkout.
	conns := make(map[Key]*backend.Conn, 3)
	for _, k := range []Key{
		{DB: "a", User: "u"}, {DB: "b", User: "u"}, {DB: "c", User: "u"},
	} {
		c, err := m.Acquire(context.Background(), k)
		require.NoError(t, err)
		conns[k] = c
	}

	// Release after a short delay; drain should complete within deadline.
	go func() {
		time.Sleep(20 * time.Millisecond)
		for k, c := range conns {
			m.Release(k, c, false)
		}
	}()

	start := time.Now()
	err := m.CloseWithDeadline(time.Now().Add(time.Second))
	require.NoError(t, err)
	require.Less(t, time.Since(start), 200*time.Millisecond,
		"all 3 pools should drain in parallel")
}

func TestManagerCloseWithDeadlineTimesOut(t *testing.T) {
	dial := func(_ context.Context) (*backend.Conn, error) {
		return &backend.Conn{}, nil
	}
	m := NewManager(Config{
		DefaultPoolSize: 1,
		Log:             testutil.Discard,
	}, func(_ Key) Dialer { return dial })

	_, _ = m.Acquire(context.Background(), Key{DB: "stuck", User: "u"})

	err := m.CloseWithDeadline(time.Now().Add(50 * time.Millisecond))
	require.ErrorIs(t, err, ErrDrainTimeout)
}

// TestStrictFIFOSerialWaiters: park N waiters one at a time (with a
// tiny stagger so the queue order is deterministic), then release the
// held backend N times. Each release must wake the earliest-parked
// waiter.
func TestStrictFIFOSerialWaiters(t *testing.T) {
	dial := func(_ context.Context) (*backend.Conn, error) {
		return &backend.Conn{}, nil
	}
	p := New("strict-fifo", dial, Config{
		DefaultPoolSize: 1,
		QueryWait:       2 * time.Second,
		Log:             testutil.Discard,
	})
	defer p.Close()

	c1, err := p.Acquire(context.Background())
	require.NoError(t, err)

	const N = 8
	var (
		startedMu sync.Mutex
		started   []int
		gotMu     sync.Mutex
		got       []int
		wg        sync.WaitGroup
	)
	wg.Add(N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			startedMu.Lock()
			started = append(started, i)
			startedMu.Unlock()
			c, err := p.Acquire(context.Background())
			if err != nil {
				return
			}
			gotMu.Lock()
			got = append(got, i)
			gotMu.Unlock()
			p.Release(c, false)
		}()
		// Stagger by ~3ms so each goroutine adds itself to waiters
		// before the next one starts.
		time.Sleep(3 * time.Millisecond)
	}

	// Wait until all N goroutines are parked.
	require.Eventually(t, func() bool {
		return p.Stats().Waiters == N
	}, time.Second, 5*time.Millisecond)

	p.Release(c1, false)
	wg.Wait()

	require.Equal(t, len(started), len(got), "all waiters served")
	// The order in `got` follows release order. With sequential releases
	// inside the workers (each releases before the next can pop), FIFO
	// is fully strict — `got` must equal `started`.
	require.Equal(t, started, got, "FIFO order violated")
}

// TestStressLargeFleet: 200 workers × 50 iters × pool-size-8 = 10k
// acquires. Larger than the basic stress; verifies no leak.
func TestStressLargeFleet(t *testing.T) {
	const (
		workers    = 200
		iterations = 50
		poolSize   = 8
	)
	dialed := atomic.Int64{}
	dial := func(_ context.Context) (*backend.Conn, error) {
		dialed.Add(1)
		return &backend.Conn{}, nil
	}
	p := New("big-stress", dial, Config{
		DefaultPoolSize: poolSize,
		QueryWait:       5 * time.Second,
		Log:             testutil.Discard,
	})
	defer p.Close()

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				c, err := p.Acquire(context.Background())
				require.NoError(t, err)
				p.Release(c, false)
			}
		}()
	}
	wg.Wait()

	st := p.Stats()
	require.Equal(t, int64(workers*iterations), int64(st.TotalAcquired))
	require.LessOrEqual(t, int(dialed.Load()), poolSize,
		"no extra dials beyond pool size")
	require.Equal(t, 0, st.Active, "no leaked checkouts")
}
