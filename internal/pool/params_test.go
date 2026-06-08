package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pg-router-go/internal/backend"
	"github.com/JustAnotherDevv/pg-router-go/internal/testutil"
)

func TestCachedParamsEmptyBeforeAnyDial(t *testing.T) {
	p := newPool(t, "noop", func(_ context.Context) (*backend.Conn, error) {
		return &backend.Conn{}, nil
	}, Config{DefaultPoolSize: 1})
	require.Nil(t, p.CachedParams())
}

func TestCachedParamsCapturedOnFirstDial(t *testing.T) {
	wantParams := map[string]string{
		"server_version":  "16.4 (Ubuntu)",
		"client_encoding": "UTF8",
		"TimeZone":        "Etc/UTC",
	}
	p := newPool(t, "real-params", func(_ context.Context) (*backend.Conn, error) {
		// Copy because callers may mutate; defensive.
		params := make(map[string]string, len(wantParams))
		for k, v := range wantParams {
			params[k] = v
		}
		return &backend.Conn{Params: params}, nil
	}, Config{DefaultPoolSize: 2})

	c, err := p.Acquire(context.Background())
	require.NoError(t, err)
	defer p.Release(c, false)

	got := p.CachedParams()
	require.Equal(t, wantParams, got)
}

func TestCachedParamsFirstDialWins(t *testing.T) {
	dials := atomic.Int64{}
	p := newPool(t, "first-wins", func(_ context.Context) (*backend.Conn, error) {
		n := dials.Add(1)
		// Each dial returns a DIFFERENT server_version. We want to
		// verify only the FIRST dial's params show up in the cache.
		params := map[string]string{
			"server_version": map[int64]string{
				1: "16.4 (first)",
				2: "16.4 (second)",
			}[n],
		}
		return &backend.Conn{Params: params}, nil
	}, Config{DefaultPoolSize: 2})

	c1, _ := p.Acquire(context.Background())
	c2, _ := p.Acquire(context.Background())
	require.Equal(t, int64(2), dials.Load())

	got := p.CachedParams()
	require.Equal(t, "16.4 (first)", got["server_version"],
		"only the FIRST upstream's params should populate the cache")

	p.Release(c1, false)
	p.Release(c2, false)
}

func TestCachedParamsConcurrentCapture(t *testing.T) {
	// Race-detector test: many goroutines acquiring simultaneously.
	// captureParams must be safe and produce SOME deterministic snapshot.
	p := newPool(t, "concurrent", func(_ context.Context) (*backend.Conn, error) {
		return &backend.Conn{Params: map[string]string{"k": "v"}}, nil
	}, Config{DefaultPoolSize: 16})

	const N = 16
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			c, err := p.Acquire(context.Background())
			require.NoError(t, err)
			p.Release(c, false)
		}()
	}
	wg.Wait()
	require.Equal(t, map[string]string{"k": "v"}, p.CachedParams())
}

func TestCachedParamsEmptyParamsDoNotPopulate(t *testing.T) {
	p := newPool(t, "empty-conn", func(_ context.Context) (*backend.Conn, error) {
		// A dial that returns a Conn with no Params (defensive against
		// upstreams that hand us none â€” e.g. our test stubs).
		return &backend.Conn{}, nil
	}, Config{DefaultPoolSize: 1})

	c, err := p.Acquire(context.Background())
	require.NoError(t, err)
	defer p.Release(c, false)

	require.Nil(t, p.CachedParams(),
		"empty Params on conn should leave cache nil for next dial to try")
}

func TestCachedParamsAvailableQuickly(t *testing.T) {
	// Regression: cache must be populated BEFORE Acquire returns to the
	// caller (i.e. before any sendWelcome could read it).
	captured := make(chan map[string]string, 1)
	p := New("ordering", func(_ context.Context) (*backend.Conn, error) {
		return &backend.Conn{Params: map[string]string{"server_version": "16.0"}}, nil
	}, Config{
		DefaultPoolSize: 1,
		Log:             testutil.Discard,
	})
	defer p.Close()

	go func() {
		c, err := p.Acquire(context.Background())
		if err != nil {
			captured <- nil
			return
		}
		// As soon as Acquire returns, cache MUST be populated.
		captured <- p.CachedParams()
		p.Release(c, false)
	}()
	select {
	case got := <-captured:
		require.NotNil(t, got)
		require.Equal(t, "16.0", got["server_version"])
	case <-time.After(time.Second):
		t.Fatal("acquire never returned")
	}
}
