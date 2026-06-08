package pool

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pg-router-go/internal/backend"
	"github.com/JustAnotherDevv/pg-router-go/internal/testutil"
)

func TestPoolResizeGrowsCap(t *testing.T) {
	dial := func(ctx context.Context) (*backend.Conn, error) {
		return &backend.Conn{}, nil
	}
	p := New("t", dial, Config{
		DefaultPoolSize: 2,
		QueryWait:       time.Second,
		Log:             testutil.Discard,
	})
	require.Equal(t, 2, p.Size())
	prev := p.Resize(8)
	require.Equal(t, 2, prev)
	require.Equal(t, 8, p.Size())
}

func TestPoolResizeShrinkEvictsIdle(t *testing.T) {
	dial := func(ctx context.Context) (*backend.Conn, error) {
		return &backend.Conn{}, nil
	}
	p := New("t", dial, Config{
		DefaultPoolSize: 5,
		QueryWait:       time.Second,
		Log:             testutil.Discard,
	})
	// Acquire + release 5 to build the idle stack.
	conns := make([]*backend.Conn, 0, 5)
	for i := 0; i < 5; i++ {
		c, err := p.Acquire(context.Background())
		require.NoError(t, err)
		conns = append(conns, c)
	}
	for _, c := range conns {
		p.Release(c, false)
	}
	s := p.Stats()
	require.Equal(t, 5, s.Idle)

	p.Resize(2)
	require.Equal(t, 2, p.Size())

	require.Eventually(t, func() bool {
		s := p.Stats()
		return s.Idle <= 2
	}, time.Second, 10*time.Millisecond)
}

func TestManagerApplyDefaultSizeRetargets(t *testing.T) {
	dial := func(_ Key) Dialer {
		return func(ctx context.Context) (*backend.Conn, error) {
			return &backend.Conn{}, nil
		}
	}
	m := NewManager(Config{DefaultPoolSize: 4, Log: testutil.Discard}, dial)
	p := m.Get(Key{DB: "appdb", User: "alice"})
	require.Equal(t, 4, p.Size())
	changes := m.ApplyDefaultSize(10, nil)
	require.Len(t, changes, 1)
	require.Equal(t, 4, changes[0].From)
	require.Equal(t, 10, changes[0].To)
	require.Equal(t, 10, p.Size())
}

func TestManagerApplyDefaultSizePerKeyOverride(t *testing.T) {
	dial := func(_ Key) Dialer {
		return func(ctx context.Context) (*backend.Conn, error) {
			return &backend.Conn{}, nil
		}
	}
	m := NewManager(Config{DefaultPoolSize: 4, Log: testutil.Discard}, dial)
	pa := m.Get(Key{DB: "appdb", User: "alice"})
	pb := m.Get(Key{DB: "logs", User: "writer"})
	changes := m.ApplyDefaultSize(10, func(k Key) int {
		if k.DB == "logs" {
			return 50
		}
		return 0
	})
	require.Len(t, changes, 2)
	require.Equal(t, 10, pa.Size())
	require.Equal(t, 50, pb.Size())
}
