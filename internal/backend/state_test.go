package backend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLifecycleInitialState(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	lc := NewLifecycle(now)
	require.Equal(t, StateNew, lc.State())
	require.Equal(t, now, lc.CreatedAt())
	require.Equal(t, now, lc.LastActive())
	require.Equal(t, uint64(0), lc.UseCount())
}

func TestLifecycleTransitions(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	lc := NewLifecycle(t0)

	t1 := t0.Add(time.Second)
	lc.MarkActive(t1)
	require.Equal(t, StateActive, lc.State())
	require.Equal(t, t1, lc.LastActive())
	require.Equal(t, uint64(1), lc.UseCount())

	t2 := t1.Add(time.Second)
	lc.MarkIdle(t2)
	require.Equal(t, StateIdle, lc.State())
	require.Equal(t, t2, lc.LastActive())

	lc.MarkActive(t2.Add(time.Second))
	require.Equal(t, uint64(2), lc.UseCount())

	lc.MarkClosed()
	require.Equal(t, StateClosed, lc.State())
}

func TestLifecycleShouldRecycle(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	lc := NewLifecycle(t0)
	require.False(t, lc.ShouldRecycle(t0, 0)) // 0 = disabled
	require.False(t, lc.ShouldRecycle(t0.Add(time.Minute), time.Hour))
	require.True(t, lc.ShouldRecycle(t0.Add(time.Hour), time.Hour))
	require.True(t, lc.ShouldRecycle(t0.Add(2*time.Hour), time.Hour))
}

func TestLifecycleShouldEvict(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	lc := NewLifecycle(t0)
	// Active state never evicts.
	lc.MarkActive(t0)
	require.False(t, lc.ShouldEvict(t0.Add(time.Hour), time.Second))

	// Idle state evicts past maxIdle.
	lc.MarkIdle(t0)
	require.False(t, lc.ShouldEvict(t0.Add(time.Second), time.Minute))
	require.True(t, lc.ShouldEvict(t0.Add(2*time.Minute), time.Minute))

	// maxIdle=0 disables.
	require.False(t, lc.ShouldEvict(t0.Add(time.Hour), 0))
}
