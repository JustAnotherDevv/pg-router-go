package backend

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreparedCacheEmpty(t *testing.T) {
	c := NewPreparedCache(8)
	require.Equal(t, 0, c.Len())
	require.False(t, c.Has("anything"))
	require.Equal(t, "", c.Add("")) // empty name no-op
	require.Equal(t, 0, c.Len())
}

func TestPreparedCacheDefaultCapacity(t *testing.T) {
	c := NewPreparedCache(0) // 0 → default
	require.Equal(t, DefaultPreparedCacheCapacity, c.Cap())
	c2 := NewPreparedCache(-5)
	require.Equal(t, DefaultPreparedCacheCapacity, c2.Cap())
}

func TestPreparedCacheAddHas(t *testing.T) {
	c := NewPreparedCache(8)
	require.Equal(t, "", c.Add("a"))
	require.True(t, c.Has("a"))
	require.False(t, c.Has("b"))
	require.Equal(t, 1, c.Len())
}

func TestPreparedCacheAddExistingReturnsNoEvict(t *testing.T) {
	c := NewPreparedCache(2)
	require.Equal(t, "", c.Add("a"))
	require.Equal(t, "", c.Add("a"))
	require.Equal(t, 1, c.Len())
}

func TestPreparedCacheLRUEvictsOldest(t *testing.T) {
	c := NewPreparedCache(3)
	require.Equal(t, "", c.Add("a")) // [a]
	require.Equal(t, "", c.Add("b")) // [b, a]
	require.Equal(t, "", c.Add("c")) // [c, b, a]
	evicted := c.Add("d")              // [d, c, b]
	require.Equal(t, "a", evicted, "LRU should evict the least-recently-added")
	require.False(t, c.Has("a"))
	require.True(t, c.Has("b"))
	require.True(t, c.Has("c"))
	require.True(t, c.Has("d"))
}

func TestPreparedCacheTouchBumpsToMRU(t *testing.T) {
	c := NewPreparedCache(3)
	c.Add("a") // [a]
	c.Add("b") // [b, a]
	c.Add("c") // [c, b, a]
	c.Touch("a") // [a, c, b]
	evicted := c.Add("d") // [d, a, c]
	require.Equal(t, "b", evicted, "Touch should have moved a away from LRU")
	require.True(t, c.Has("a"))
	require.False(t, c.Has("b"))
}

func TestPreparedCacheRemove(t *testing.T) {
	c := NewPreparedCache(8)
	c.Add("a")
	c.Add("b")
	c.Remove("a")
	require.False(t, c.Has("a"))
	require.Equal(t, 1, c.Len())
	c.Remove("nonexistent") // no-op
	require.Equal(t, 1, c.Len())
}

func TestPreparedCacheClear(t *testing.T) {
	c := NewPreparedCache(8)
	c.Add("a")
	c.Add("b")
	c.Add("c")
	c.Clear()
	require.Equal(t, 0, c.Len())
	require.False(t, c.Has("a"))
}

func TestPreparedCacheNilSafe(t *testing.T) {
	var c *PreparedCache
	require.False(t, c.Has("x"))
	require.Equal(t, "", c.Add("x"))
	require.Equal(t, 0, c.Len())
	require.Equal(t, 0, c.Cap())
	c.Touch("x")    // no-op
	c.Remove("x")   // no-op
	c.Clear()       // no-op
	require.Nil(t, c.Snapshot())
}

func TestPreparedCacheSnapshotIsMRUFirst(t *testing.T) {
	c := NewPreparedCache(3)
	c.Add("a")
	c.Add("b")
	c.Add("c")
	snap := c.Snapshot()
	require.Equal(t, []string{"c", "b", "a"}, snap)
}
