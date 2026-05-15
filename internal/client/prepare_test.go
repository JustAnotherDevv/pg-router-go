package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareCacheEmpty(t *testing.T) {
	c := NewPrepareCache()
	require.Equal(t, 0, c.Len())
	require.Nil(t, c.Get("nope"))
	require.False(t, c.Close("nope"))
}

func TestPrepareCacheObserve(t *testing.T) {
	c := NewPrepareCache()
	prev := c.Observe("s1", "SELECT $1::int", []uint32{23})
	require.Nil(t, prev)
	require.Equal(t, 1, c.Len())

	s := c.Get("s1")
	require.NotNil(t, s)
	require.Equal(t, "s1", s.Name)
	require.Equal(t, "SELECT $1::int", s.Query)
	require.Equal(t, []uint32{23}, s.ParamOIDs)
}

func TestPrepareCacheRebindReturnsPrev(t *testing.T) {
	c := NewPrepareCache()
	c.Observe("s1", "SELECT 1", nil)
	prev := c.Observe("s1", "SELECT 2", nil)
	require.NotNil(t, prev)
	require.Equal(t, "SELECT 1", prev.Query)
	require.Equal(t, "SELECT 2", c.Get("s1").Query)
}

func TestPrepareCacheUnnamedIgnored(t *testing.T) {
	c := NewPrepareCache()
	c.Observe("", "SELECT 1", nil)
	require.Equal(t, 0, c.Len(), "unnamed stmts are not tracked")
}

func TestPrepareCacheClose(t *testing.T) {
	c := NewPrepareCache()
	c.Observe("a", "Q", nil)
	c.Observe("b", "Q", nil)
	require.True(t, c.Close("a"))
	require.Equal(t, 1, c.Len())
	require.False(t, c.Close("a"), "second close is a noop")
}

func TestPrepareCacheCloseAll(t *testing.T) {
	c := NewPrepareCache()
	c.Observe("a", "Q", nil)
	c.Observe("b", "Q", nil)
	c.CloseAll()
	require.Equal(t, 0, c.Len())
}

func TestPrepareCacheSnapshotIsCopy(t *testing.T) {
	c := NewPrepareCache()
	c.Observe("s1", "Q", []uint32{1})
	snap := c.Snapshot()
	require.Len(t, snap, 1)
	delete(snap, "s1")
	require.Equal(t, 1, c.Len(), "snapshot mutation must not affect cache")
}

func TestPrepareCacheParamOIDsCopied(t *testing.T) {
	c := NewPrepareCache()
	oids := []uint32{23, 25}
	c.Observe("s1", "Q", oids)
	oids[0] = 999
	require.Equal(t, []uint32{23, 25}, c.Get("s1").ParamOIDs,
		"observed ParamOIDs should be a copy")
}

