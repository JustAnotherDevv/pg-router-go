package client

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGUCEmpty(t *testing.T) {
	c := NewGUCCache()
	require.Equal(t, 0, c.Len())
	require.Empty(t, c.ReplayQuery())
}

func TestGUCSimpleSet(t *testing.T) {
	c := NewGUCCache()
	require.True(t, c.ObserveQuery("SET search_path = my_schema"))
	require.Equal(t, 1, c.Len())
	require.Equal(t, "my_schema", c.Snapshot()["search_path"])
}

func TestGUCSetTOSyntax(t *testing.T) {
	c := NewGUCCache()
	require.True(t, c.ObserveQuery("SET timezone TO 'UTC'"))
	require.Equal(t, "'UTC'", c.Snapshot()["timezone"])
}

func TestGUCSetSessionExplicit(t *testing.T) {
	c := NewGUCCache()
	require.True(t, c.ObserveQuery("SET SESSION work_mem = '64MB'"))
	require.Equal(t, "'64MB'", c.Snapshot()["work_mem"])
}

func TestGUCSetLocalSkipped(t *testing.T) {
	c := NewGUCCache()
	require.False(t, c.ObserveQuery("SET LOCAL search_path = tx_only"))
	require.Equal(t, 0, c.Len())
}

func TestGUCSetCaseInsensitive(t *testing.T) {
	c := NewGUCCache()
	require.True(t, c.ObserveQuery("set TimeZone = utc"))
	// Variable name is normalised to lowercase.
	require.Equal(t, "utc", c.Snapshot()["timezone"])
}

func TestGUCSetReplacesValue(t *testing.T) {
	c := NewGUCCache()
	c.ObserveQuery("SET x = 1")
	require.True(t, c.ObserveQuery("SET x = 2"))
	require.Equal(t, "2", c.Snapshot()["x"])
	require.False(t, c.ObserveQuery("SET x = 2"), "no change → not modified")
}

func TestGUCResetSingle(t *testing.T) {
	c := NewGUCCache()
	c.ObserveQuery("SET a = 1")
	c.ObserveQuery("SET b = 2")
	require.True(t, c.ObserveQuery("RESET a"))
	require.Equal(t, 1, c.Len())
	require.False(t, c.ObserveQuery("RESET nonexistent"))
}

func TestGUCResetAll(t *testing.T) {
	c := NewGUCCache()
	c.ObserveQuery("SET a = 1")
	c.ObserveQuery("SET b = 2")
	require.True(t, c.ObserveQuery("RESET ALL"))
	require.Equal(t, 0, c.Len())
}

func TestGUCDiscardAll(t *testing.T) {
	c := NewGUCCache()
	c.ObserveQuery("SET a = 1")
	c.ObserveQuery("SET b = 2")
	require.True(t, c.ObserveQuery("DISCARD ALL"))
	require.Equal(t, 0, c.Len())
}

func TestGUCNotASetIsIgnored(t *testing.T) {
	c := NewGUCCache()
	require.False(t, c.ObserveQuery("SELECT 1"))
	require.False(t, c.ObserveQuery("BEGIN"))
	require.False(t, c.ObserveQuery("CREATE TABLE foo (id int)"))
	require.False(t, c.ObserveQuery(""))
}

func TestGUCReplayQueryContainsAllVars(t *testing.T) {
	c := NewGUCCache()
	c.ObserveQuery("SET search_path = my_schema")
	c.ObserveQuery("SET timezone = 'UTC'")
	replay := c.ReplayQuery()
	require.Contains(t, replay, "SET search_path=my_schema")
	require.Contains(t, replay, "SET timezone='UTC'")
	require.Equal(t, 2, strings.Count(replay, "SET "))
}

func TestGUCConcurrentReadWrite(t *testing.T) {
	// Smoke test: under -race a fence shows up if locking is wrong.
	c := NewGUCCache()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			c.ObserveQuery("SET k = v")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = c.Snapshot()
		}
	}()
	wg.Wait()
}
