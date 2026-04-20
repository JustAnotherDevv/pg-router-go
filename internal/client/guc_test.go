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
	require.True(t, c.ObserveQuery("SET SESSION statement_timeout = '5min'"))
	require.Equal(t, "'5min'", c.Snapshot()["statement_timeout"])
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
	c.ObserveQuery("SET statement_timeout = '1s'")
	require.True(t, c.ObserveQuery("SET statement_timeout = '2s'"))
	require.Equal(t, "'2s'", c.Snapshot()["statement_timeout"])
	require.False(t, c.ObserveQuery("SET statement_timeout = '2s'"), "no change → not modified")
}

func TestGUCResetSingle(t *testing.T) {
	c := NewGUCCache()
	c.ObserveQuery("SET search_path = a")
	c.ObserveQuery("SET timezone = 'UTC'")
	require.True(t, c.ObserveQuery("RESET search_path"))
	require.Equal(t, 1, c.Len())
	require.False(t, c.ObserveQuery("RESET nonexistent"))
}

func TestGUCResetAll(t *testing.T) {
	c := NewGUCCache()
	c.ObserveQuery("SET search_path = a")
	c.ObserveQuery("SET timezone = 'UTC'")
	require.True(t, c.ObserveQuery("RESET ALL"))
	require.Equal(t, 0, c.Len())
}

func TestGUCDiscardAll(t *testing.T) {
	c := NewGUCCache()
	c.ObserveQuery("SET search_path = a")
	c.ObserveQuery("SET timezone = 'UTC'")
	require.True(t, c.ObserveQuery("DISCARD ALL"))
	require.Equal(t, 0, c.Len())
}

// --- whitelist behaviour (M.10 Phase A) ---

func TestGUCUnrecognizedSetTriggersPin(t *testing.T) {
	c := NewGUCCache()
	require.False(t, c.HasUnrecognizedSet())
	// `work_mem` is NOT in the default replayable whitelist.
	require.True(t, c.ObserveQuery("SET work_mem = '64MB'"))
	require.True(t, c.HasUnrecognizedSet(),
		"SET of un-whitelisted GUC must trigger pin signal")
	// And the unrecognized name must NOT be stored.
	require.Equal(t, 0, c.Len(),
		"un-whitelisted SET must not enter the replay cache")
}

func TestGUCDiscardClearsUnrecognizedFlag(t *testing.T) {
	c := NewGUCCache()
	c.ObserveQuery("SET work_mem = '64MB'")
	require.True(t, c.HasUnrecognizedSet())
	c.ObserveQuery("DISCARD ALL")
	require.False(t, c.HasUnrecognizedSet(),
		"DISCARD ALL must reset the unrecognized flag")
}

func TestGUCResetAllClearsUnrecognizedFlag(t *testing.T) {
	c := NewGUCCache()
	c.ObserveQuery("SET work_mem = '64MB'")
	require.True(t, c.HasUnrecognizedSet())
	c.ObserveQuery("RESET ALL")
	require.False(t, c.HasUnrecognizedSet())
}

func TestGUCWhitelistedSetDoesNotTriggerPin(t *testing.T) {
	c := NewGUCCache()
	require.True(t, c.ObserveQuery("SET search_path = myschema"))
	require.False(t, c.HasUnrecognizedSet())
	require.True(t, c.ObserveQuery("SET application_name = myapp"))
	require.False(t, c.HasUnrecognizedSet())
}

func TestGUCCustomWhitelist(t *testing.T) {
	wl := map[string]struct{}{"work_mem": {}}
	c := NewGUCCacheWith(wl)
	require.True(t, c.ObserveQuery("SET work_mem = '64MB'"))
	require.False(t, c.HasUnrecognizedSet())
	require.Equal(t, "'64MB'", c.Snapshot()["work_mem"])
	// And search_path is NOT replayable in this custom map.
	require.True(t, c.ObserveQuery("SET search_path = x"))
	require.True(t, c.HasUnrecognizedSet())
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
