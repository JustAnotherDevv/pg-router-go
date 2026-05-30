package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNeedsSessionPinPositive(t *testing.T) {
	pos := []string{
		"LISTEN channel_name",
		"LISTEN     ch",
		"listen ch_low",
		"SELECT pg_advisory_lock(42)",
		"SELECT pg_advisory_lock_shared(42)",
		"CREATE TEMP TABLE t (id int)",
		"CREATE TEMPORARY TABLE t (id int)",
		"create local temp table t(id int)",
		"CREATE GLOBAL TEMPORARY TABLE t(id int)",
		"DECLARE c CURSOR FOR SELECT 1",
		"DECLARE c BINARY CURSOR FOR SELECT 1",
		"DECLARE c SCROLL CURSOR FOR SELECT 1",
		"declare c insensitive cursor for select 1",
	}
	for _, s := range pos {
		require.True(t, needsSessionPin(s), "should pin: %q", s)
	}
}

func TestNeedsSessionPinNegative(t *testing.T) {
	neg := []string{
		"SELECT 1",
		"SELECT * FROM t",
		"BEGIN",
		"COMMIT",
		"SET timezone = 'UTC'",
		// xact_lock variants are txn-scoped → safe in txn mode.
		"SELECT pg_advisory_xact_lock(42)",
		"SELECT pg_advisory_xact_lock_shared(42)",
		// LISTEN inside a comment-prefixed sentence
		"-- explain LISTEN later",
		// CREATE non-temp table
		"CREATE TABLE t (id int)",
		// CURSOR WITH HOLD — actually txn-safe in pg; we err on the
		// safe side and pin anyway. Not asserting either way for this
		// edge case.
	}
	for _, s := range neg {
		require.False(t, needsSessionPin(s), "should NOT pin: %q", s)
	}
}
