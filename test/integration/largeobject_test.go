// Large Object (lo) protocol smoke test through pgrouter.
//
// Postgres large objects use lo_create / lo_open / lo_read / lo_write
// SQL functions and the LargeObject API (pg_largeobject_metadata
// catalog underneath). All ops must happen inside a single transaction
// — pgrouter's transaction-mode pool naturally satisfies that as long
// as the client opens BEGIN before lo_create and commits at the end.
//
// This test:
//   1. Open a tx.
//   2. Create a large object, write 64 KiB, read it back.
//   3. Commit.
//
// Documents the caveat: clients using lo MUST wrap calls in an explicit
// transaction, otherwise pgrouter releases the backend mid-flow and the
// lo handle becomes invalid.

//go:build integration

package integration

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestLargeObjectRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, Dsn())
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// MUST be wrapped in an explicit transaction so the same backend
	// stays attached across lo_create / lo_write / lo_read.
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	los := tx.LargeObjects()
	oid, err := los.Create(ctx, 0)
	require.NoError(t, err)

	lo, err := los.Open(ctx, oid, pgx.LargeObjectModeWrite|pgx.LargeObjectModeRead)
	require.NoError(t, err)

	want := bytes.Repeat([]byte("ABCD"), 16*1024) // 64 KiB
	n, err := lo.Write(want)
	require.NoError(t, err)
	require.Equal(t, len(want), n)

	// Rewind.
	_, err = lo.Seek(0, 0)
	require.NoError(t, err)

	got := make([]byte, len(want))
	read, err := lo.Read(got)
	require.NoError(t, err)
	require.Equal(t, len(want), read)
	require.Equal(t, want, got)

	require.NoError(t, lo.Close())

	// Cleanup + commit.
	require.NoError(t, los.Unlink(ctx, oid))
	require.NoError(t, tx.Commit(ctx))
}
