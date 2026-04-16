// Integration tests for the PoC pass-through proxy.
// Requires a running Postgres and pgrouter listening on $PGROUTER_ADDR.
//
//go:build integration

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func dsn() string {
	if v := os.Getenv("PGROUTER_DSN"); v != "" {
		return v
	}
	return "postgres://test@127.0.0.1:6432/test?sslmode=disable"
}

func connect(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dsn())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func TestSimpleSelect(t *testing.T) {
	c := connect(t)
	var n int
	require.NoError(t, c.QueryRow(context.Background(), "SELECT 1+1").Scan(&n))
	require.Equal(t, 2, n)
}

func TestParameterizedQuery(t *testing.T) {
	c := connect(t)
	var n int
	require.NoError(t, c.QueryRow(context.Background(), "SELECT $1::int", 42).Scan(&n))
	require.Equal(t, 42, n)
}

func TestPreparedStatement(t *testing.T) {
	c := connect(t)
	ctx := context.Background()
	_, err := c.Prepare(ctx, "stmt1", "SELECT $1::text || 'x'")
	require.NoError(t, err)
	var s string
	require.NoError(t, c.QueryRow(ctx, "stmt1", "foo").Scan(&s))
	require.Equal(t, "foox", s)
}

func TestMultiRow(t *testing.T) {
	c := connect(t)
	rows, err := c.Query(context.Background(), "SELECT generate_series(1,3)")
	require.NoError(t, err)
	defer rows.Close()
	var got []int
	for rows.Next() {
		var v int
		require.NoError(t, rows.Scan(&v))
		got = append(got, v)
	}
	require.Equal(t, []int{1, 2, 3}, got)
}

func TestTransaction(t *testing.T) {
	c := connect(t)
	ctx := context.Background()
	tx, err := c.Begin(ctx)
	require.NoError(t, err)
	var db string
	require.NoError(t, tx.QueryRow(ctx, "SELECT current_database()").Scan(&db))
	require.NotEmpty(t, db)
	require.NoError(t, tx.Commit(ctx))
}
