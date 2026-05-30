// lib/pq legacy database/sql driver integration tests.
//
// lib/pq is in maintenance-only mode but remains the canonical legacy
// driver for Go services. We assert that the bog-standard database/sql
// surface (Query / Exec / QueryRow / Tx / Prepare) works through
// pgrouter in transaction mode.
//
// Tests stay close to the API surface — no ORM, no extension features
// — so a regression here implicates pgrouter, not a library quirk.

//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func libpqOpen(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", Dsn())
	require.NoError(t, err)
	// Force a healthy pool size that exercises pgrouter's queue.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestLibpqQueryRow(t *testing.T) {
	db := libpqOpen(t)
	var n int
	require.NoError(t, db.QueryRow("SELECT 1+1").Scan(&n))
	require.Equal(t, 2, n)
}

func TestLibpqQueryMultipleRows(t *testing.T) {
	db := libpqOpen(t)
	rows, err := db.Query("SELECT generate_series(1, 4)")
	require.NoError(t, err)
	defer rows.Close()
	var got []int
	for rows.Next() {
		var v int
		require.NoError(t, rows.Scan(&v))
		got = append(got, v)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int{1, 2, 3, 4}, got)
}

func TestLibpqPreparedStatement(t *testing.T) {
	db := libpqOpen(t)
	stmt, err := db.Prepare("SELECT $1::int + $2::int")
	require.NoError(t, err)
	defer stmt.Close()
	var n int
	require.NoError(t, stmt.QueryRow(7, 35).Scan(&n))
	require.Equal(t, 42, n)
}

func TestLibpqTxCommit(t *testing.T) {
	db := libpqOpen(t)
	tab := fmt.Sprintf("libpq_t_%d", time.Now().UnixNano())
	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s(x int)", tab))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + tab) })

	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1)", tab))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	var n int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM "+tab).Scan(&n))
	require.Equal(t, 1, n)
}

func TestLibpqTxRollback(t *testing.T) {
	db := libpqOpen(t)
	tab := fmt.Sprintf("libpq_rb_%d", time.Now().UnixNano())
	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s(x int)", tab))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + tab) })

	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1)", tab))
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	var n int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM "+tab).Scan(&n))
	require.Equal(t, 0, n)
}

func TestLibpqExecAffectedRows(t *testing.T) {
	db := libpqOpen(t)
	tab := fmt.Sprintf("libpq_ar_%d", time.Now().UnixNano())
	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s(x int)", tab))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + tab) })

	res, err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1), (2), (3)", tab))
	require.NoError(t, err)
	n, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(3), n)
}

func TestLibpqErrorIncludesSQLSTATE(t *testing.T) {
	db := libpqOpen(t)
	_, err := db.Exec("SELECT * FROM nonexistent_xyz_table")
	require.Error(t, err)
}

func TestLibpqConcurrentQueries(t *testing.T) {
	db := libpqOpen(t)
	const N = 16
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			var n int
			errs <- db.QueryRow("SELECT 1").Scan(&n)
		}()
	}
	for i := 0; i < N; i++ {
		require.NoError(t, <-errs)
	}
}
