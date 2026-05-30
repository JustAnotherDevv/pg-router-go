// sqlx integration tests for pgrouter.
//
// sqlx is database/sql + a thin sugar layer (Get / Select / NamedExec).
// It uses lib/pq under the hood here so this suite also incidentally
// exercises the legacy lib/pq driver against pgrouter — see libpq_test.go
// for the explicit lib/pq coverage.

//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func sqlxOpen(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := sqlx.Open("postgres", Dsn())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type sqlxRow struct {
	ID   int       `db:"id"`
	Name string    `db:"name"`
	Made time.Time `db:"made_at"`
}

func sqlxMakeTable(t *testing.T, db *sqlx.DB) string {
	t.Helper()
	name := fmt.Sprintf("sqlx_t_%d", time.Now().UnixNano())
	_, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (id serial primary key, name text not null, made_at timestamptz default now())", name))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + name) })
	return name
}

func TestSqlxGet(t *testing.T) {
	db := sqlxOpen(t)
	var n int
	require.NoError(t, db.Get(&n, "SELECT 7"))
	require.Equal(t, 7, n)
}

func TestSqlxSelectInto(t *testing.T) {
	db := sqlxOpen(t)
	tab := sqlxMakeTable(t, db)
	_, err := db.Exec(fmt.Sprintf(
		"INSERT INTO %s(name) VALUES ($1), ($2), ($3)", tab),
		"Alice", "Bob", "Carol")
	require.NoError(t, err)
	var rows []sqlxRow
	require.NoError(t, db.Select(&rows,
		fmt.Sprintf("SELECT id, name, made_at FROM %s ORDER BY id", tab)))
	require.Len(t, rows, 3)
	require.Equal(t, "Alice", rows[0].Name)
}

func TestSqlxNamedExec(t *testing.T) {
	db := sqlxOpen(t)
	tab := sqlxMakeTable(t, db)
	res, err := db.NamedExec(
		fmt.Sprintf("INSERT INTO %s(name) VALUES (:name)", tab),
		map[string]any{"name": "Dave"})
	require.NoError(t, err)
	n, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
}

func TestSqlxTransactionCommit(t *testing.T) {
	db := sqlxOpen(t)
	tab := sqlxMakeTable(t, db)
	tx, err := db.Beginx()
	require.NoError(t, err)
	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s(name) VALUES ($1)", tab), "Eve")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	var c int
	require.NoError(t, db.Get(&c, "SELECT count(*) FROM "+tab))
	require.Equal(t, 1, c)
}

func TestSqlxTransactionRollback(t *testing.T) {
	db := sqlxOpen(t)
	tab := sqlxMakeTable(t, db)
	tx, err := db.Beginx()
	require.NoError(t, err)
	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s(name) VALUES ($1)", tab), "Eve")
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	var c int
	require.NoError(t, db.Get(&c, "SELECT count(*) FROM "+tab))
	require.Equal(t, 0, c)
}

func TestSqlxQueryxScan(t *testing.T) {
	db := sqlxOpen(t)
	rows, err := db.Queryx("SELECT generate_series(1, 5) AS x")
	require.NoError(t, err)
	defer rows.Close()
	var got []int
	for rows.Next() {
		var v int
		require.NoError(t, rows.Scan(&v))
		got = append(got, v)
	}
	require.Equal(t, []int{1, 2, 3, 4, 5}, got)
}

func TestSqlxContextDeadlineHonoured(t *testing.T) {
	db := sqlxOpen(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var n int
	err := db.GetContext(ctx, &n, "SELECT pg_sleep(1), 1")
	require.Error(t, err, "expected context deadline to abort the query")
}
