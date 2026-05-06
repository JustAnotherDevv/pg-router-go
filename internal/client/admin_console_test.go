package client

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
)

// adminClient drives the admin console over a net.Pipe and returns
// the rows for one query (SHOW ...).
func adminClient(t *testing.T, ac *AdminConsole, sql string) [][]string {
	t.Helper()
	clt, srv := net.Pipe()
	defer clt.Close()
	go ac.Serve(context.Background(), srv)

	fe := pgproto3.NewFrontend(clt, clt)
	// Drain welcome.
	for {
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	fe.Send(&pgproto3.Query{String: sql})
	require.NoError(t, fe.Flush())

	var rows [][]string
	for {
		m, err := fe.Receive()
		require.NoError(t, err)
		switch x := m.(type) {
		case *pgproto3.DataRow:
			vals := make([]string, len(x.Values))
			for i, v := range x.Values {
				vals[i] = string(v)
			}
			rows = append(rows, vals)
		case *pgproto3.ReadyForQuery:
			return rows
		}
	}
}

func makeMgrWithOnePool(t *testing.T) *pool.Manager {
	dial := func(k pool.Key) pool.Dialer {
		return func(ctx context.Context) (*backend.Conn, error) {
			return &backend.Conn{}, nil
		}
	}
	m := pool.NewManager(pool.Config{
		DefaultPoolSize: 4,
		Log:             testutil.Discard,
	}, dial)
	m.Get(pool.Key{DB: "appdb", User: "alice"})
	t.Cleanup(func() { _ = m.CloseWithDeadline(time.Now().Add(time.Second)) })
	return m
}

func TestAdminShowPools(t *testing.T) {
	ac := &AdminConsole{Log: testutil.Discard, Manager: makeMgrWithOnePool(t)}
	rows := adminClient(t, ac, "SHOW POOLS")
	require.Len(t, rows, 1)
	require.Equal(t, "appdb", rows[0][0])
	require.Equal(t, "alice", rows[0][1])
	require.Equal(t, "4", rows[0][6], "pool_size column")
}

func TestAdminShowStats(t *testing.T) {
	ac := &AdminConsole{Log: testutil.Discard, Manager: makeMgrWithOnePool(t)}
	rows := adminClient(t, ac, "SHOW STATS")
	require.GreaterOrEqual(t, len(rows), 1)
	require.Equal(t, "appdb", rows[0][0])
}

func TestAdminShowDatabases(t *testing.T) {
	ac := &AdminConsole{Log: testutil.Discard, Manager: makeMgrWithOnePool(t)}
	rows := adminClient(t, ac, "SHOW DATABASES")
	require.Len(t, rows, 1)
	require.Equal(t, "appdb", rows[0][0])
}

func TestAdminShowLists(t *testing.T) {
	ac := &AdminConsole{Log: testutil.Discard, Manager: makeMgrWithOnePool(t)}
	rows := adminClient(t, ac, "SHOW LISTS")
	require.GreaterOrEqual(t, len(rows), 3)
	got := map[string]string{}
	for _, r := range rows {
		got[r[0]] = r[1]
	}
	require.Contains(t, got, "databases")
	require.Contains(t, got, "users")
	require.Contains(t, got, "pools")
}

func TestAdminShowVersion(t *testing.T) {
	ac := &AdminConsole{Log: testutil.Discard, Manager: makeMgrWithOnePool(t)}
	rows := adminClient(t, ac, "SHOW VERSION")
	require.Len(t, rows, 1)
	require.True(t, strings.HasPrefix(rows[0][0], "pgrouter"))
}

func TestAdminUnknownCommand(t *testing.T) {
	ac := &AdminConsole{Log: testutil.Discard, Manager: makeMgrWithOnePool(t)}
	clt, srv := net.Pipe()
	defer clt.Close()
	go ac.Serve(context.Background(), srv)

	fe := pgproto3.NewFrontend(clt, clt)
	for {
		m, _ := fe.Receive()
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	fe.Send(&pgproto3.Query{String: "DROP DATABASE prod"})
	require.NoError(t, fe.Flush())

	var sawErr bool
	for i := 0; i < 4; i++ {
		m, err := fe.Receive()
		require.NoError(t, err)
		if e, ok := m.(*pgproto3.ErrorResponse); ok {
			sawErr = true
			require.Equal(t, "42601", e.Code)
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	require.True(t, sawErr)
}

func TestAdminReloadFiresClosure(t *testing.T) {
	called := false
	ac := &AdminConsole{
		Log:     testutil.Discard,
		Manager: makeMgrWithOnePool(t),
		Reload:  func() error { called = true; return nil },
	}
	clt, srv := net.Pipe()
	defer clt.Close()
	go ac.Serve(context.Background(), srv)
	fe := pgproto3.NewFrontend(clt, clt)
	for {
		m, _ := fe.Receive()
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	fe.Send(&pgproto3.Query{String: "RELOAD"})
	require.NoError(t, fe.Flush())
	for {
		m, _ := fe.Receive()
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	require.True(t, called)
}
