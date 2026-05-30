package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// fakeQueryConn scripts a single round trip: it expects exactly one
// Send (validated), then drains its `responses` slice to the caller's
// Receive() calls in order.
type fakeQueryConn struct {
	got       pgproto3.FrontendMessage
	responses []pgproto3.BackendMessage
	sendErr   error
	closeErr  error
	closed    bool
}

func (f *fakeQueryConn) Send(m pgproto3.FrontendMessage) error {
	f.got = m
	return f.sendErr
}
func (f *fakeQueryConn) Receive() (pgproto3.BackendMessage, error) {
	if len(f.responses) == 0 {
		return nil, errors.New("no more responses")
	}
	m := f.responses[0]
	f.responses = f.responses[1:]
	return m, nil
}
func (f *fakeQueryConn) Close() error { f.closed = true; return f.closeErr }

func TestAuthQueryLookupSuccess(t *testing.T) {
	fc := &fakeQueryConn{
		responses: []pgproto3.BackendMessage{
			&pgproto3.DataRow{Values: [][]byte{
				[]byte("alice"),
				[]byte("md5d8578edf8458ce06fbc5bb76a58c5ca4"),
			}},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		},
	}
	f := NewAuthQueryFetcher(
		func(ctx context.Context, db string) (QueryConn, error) {
			require.Equal(t, "appdb", db)
			return fc, nil
		},
		"SELECT usename, passwd FROM pg_shadow WHERE usename = $1",
		time.Minute,
	)
	entry, err := f.Lookup(context.Background(), "appdb", "alice")
	require.NoError(t, err)
	require.Equal(t, "md5d8578edf8458ce06fbc5bb76a58c5ca4", entry.MD5Hash)
	require.True(t, fc.closed)

	q, ok := fc.got.(*pgproto3.Query)
	require.True(t, ok)
	require.Contains(t, q.String, "'alice'")
}

func TestAuthQueryRejectsBadUsername(t *testing.T) {
	f := NewAuthQueryFetcher(
		func(ctx context.Context, db string) (QueryConn, error) {
			t.Fatal("Dial should not be called for invalid username")
			return nil, nil
		},
		"SELECT 1",
		time.Minute,
	)
	_, err := f.Lookup(context.Background(), "appdb", "alice'; DROP--")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid username")
}

func TestAuthQueryEmptyRow(t *testing.T) {
	fc := &fakeQueryConn{
		responses: []pgproto3.BackendMessage{
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		},
	}
	f := NewAuthQueryFetcher(
		func(ctx context.Context, db string) (QueryConn, error) { return fc, nil },
		"SELECT 1 WHERE false", time.Minute,
	)
	_, err := f.Lookup(context.Background(), "appdb", "alice")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no row")
}

func TestAuthQueryCachesByUsername(t *testing.T) {
	calls := 0
	mkResp := func() *fakeQueryConn {
		return &fakeQueryConn{
			responses: []pgproto3.BackendMessage{
				&pgproto3.DataRow{Values: [][]byte{[]byte("alice"), []byte("wonderland")}},
				&pgproto3.ReadyForQuery{TxStatus: 'I'},
			},
		}
	}
	f := NewAuthQueryFetcher(
		func(ctx context.Context, db string) (QueryConn, error) {
			calls++
			return mkResp(), nil
		},
		"SELECT 1", time.Minute,
	)
	_, err := f.Lookup(context.Background(), "appdb", "alice")
	require.NoError(t, err)
	_, err = f.Lookup(context.Background(), "appdb", "alice")
	require.NoError(t, err)
	require.Equal(t, 1, calls, "second lookup should hit cache")
}

func TestAuthQueryMultipleRowsRejected(t *testing.T) {
	// A wildcard LIKE in auth_query (operator typo) could return many
	// rows. The fetcher must reject rather than silently use the first
	// row — the wrong row could grant the wrong user's credential.
	fc := &fakeQueryConn{
		responses: []pgproto3.BackendMessage{
			&pgproto3.DataRow{Values: [][]byte{[]byte("alice"), []byte("pw1")}},
			&pgproto3.DataRow{Values: [][]byte{[]byte("alex"), []byte("pw2")}},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		},
	}
	f := NewAuthQueryFetcher(
		func(ctx context.Context, db string) (QueryConn, error) { return fc, nil },
		"SELECT usename, passwd FROM pg_shadow WHERE usename LIKE $1",
		time.Minute,
	)
	_, err := f.Lookup(context.Background(), "appdb", "alice")
	require.Error(t, err)
	require.Contains(t, err.Error(), "returned 2 rows")
	require.Contains(t, err.Error(), "LIKE")
}

func TestAuthQuerySingleRowMultiColumn(t *testing.T) {
	// Single DataRow with the expected (user, pwd) — must succeed.
	// Regression: the old accumulator built `row = append(...)` per
	// value across all DataRows, so a future regression to per-call
	// append would silently pass this test by treating extra rows as
	// continuation. The single-row case must keep working.
	fc := &fakeQueryConn{
		responses: []pgproto3.BackendMessage{
			&pgproto3.DataRow{Values: [][]byte{
				[]byte("alice"),
				[]byte("md5d8578edf8458ce06fbc5bb76a58c5ca4"),
			}},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		},
	}
	f := NewAuthQueryFetcher(
		func(ctx context.Context, db string) (QueryConn, error) { return fc, nil },
		"SELECT usename, passwd FROM pg_shadow WHERE usename = $1",
		time.Minute,
	)
	entry, err := f.Lookup(context.Background(), "appdb", "alice")
	require.NoError(t, err)
	require.Equal(t, "md5d8578edf8458ce06fbc5bb76a58c5ca4", entry.MD5Hash)
}

func TestAuthQueryServerError(t *testing.T) {
	fc := &fakeQueryConn{
		responses: []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{Message: "boom"},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		},
	}
	f := NewAuthQueryFetcher(
		func(ctx context.Context, db string) (QueryConn, error) { return fc, nil },
		"SELECT 1", time.Minute,
	)
	_, err := f.Lookup(context.Background(), "appdb", "alice")
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
}

func TestSubstituteOne(t *testing.T) {
	require.Equal(t, "SELECT 'alice'",
		substituteOne("SELECT $1", "alice"))
	require.Equal(t, "WHERE u='bob' AND v='bob'",
		substituteOne("WHERE u=$1 AND v=$1", "bob"))
}

func TestValidIdent(t *testing.T) {
	require.True(t, validIdent("alice"))
	require.True(t, validIdent("user_42"))
	require.True(t, validIdent("u$"))
	require.False(t, validIdent(""))
	require.False(t, validIdent("alice'"))
	require.False(t, validIdent("alice; DROP"))
	require.False(t, validIdent("alice\x00"))
}
