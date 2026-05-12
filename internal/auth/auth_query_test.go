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

// newFetcher builds an AuthQueryFetcher that dials fc on every call.
// 5-line site-level boilerplate -> 1 line.
func newFetcher(fc *fakeQueryConn, sql string) *AuthQueryFetcher {
	return NewAuthQueryFetcher(
		func(_ context.Context, _ string) (QueryConn, error) { return fc, nil },
		sql, time.Minute,
	)
}

// dataRow returns a DataRow with the given byte-string values. Saves
// the repetitive `&pgproto3.DataRow{Values: [][]byte{...}}` literal.
func dataRow(values ...string) *pgproto3.DataRow {
	bs := make([][]byte, len(values))
	for i, v := range values {
		bs[i] = []byte(v)
	}
	return &pgproto3.DataRow{Values: bs}
}

var rfqIdle = &pgproto3.ReadyForQuery{TxStatus: 'I'}

// --- happy paths ---

func TestAuthQueryLookupSuccess(t *testing.T) {
	fc := &fakeQueryConn{responses: []pgproto3.BackendMessage{
		dataRow("alice", "md5d8578edf8458ce06fbc5bb76a58c5ca4"),
		rfqIdle,
	}}
	f := newFetcher(fc, "SELECT usename, passwd FROM pg_shadow WHERE usename = $1")
	entry, err := f.Lookup(context.Background(), "appdb", "alice")
	require.NoError(t, err)
	require.Equal(t, "md5d8578edf8458ce06fbc5bb76a58c5ca4", entry.MD5Hash)
	require.True(t, fc.closed)
	q, ok := fc.got.(*pgproto3.Query)
	require.True(t, ok)
	require.Contains(t, q.String, "'alice'")
}

func TestAuthQueryCachesByUsername(t *testing.T) {
	calls := 0
	mkResp := func() *fakeQueryConn {
		return &fakeQueryConn{responses: []pgproto3.BackendMessage{
			dataRow("alice", "wonderland"),
			rfqIdle,
		}}
	}
	f := NewAuthQueryFetcher(
		func(_ context.Context, _ string) (QueryConn, error) {
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

// --- error paths via table ---

func TestAuthQueryErrors(t *testing.T) {
	cases := []struct {
		name      string
		responses []pgproto3.BackendMessage
		sql       string
		username  string
		wantSub   []string // substrings expected in error message
		noDial    bool     // true: Dial must not be called
	}{
		{
			name:     "rejects bad username (no dial)",
			sql:      "SELECT 1",
			username: "alice'; DROP--",
			wantSub:  []string{"invalid username"},
			noDial:   true,
		},
		{
			name:      "empty row → no row error",
			responses: []pgproto3.BackendMessage{rfqIdle},
			sql:       "SELECT 1 WHERE false",
			username:  "alice",
			wantSub:   []string{"no row"},
		},
		{
			name: "multiple rows rejected (catches wildcard LIKE)",
			responses: []pgproto3.BackendMessage{
				dataRow("alice", "pw1"),
				dataRow("alex", "pw2"),
				rfqIdle,
			},
			sql:      "SELECT usename, passwd FROM pg_shadow WHERE usename LIKE $1",
			username: "alice",
			wantSub:  []string{"returned 2 rows", "LIKE"},
		},
		{
			name: "server ErrorResponse surfaced",
			responses: []pgproto3.BackendMessage{
				&pgproto3.ErrorResponse{Message: "boom"},
				rfqIdle,
			},
			sql:      "SELECT 1",
			username: "alice",
			wantSub:  []string{"boom"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var f *AuthQueryFetcher
			if tc.noDial {
				f = NewAuthQueryFetcher(
					func(_ context.Context, _ string) (QueryConn, error) {
						t.Fatal("Dial should not be called")
						return nil, nil
					},
					tc.sql, time.Minute,
				)
			} else {
				fc := &fakeQueryConn{responses: tc.responses}
				f = newFetcher(fc, tc.sql)
			}
			_, err := f.Lookup(context.Background(), "appdb", tc.username)
			require.Error(t, err)
			for _, sub := range tc.wantSub {
				require.Contains(t, err.Error(), sub)
			}
		})
	}
}

// --- pure helpers (no fake-conn needed) ---

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
