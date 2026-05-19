package client

import (
	"bytes"
	"strings"
	"testing"

	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestLogSQLOffEmitsNoSQLField(t *testing.T) {
	var buf bytes.Buffer
	pc := &PooledConn{PooledConfig: PooledConfig{LogSQL: "off"}, Log: testutil.CaptureLog(&buf)}
	pc.logSQL(pc.Log, "query", "", "SELECT 'secret'")
	out := buf.String()
	require.NotContains(t, out, "secret")
	require.NotContains(t, out, "sql=")
}

func TestLogSQLRedactedHidesLiterals(t *testing.T) {
	var buf bytes.Buffer
	pc := &PooledConn{PooledConfig: PooledConfig{LogSQL: "redacted"}, Log: testutil.CaptureLog(&buf)}
	pc.logSQL(pc.Log, "query", "", "SELECT 'alice@example.com', 4111111111111111")
	out := buf.String()
	require.NotContains(t, out, "alice@example.com")
	require.NotContains(t, out, "4111111111111111")
	require.Contains(t, out, "sql=")
	require.Contains(t, out, "?") // redactor output
}

func TestLogSQLFullEmitsRawSQL(t *testing.T) {
	var buf bytes.Buffer
	pc := &PooledConn{PooledConfig: PooledConfig{LogSQL: "full"}, Log: testutil.CaptureLog(&buf)}
	pc.logSQL(pc.Log, "query", "", "SELECT 'secret'")
	out := buf.String()
	require.Contains(t, out, "secret")
}

func TestLogSQLEmptyDefaultsToRedacted(t *testing.T) {
	var buf bytes.Buffer
	pc := &PooledConn{Log: testutil.CaptureLog(&buf)}
	pc.logSQL(pc.Log, "parse", "", "SELECT 'leaky-text'")
	out := buf.String()
	require.NotContains(t, out, "leaky-text")
}

func TestLogSQLParseIncludesPrepName(t *testing.T) {
	var buf bytes.Buffer
	pc := &PooledConn{PooledConfig: PooledConfig{LogSQL: "redacted"}, Log: testutil.CaptureLog(&buf)}
	pc.logSQL(pc.Log, "parse", "stmt7", "SELECT $1")
	require.Contains(t, buf.String(), "prepared_name=stmt7")
}

// End-to-end through observeClientMessage: a Query message produces a
// `kind=query` log entry; a Parse produces `kind=parse`.
func TestObserveClientMessageEmitsQueryLog(t *testing.T) {
	var buf bytes.Buffer
	pc := &PooledConn{PooledConfig: PooledConfig{LogSQL: "redacted"}, Log: testutil.CaptureLog(&buf)}
	guc := NewGUCCache()
	prep := NewPrepareCache()
	pinned := false
	pc.observeClientMessage(&pgproto3.Query{String: "SELECT 1"},
		guc, prep, &pinned, pc.Log, nil)
	require.Contains(t, buf.String(), "kind=query")
}

func TestObserveClientMessageEmitsParseLog(t *testing.T) {
	var buf bytes.Buffer
	pc := &PooledConn{PooledConfig: PooledConfig{LogSQL: "redacted"}, Log: testutil.CaptureLog(&buf)}
	guc := NewGUCCache()
	prep := NewPrepareCache()
	pinned := false
	pc.observeClientMessage(&pgproto3.Parse{Name: "s1", Query: "SELECT $1"},
		guc, prep, &pinned, pc.Log, nil)
	out := buf.String()
	require.Contains(t, out, "kind=parse")
	require.Contains(t, out, "prepared_name=s1")
}

// Sanity: the dispatcher attaches a req_id field via the slog logger
// that survives at least one observeClientMessage hop. We can't easily
// drive Handle without a full TCP path, so verify the logger threading
// by manually applying With on the captured logger.
func TestRequestIDPropagatesThroughLogSQL(t *testing.T) {
	var buf bytes.Buffer
	base := testutil.CaptureLog(&buf).With("req_id", "abc012345678")
	pc := &PooledConn{PooledConfig: PooledConfig{LogSQL: "redacted"}, Log: base}
	pc.logSQL(pc.Log, "query", "", "SELECT 1")
	require.Contains(t, buf.String(), "req_id=abc012345678")
}

// Long SQL gets truncated.
func TestLogSQLTruncatesLongStatements(t *testing.T) {
	var buf bytes.Buffer
	pc := &PooledConn{PooledConfig: PooledConfig{LogSQL: "full"}, Log: testutil.CaptureLog(&buf)}
	long := strings.Repeat("X", 1024)
	pc.logSQL(pc.Log, "query", "", long)
	out := buf.String()
	// 256 chars truncation cap; full line plus framing should be < 600.
	require.Less(t, len(out), 600)
}
