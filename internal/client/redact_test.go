package client

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactSQLEmpty(t *testing.T) {
	require.Equal(t, "", RedactSQL(""))
}

func TestRedactSQLSingleQuotedString(t *testing.T) {
	require.Equal(t, "SELECT ? WHERE x = ?",
		RedactSQL("SELECT 'hello' WHERE x = 42"))
}

func TestRedactSQLEscapedQuote(t *testing.T) {
	// '' inside a literal does NOT terminate it.
	require.Equal(t, "SELECT ?", RedactSQL("SELECT 'it''s fine'"))
}

func TestRedactSQLEStyleEscapedString(t *testing.T) {
	require.Equal(t, "SELECT ?", RedactSQL(`SELECT E'line\nbreak'`))
	require.Equal(t, "SELECT ?", RedactSQL(`SELECT e'\\'`))
}

func TestRedactSQLNumericFloatHex(t *testing.T) {
	require.Equal(t, "x = ?", RedactSQL("x = 42"))
	require.Equal(t, "x = ?", RedactSQL("x = 3.14"))
	require.Equal(t, "x = ?", RedactSQL("x = .5"))
	require.Equal(t, "x = ?", RedactSQL("x = 1.5e-3"))
	require.Equal(t, "x = ?", RedactSQL("x = 0xFF"))
}

func TestRedactSQLBindParamsKept(t *testing.T) {
	require.Equal(t, "SELECT * WHERE id = $1 AND email = $2",
		RedactSQL("SELECT * WHERE id = $1 AND email = $2"))
}

func TestRedactSQLDollarQuoteFunction(t *testing.T) {
	in := `CREATE FUNCTION f() AS $body$ SELECT 'secret'; $body$ LANGUAGE sql`
	out := RedactSQL(in)
	require.NotContains(t, out, "secret")
	require.NotContains(t, out, "'secret'")
	require.Contains(t, out, "?")
	require.Contains(t, out, "LANGUAGE sql")
}

func TestRedactSQLEmptyDollarQuote(t *testing.T) {
	require.Equal(t, "SELECT ?", RedactSQL(`SELECT $$hello$$`))
}

func TestRedactSQLDoubleQuotedIdentifierKept(t *testing.T) {
	require.Equal(t, `SELECT "My Col" FROM t WHERE x = ?`,
		RedactSQL(`SELECT "My Col" FROM t WHERE x = 1`))
}

func TestRedactSQLLineCommentKept(t *testing.T) {
	require.Equal(t, "SELECT ? -- the answer\n",
		RedactSQL("SELECT 1 -- the answer\n"))
	require.Contains(t, RedactSQL("SELECT 1 -- comment"), "-- comment")
}

func TestRedactSQLBlockCommentKept(t *testing.T) {
	require.Equal(t, "SELECT ? /* nested /* deeper */ end */ FROM t",
		RedactSQL("SELECT 5 /* nested /* deeper */ end */ FROM t"))
}

func TestRedactSQLUnterminatedLiteralPreserved(t *testing.T) {
	// We choose to preserve the suspicious form so the bug surfaces in
	// logs rather than silently being hidden.
	out := RedactSQL("SELECT $bad$ unterminated")
	require.True(t, strings.Contains(out, "$bad$"),
		"unterminated dollar-quote should be preserved verbatim, got %q", out)
}

func TestRedactSQLNoFalseRedactInIdentifier(t *testing.T) {
	// `user1` is an identifier, not a literal.
	require.Equal(t, "SELECT user1 FROM t",
		RedactSQL("SELECT user1 FROM t"))
}

func TestSQLForLogModes(t *testing.T) {
	sql := "SELECT 1 WHERE email='alice@example.com'"
	require.Equal(t, "", SQLForLog("off", sql, 0))
	require.Equal(t, sql, SQLForLog("full", sql, 0))
	red := SQLForLog("redacted", sql, 0)
	require.NotContains(t, red, "alice@example.com")
	require.Contains(t, red, "?")
}

func TestSQLForLogTruncates(t *testing.T) {
	long := strings.Repeat("x", 1000)
	out := SQLForLog("full", long, 50)
	// 50 chars + "..." = 53.
	require.LessOrEqual(t, len(out), 53)
	require.Less(t, len(out), len(long))
}
