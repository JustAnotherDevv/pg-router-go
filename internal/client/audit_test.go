package client

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAuditWriterEmitsJSONLine(t *testing.T) {
	var buf bytes.Buffer
	a := NewAuditWriter(&buf)
	a.Write("abc123", "appdb", "alice", "myapp", "query",
		"SELECT ?", 12*time.Millisecond+700*time.Microsecond)

	line := strings.TrimSpace(buf.String())
	require.NotEmpty(t, line)
	// Parse via generic map — the auditEvent struct was deleted as
	// part of the hand-formatted-JSON refactor; the wire format
	// is still the same JSON keys.
	var ev map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &ev))
	require.Equal(t, "abc123", ev["req_id"])
	require.Equal(t, "appdb", ev["db"])
	require.Equal(t, "alice", ev["user"])
	require.Equal(t, "myapp", ev["app"])
	require.Equal(t, "query", ev["kind"])
	require.Equal(t, "SELECT ?", ev["sql"])
	require.InDelta(t, 12.7, ev["duration_ms"].(float64), 0.01)
	require.NotEmpty(t, ev["ts"])
}

func TestAuditWriterNilSafe(t *testing.T) {
	var a *AuditWriter
	a.Write("x", "x", "x", "x", "query", "SELECT 1", time.Second)
	a = &AuditWriter{}
	a.Write("x", "x", "x", "x", "query", "SELECT 1", time.Second)
}

func TestOpenAuditFileNopOnEmptyPath(t *testing.T) {
	a, err := OpenAuditFile("")
	require.NoError(t, err)
	require.Nil(t, a)
}

func TestOpenAuditFileAppends(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	a, err := OpenAuditFile(path)
	require.NoError(t, err)
	require.NotNil(t, a)
	a.Write("r1", "db", "u", "", "query", "SELECT 1", time.Millisecond)
	a.Write("r2", "db", "u", "", "parse", "SELECT $1", 2*time.Millisecond)
	require.NoError(t, a.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 2)
	require.Contains(t, lines[0], `"req_id":"r1"`)
	require.Contains(t, lines[1], `"req_id":"r2"`)
}
