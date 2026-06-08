// Per-tenant audit log.
//
// When logging.audit_file is set, pgrouter opens an append-only file
// and writes one JSON line per executed Query/Parse â€” separate from
// the main slog stream. Schema (stable for downstream pipelines):
//
//	{
//	  "ts":       "2026-05-30T12:34:56.789Z",
//	  "req_id":   "ab12fa3b4c5d",
//	  "db":       "appdb",
//	  "user":     "alice",
//	  "app":      "my-service",
//	  "kind":     "query" | "parse",
//	  "duration_ms": 12.7,
//	  "sql":      "SELECT ?"  // honours LogSQL mode (off/redacted/full)
//	}
//
// The writer is shared across all PooledConns of the process: opens
// once at startup, writes are serialised on a mutex. Rotation is
// expected to be done externally (logrotate / journald compaction).

package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JustAnotherDevv/pg-router-go/internal/stats"
)

// AuditWriter is the process-wide audit log target. nil = audit off.
type AuditWriter struct {
	mu     sync.Mutex
	w      io.Writer
	closer io.Closer // optional; set when the writer was opened from a file

	// errored is a sticky flag: once a Write fails, subsequent
	// failures don't spam slog â€” the operator already saw the first
	// one and pgrouter_audit_write_errors_total ticks for each
	// failure. Cleared when a Write succeeds.
	errored atomic.Bool
	Log     *slog.Logger // optional; defaults to slog.Default
}

// Close flushes + closes the underlying file (if any).
func (a *AuditWriter) Close() error {
	if a == nil || a.closer == nil {
		return nil
	}
	return a.closer.Close()
}

// OpenAuditFile opens (creates if missing, appends if exists) the file
// at path with 0o640 perms. Returns nil + nil error when path == "" so
// callers can use the result unconditionally.
func OpenAuditFile(path string) (*AuditWriter, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.OpenFile(path,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open audit file %s: %w", path, err)
	}
	return &AuditWriter{w: f, closer: f}, nil
}

// NewAuditWriter is the in-memory variant for tests.
func NewAuditWriter(w io.Writer) *AuditWriter {
	return &AuditWriter{w: w}
}

// bufPool recycles the per-Write bytes.Buffer + RFC3339Nano scratch
// across all PooledConns. Audit on a 10k-QPS pooler used to allocate
// 1 buffer + 1 marshal output per query; we now share both.
var bufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// Write emits one JSON record terminated by '\n'. Safe to call
// concurrently from many goroutines.
//
// Hand-formats the JSON instead of going through reflect-based
// encoding/json â€” the schema is tiny + stable, so the saved
// reflection cost matters under load. Output is identical byte-for-
// byte (well, modulo float trailing zeros which we suppress).
//
// Encode happens OUTSIDE the lock so concurrent callers serialise
// only on the underlying file Write.
func (a *AuditWriter) Write(reqID, db, user, app, kind, sql string, dur time.Duration) {
	if a == nil || a.w == nil {
		return
	}
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	durMs := float64(dur.Microseconds()) / 1000.0

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	// Hand-built object â€” all values are JSON-quoted via json.Marshal
	// (handles UTF-8 + escape sequences) but skipping reflect on the
	// struct itself.
	buf.WriteByte('{')
	writeKV(buf, "ts", ts, false)
	writeKV(buf, "req_id", reqID, false)
	writeKV(buf, "db", db, false)
	writeKV(buf, "user", user, false)
	if app != "" {
		writeKV(buf, "app", app, false)
	}
	writeKV(buf, "kind", kind, false)
	buf.WriteString(`"duration_ms":`)
	buf.WriteString(strconv.FormatFloat(durMs, 'f', -1, 64))
	if sql != "" {
		buf.WriteByte(',')
		writeKV(buf, "sql", sql, true)
	}
	buf.WriteByte('}')
	buf.WriteByte('\n')

	a.mu.Lock()
	_, err := a.w.Write(buf.Bytes())
	a.mu.Unlock()
	a.observeWriteResult(err)
}

// observeWriteResult ticks pgrouter_audit_write_errors_total on err,
// emits a one-shot WARN on first transition into the failed state,
// and clears the sticky flag on a successful write. Audit failures
// are best-effort so the SQL request flow isn't affected.
func (a *AuditWriter) observeWriteResult(err error) {
	if err == nil {
		if a.errored.CompareAndSwap(true, false) {
			a.logger().Info("audit log writes recovered")
		}
		return
	}
	stats.OnAuditWriteError()
	if a.errored.CompareAndSwap(false, true) {
		a.logger().Warn("audit log write failed; subsequent failures suppressed",
			"err", err)
	}
}

func (a *AuditWriter) logger() *slog.Logger {
	if a.Log != nil {
		return a.Log
	}
	return slog.Default()
}

// writeKV appends `"key":<json-quoted value>` to buf. last=true skips
// the trailing comma (caller is the last field before duration_ms /
// closing brace).
//
// Falls back to json.Marshal for the value so we handle escapes +
// non-ASCII UTF-8 the same way encoding/json would. On Marshal err
// (rare â€” non-UTF-8 bytes), writes `"<key>":"<err marker>"` so the
// operator sees a per-field gap instead of a silent drop.
func writeKV(buf *bytes.Buffer, key, value string, last bool) {
	buf.WriteByte('"')
	buf.WriteString(key)
	buf.WriteString(`":`)
	if b, err := json.Marshal(value); err == nil {
		buf.Write(b)
	} else {
		buf.WriteString(`"<marshal err: `)
		buf.WriteString(err.Error())
		buf.WriteString(`>"`)
	}
	if !last {
		buf.WriteByte(',')
	}
}

// Ensure fmt is still referenced (used by OpenAuditFile).
var _ = fmt.Sprintf
