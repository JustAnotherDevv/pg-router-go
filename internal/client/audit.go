// Per-tenant audit log.
//
// When logging.audit_file is set, pgrouter opens an append-only file
// and writes one JSON line per executed Query/Parse — separate from
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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// AuditWriter is the process-wide audit log target. nil = audit off.
type AuditWriter struct {
	mu sync.Mutex
	w  io.Writer
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
	return &AuditWriter{w: f}, nil
}

// NewAuditWriter is the in-memory variant for tests.
func NewAuditWriter(w io.Writer) *AuditWriter {
	return &AuditWriter{w: w}
}

// auditEvent is the wire-format JSON record.
type auditEvent struct {
	TS         string  `json:"ts"`
	ReqID      string  `json:"req_id"`
	DB         string  `json:"db"`
	User       string  `json:"user"`
	App        string  `json:"app,omitempty"`
	Kind       string  `json:"kind"`
	DurationMs float64 `json:"duration_ms"`
	SQL        string  `json:"sql,omitempty"`
}

// Write emits one JSON record terminated by '\n'. Safe to call
// concurrently from many goroutines.
func (a *AuditWriter) Write(reqID, db, user, app, kind, sql string, dur time.Duration) {
	if a == nil || a.w == nil {
		return
	}
	ev := auditEvent{
		TS:         time.Now().UTC().Format(time.RFC3339Nano),
		ReqID:      reqID,
		DB:         db,
		User:       user,
		App:        app,
		Kind:       kind,
		DurationMs: float64(dur.Microseconds()) / 1000.0,
		SQL:        sql,
	}
	b, err := json.Marshal(&ev)
	if err != nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, _ = a.w.Write(append(b, '\n'))
}
