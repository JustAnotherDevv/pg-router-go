// Per-query event surface for downstream observability plugins.
//
// onQueryComplete used to be a closed fan-out: stats + slow_query +
// audit + OTel span end. Adding a new sink (per-tenant cost meter,
// SLA tracker, custom log channel) required editing onQueryComplete
// + recompiling pgrouter.
//
// QueryHook is the extension point. PooledConn.Hooks is a slice
// of user-provided callbacks invoked once per completed Query/Parse,
// AFTER the built-in sinks have run. The QueryEvent carries the
// rendered (LogSQL-mode-honoured) SQL so hooks don't re-run RedactSQL.
//
// Hooks must be cheap + non-blocking. A slow hook ties up the
// per-conn drain goroutine. For external dispatch (Kafka, Datadog),
// hooks should push into a buffered channel and let a separate
// goroutine drain.

package client

import "time"

// QueryEvent is the per-query payload passed to QueryHook callbacks.
// Fields are read-only; hooks MUST NOT mutate.
type QueryEvent struct {
	Kind        string        // "query" | "parse"
	SQL         string        // raw SQL as received from the client
	RenderedSQL string        // LogSQL-mode-applied form (off / redacted / full)
	PrepName    string        // server-side prepared name (empty for simple Query)
	Duration    time.Duration // time from message received → backend RFQ
	Database    string        // PooledConn.Database
	User        string        // PooledConn.User
	App         string        // PooledConn.App (application_name)
	ReqID       string        // PooledConn.ReqID
}

// QueryHook is one subscriber. Called after stats + slow_query + audit
// + OTel span have run, with the same rendered SQL those sinks used.
type QueryHook func(ev QueryEvent)
