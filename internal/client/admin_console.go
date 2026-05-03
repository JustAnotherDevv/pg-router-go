// PgBouncer-compatible SQL admin console.
//
// Clients connecting to the virtual database name "pgbouncer" land in
// this handler instead of the regular pool. We synthesise pgwire v3
// responses to a small SQL surface — no real Postgres involved.
//
// Supported commands (case-insensitive):
//
//   SHOW STATS        — per-(db, user) query + transaction totals
//   SHOW POOLS        — per-pool size/idle/active/waiters snapshot
//   SHOW DATABASES    — configured database list
//   SHOW LISTS        — global counters (databases, pools, conns)
//   SHOW VERSION      — pgrouter version + commit
//   SHOW HELP         — quick command list
//   PAUSE / RESUME    — accept/log; live drain is post-v1
//   RELOAD            — fires the synthetic SIGHUP via AdminAPI.Reload
//   <anything else>   — ERROR "unknown command"
//
// This matches the conventions of PgBouncer's admin console closely
// enough that tools like pgcli + Grafana sidecars + ops scripts work.

package client

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/proto"
	"github.com/JustAnotherDevv/pgrouter/internal/stats"
)

// AdminConsole is the handler invoked when a client connects to the
// virtual "pgbouncer" database.
type AdminConsole struct {
	Log     *slog.Logger
	Manager *pool.Manager

	// Reload, if non-nil, is fired by RELOAD. Same closure as the HTTP
	// admin API's Reload — pushes a synthetic SIGHUP into the reloader.
	Reload func() error
}

// adminReceiveDeadline bounds each Receive() so a stalled / idle admin
// conn doesn't block process shutdown. Plenty of headroom for slow
// human-typed psql sessions; ctx cancel breaks the loop sooner.
const adminReceiveDeadline = 30 * time.Second

// Serve runs the admin protocol on an already-authenticated client.
// Emits the AuthOK welcome itself; no upstream backend touched.
//
// Honors ctx — when the parent context is cancelled (SIGTERM / Stop),
// the conn is closed and Serve returns. Without this an idle admin
// client blocks on be.Receive() forever and gracefulshutdown stalls
// past the drain deadline.
func (a *AdminConsole) Serve(ctx context.Context, conn net.Conn) error {
	defer conn.Close()
	be := pgproto3.NewBackend(conn, conn)

	// Close the conn when ctx fires — unblocks the Receive() loop.
	// done channel guards the goroutine from outliving Serve.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	// Welcome.
	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "pgrouter-admin"})
	be.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	be.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 0}})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := be.Flush(); err != nil {
		return err
	}

	for {
		// Per-message deadline; idle clients can't pin the goroutine
		// indefinitely. Cleared after Receive so handleQuery doesn't
		// inherit it.
		_ = conn.SetReadDeadline(time.Now().Add(adminReceiveDeadline))
		msg, err := be.Receive()
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil {
			if ctx.Err() != nil {
				return nil // shutdown
			}
			return err
		}
		switch m := msg.(type) {
		case *pgproto3.Terminate:
			return nil
		case *pgproto3.Query:
			a.handleQuery(be, m.String)
		default:
			a.sendError(be, "0A000",
				fmt.Sprintf("admin console: unsupported message type %T", m))
		}
	}
}

// adminHandler is one entry in the dispatch table.
type adminHandler func(a *AdminConsole, be *pgproto3.Backend, upper string)

// adminHandlers is the prefix → handler dispatch table. Replaces the
// previous HasPrefix cascade. Adding a new SHOW command is now a
// single-line registration.
//
// Lookup is linear (≤10 entries) so the lack of a real trie is fine;
// HasPrefix on a small map beats nesting more if-else branches.
var adminHandlers = []struct {
	prefix string
	fn     adminHandler
}{
	{"SHOW STATS", func(a *AdminConsole, be *pgproto3.Backend, _ string) { a.showStats(be) }},
	{"SHOW POOLS", func(a *AdminConsole, be *pgproto3.Backend, _ string) { a.showPools(be) }},
	{"SHOW DATABASES", func(a *AdminConsole, be *pgproto3.Backend, _ string) { a.showDatabases(be) }},
	{"SHOW LISTS", func(a *AdminConsole, be *pgproto3.Backend, _ string) { a.showLists(be) }},
	{"SHOW VERSION", func(a *AdminConsole, be *pgproto3.Backend, _ string) { a.showVersion(be) }},
	{"SHOW HELP", func(a *AdminConsole, be *pgproto3.Backend, _ string) { a.showHelp(be) }},
	{"PAUSE", func(a *AdminConsole, be *pgproto3.Backend, up string) {
		a.sendCommandComplete(be, up, "PAUSE accepted (no-op in v1)")
	}},
	{"RESUME", func(a *AdminConsole, be *pgproto3.Backend, up string) {
		a.sendCommandComplete(be, up, "RESUME accepted (no-op in v1)")
	}},
	{"RELOAD", func(a *AdminConsole, be *pgproto3.Backend, _ string) { a.doReload(be) }},
}

// handleQuery dispatches one SQL statement and emits the appropriate
// rowset + RFQ.
func (a *AdminConsole) handleQuery(be *pgproto3.Backend, sql string) {
	trimmed := strings.TrimSpace(strings.TrimRight(sql, ";"))
	upper := strings.ToUpper(trimmed)
	// `HELP` aliases SHOW HELP.
	if upper == "HELP" {
		a.showHelp(be)
		return
	}
	for _, h := range adminHandlers {
		if strings.HasPrefix(upper, h.prefix) {
			h.fn(a, be, upper)
			return
		}
	}
	a.sendError(be, "42601",
		fmt.Sprintf("admin console: unknown command: %s", trimmed))
}

func (a *AdminConsole) showStats(be *pgproto3.Backend) {
	cols := []pgproto3.FieldDescription{
		col("database"), col("user"),
		col("total_xact_count"), col("total_query_count"),
	}
	a.sendRowDesc(be, cols)
	for _, ps := range a.Manager.AllStats() {
		db, user := splitName(ps.Name)
		a.sendDataRow(be,
			db, user,
			fmt.Sprintf("%d", ps.TotalAcquired),
			fmt.Sprintf("%d", ps.TotalSpawned),
		)
	}
	a.completeAndRFQ(be, "SHOW")
}

func (a *AdminConsole) showPools(be *pgproto3.Backend) {
	cols := []pgproto3.FieldDescription{
		col("database"), col("user"),
		col("cl_active"), col("cl_waiting"),
		col("sv_active"), col("sv_idle"),
		col("pool_size"),
	}
	a.sendRowDesc(be, cols)
	for _, p := range a.Manager.Pools() {
		ps := p.Stats()
		db, user := splitName(ps.Name)
		a.sendDataRow(be,
			db, user,
			fmt.Sprintf("%d", ps.Active),
			fmt.Sprintf("%d", ps.Waiters),
			fmt.Sprintf("%d", ps.Active),
			fmt.Sprintf("%d", ps.Idle),
			fmt.Sprintf("%d", p.Size()),
		)
	}
	a.completeAndRFQ(be, "SHOW")
}

func (a *AdminConsole) showDatabases(be *pgproto3.Backend) {
	cols := []pgproto3.FieldDescription{col("name"), col("backend_pools")}
	a.sendRowDesc(be, cols)
	// We don't keep raw config here — derive the set from active pools.
	seen := map[string]int{}
	for _, p := range a.Manager.Pools() {
		db, _ := splitName(p.Name())
		seen[db]++
	}
	for db, n := range seen {
		a.sendDataRow(be, db, fmt.Sprintf("%d", n))
	}
	a.completeAndRFQ(be, "SHOW")
}

func (a *AdminConsole) showLists(be *pgproto3.Backend) {
	cols := []pgproto3.FieldDescription{col("list"), col("items")}
	a.sendRowDesc(be, cols)
	pools := a.Manager.Pools()
	var dbs, users int
	dbSet := map[string]struct{}{}
	userSet := map[string]struct{}{}
	for _, p := range pools {
		db, user := splitName(p.Name())
		dbSet[db] = struct{}{}
		userSet[user] = struct{}{}
	}
	dbs = len(dbSet)
	users = len(userSet)
	a.sendDataRow(be, "databases", fmt.Sprintf("%d", dbs))
	a.sendDataRow(be, "users", fmt.Sprintf("%d", users))
	a.sendDataRow(be, "pools", fmt.Sprintf("%d", len(pools)))
	a.completeAndRFQ(be, "SHOW")
}

func (a *AdminConsole) showVersion(be *pgproto3.Backend) {
	cols := []pgproto3.FieldDescription{col("version")}
	a.sendRowDesc(be, cols)
	a.sendDataRow(be,
		fmt.Sprintf("pgrouter %s (%s)", stats.Build.Version, stats.Build.Commit))
	a.completeAndRFQ(be, "SHOW")
}

func (a *AdminConsole) showHelp(be *pgproto3.Backend) {
	cols := []pgproto3.FieldDescription{col("command")}
	a.sendRowDesc(be, cols)
	for _, c := range []string{
		"SHOW STATS", "SHOW POOLS", "SHOW DATABASES",
		"SHOW LISTS", "SHOW VERSION", "SHOW HELP",
		"PAUSE", "RESUME", "RELOAD",
	} {
		a.sendDataRow(be, c)
	}
	a.completeAndRFQ(be, "SHOW")
}

func (a *AdminConsole) doReload(be *pgproto3.Backend) {
	if a.Reload == nil {
		a.sendError(be, "0A000", "RELOAD: not wired (Admin.Reload nil)")
		return
	}
	if err := a.Reload(); err != nil {
		a.sendError(be, "XX000", fmt.Sprintf("RELOAD failed: %v", err))
		return
	}
	a.sendCommandComplete(be, "RELOAD", "RELOAD")
}

// Method wrappers delegate to internal/proto so the admin handlers
// read naturally while the actual pgwire synth lives in one place
// (also reusable by future code that needs to fabricate responses).

func col(name string) pgproto3.FieldDescription { return proto.TextCol(name) }

func (a *AdminConsole) sendRowDesc(be *pgproto3.Backend, cols []pgproto3.FieldDescription) {
	proto.SendRowDesc(be, cols)
}

func (a *AdminConsole) sendDataRow(be *pgproto3.Backend, vals ...string) {
	proto.SendDataRow(be, vals...)
}

func (a *AdminConsole) completeAndRFQ(be *pgproto3.Backend, tag string) {
	proto.CompleteAndRFQ(be, tag)
}

func (a *AdminConsole) sendCommandComplete(be *pgproto3.Backend, tag, msg string) {
	proto.SendNoticeCompleteRFQ(be, tag, msg)
}

func (a *AdminConsole) sendError(be *pgproto3.Backend, code, msg string) {
	proto.SendErrorRFQ(be, code, msg)
}

// splitName forwards to the canonical pool.SplitName. Local wrapper
// kept so admin handlers read naturally.
func splitName(s string) (string, string) {
	k := pool.SplitName(s)
	return k.DB, k.User
}
