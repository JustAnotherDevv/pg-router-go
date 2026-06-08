// PgBouncer-compatible SQL admin console.
//
// Clients connecting to the virtual database name "pgbouncer" land in
// this handler instead of the regular pool. We synthesise pgwire v3
// responses to a small SQL surface â€” no real Postgres involved.
//
// Supported commands (case-insensitive):
//
//   SHOW STATS        â€” per-(db, user) query + transaction totals
//   SHOW POOLS        â€” per-pool size/idle/active/waiters snapshot
//   SHOW DATABASES    â€” configured database list
//   SHOW LISTS        â€” global counters (databases, pools, conns)
//   SHOW VERSION      â€” pgrouter version + commit
//   SHOW HELP         â€” quick command list
//   PAUSE / RESUME    â€” accept/log; live drain is post-v1
//   RELOAD            â€” fires the synthetic SIGHUP via AdminAPI.Reload
//   <anything else>   â€” ERROR "unknown command"
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

	"github.com/JustAnotherDevv/pg-router-go/internal/pool"
	"github.com/JustAnotherDevv/pg-router-go/internal/proto"
	"github.com/JustAnotherDevv/pg-router-go/internal/stats"
)

// AdminConsole is the handler invoked when a client connects to the
// virtual "pgbouncer" database.
type AdminConsole struct {
	Log     *slog.Logger
	Manager *pool.Manager

	// Reload, if non-nil, is fired by RELOAD. Same closure as the HTTP
	// admin API's Reload â€” pushes a synthetic SIGHUP into the reloader.
	Reload func() error
}

// adminReceiveDeadline bounds each Receive() so a stalled / idle admin
// conn doesn't block process shutdown. Plenty of headroom for slow
// human-typed psql sessions; ctx cancel breaks the loop sooner.
const adminReceiveDeadline = 30 * time.Second

// Serve runs the admin protocol on an already-authenticated client.
// Emits the AuthOK welcome itself; no upstream backend touched.
//
// Honors ctx â€” when the parent context is cancelled (SIGTERM / Stop),
// the conn is closed and Serve returns. Without this an idle admin
// client blocks on be.Receive() forever and gracefulshutdown stalls
// past the drain deadline.
func (a *AdminConsole) Serve(ctx context.Context, conn net.Conn) error {
	defer conn.Close()
	be := pgproto3.NewBackend(conn, conn)

	// Close the conn when ctx fires â€” unblocks the Receive() loop.
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
			proto.SendErrorRFQ(be, "0A000",
				fmt.Sprintf("admin console: unsupported message type %T", m))
		}
	}
}

// adminHandler is one entry in the dispatch table.
type adminHandler func(a *AdminConsole, be *pgproto3.Backend, upper string)

// adminHandlers is the prefix â†’ handler dispatch table. Replaces the
// previous HasPrefix cascade. Adding a new SHOW command is now a
// single-line registration.
//
// Lookup is linear (â‰¤10 entries) so the lack of a real trie is fine;
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
		proto.SendNoticeCompleteRFQ(be, up, "PAUSE accepted (no-op in v1)")
	}},
	{"RESUME", func(a *AdminConsole, be *pgproto3.Backend, up string) {
		proto.SendNoticeCompleteRFQ(be, up, "RESUME accepted (no-op in v1)")
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
	proto.SendErrorRFQ(be, "42601",
		fmt.Sprintf("admin console: unknown command: %s", trimmed))
}

// emitTable is the canonical SHOW emit pattern: row descriptor â†’ row
// stream â†’ CommandComplete + RFQ. `colNames` defines the column shape;
// `rowsFn` yields one row at a time as a slice of stringified cells
// (length must match colNames).
func (a *AdminConsole) emitTable(be *pgproto3.Backend, colNames []string, rowsFn func(emit func(...string))) {
	cols := make([]pgproto3.FieldDescription, len(colNames))
	for i, n := range colNames {
		cols[i] = proto.TextCol(n)
	}
	proto.SendRowDesc(be, cols)
	rowsFn(func(vals ...string) { proto.SendDataRow(be, vals...) })
	proto.CompleteAndRFQ(be, "SHOW")
}

func itoa(n int) string    { return fmt.Sprintf("%d", n) }
func u64a(n uint64) string { return fmt.Sprintf("%d", n) }

func (a *AdminConsole) showStats(be *pgproto3.Backend) {
	a.emitTable(be, []string{"database", "user", "total_xact_count", "total_query_count"},
		func(emit func(...string)) {
			for _, ps := range a.Manager.AllStats() {
				k := pool.SplitName(ps.Name)
				emit(k.DB, k.User, u64a(ps.TotalAcquired), u64a(ps.TotalSpawned))
			}
		})
}

func (a *AdminConsole) showPools(be *pgproto3.Backend) {
	a.emitTable(be,
		[]string{"database", "user", "cl_active", "cl_waiting", "sv_active", "sv_idle", "pool_size"},
		func(emit func(...string)) {
			for _, p := range a.Manager.Pools() {
				ps := p.Stats()
				k := pool.SplitName(ps.Name)
				emit(k.DB, k.User, itoa(ps.Active), itoa(ps.Waiters),
					itoa(ps.Active), itoa(ps.Idle), itoa(p.Size()))
			}
		})
}

func (a *AdminConsole) showDatabases(be *pgproto3.Backend) {
	a.emitTable(be, []string{"name", "backend_pools"}, func(emit func(...string)) {
		seen := map[string]int{}
		for _, p := range a.Manager.Pools() {
			seen[pool.SplitName(p.Name()).DB]++
		}
		for db, n := range seen {
			emit(db, itoa(n))
		}
	})
}

func (a *AdminConsole) showLists(be *pgproto3.Backend) {
	a.emitTable(be, []string{"list", "items"}, func(emit func(...string)) {
		pools := a.Manager.Pools()
		dbSet, userSet := map[string]struct{}{}, map[string]struct{}{}
		for _, p := range pools {
			k := pool.SplitName(p.Name())
			dbSet[k.DB] = struct{}{}
			userSet[k.User] = struct{}{}
		}
		emit("databases", itoa(len(dbSet)))
		emit("users", itoa(len(userSet)))
		emit("pools", itoa(len(pools)))
	})
}

func (a *AdminConsole) showVersion(be *pgproto3.Backend) {
	a.emitTable(be, []string{"version"}, func(emit func(...string)) {
		emit(fmt.Sprintf("pgrouter %s (%s)", stats.Build.Version, stats.Build.Commit))
	})
}

func (a *AdminConsole) showHelp(be *pgproto3.Backend) {
	a.emitTable(be, []string{"command"}, func(emit func(...string)) {
		for _, c := range []string{
			"SHOW STATS", "SHOW POOLS", "SHOW DATABASES",
			"SHOW LISTS", "SHOW VERSION", "SHOW HELP",
			"PAUSE", "RESUME", "RELOAD",
		} {
			emit(c)
		}
	})
}

func (a *AdminConsole) doReload(be *pgproto3.Backend) {
	if a.Reload == nil {
		proto.SendErrorRFQ(be, "0A000", "RELOAD: not wired (Admin.Reload nil)")
		return
	}
	if err := a.Reload(); err != nil {
		proto.SendErrorRFQ(be, "XX000", fmt.Sprintf("RELOAD failed: %v", err))
		return
	}
	proto.SendNoticeCompleteRFQ(be, "RELOAD", "RELOAD")
}
