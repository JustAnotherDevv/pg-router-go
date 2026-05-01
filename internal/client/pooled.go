// Transaction-mode pooled proxy.
//
// The PoC's client.Conn pins one backend per client conn for the full
// session. PooledConn instead:
//
//  1. Synthesizes the post-startup welcome (AuthenticationOk +
//     ParameterStatus* + BackendKeyData + ReadyForQuery) WITHOUT a
//     backend attached. The client sees pgrouter answering directly.
//  2. On the first client Query / Parse, Acquires a backend from the
//     pool and forwards the message. If the client has set any GUCs
//     during this session, the cache's ReplayQuery is fired first so
//     the (potentially fresh) backend has the right state.
//  3. Keeps forwarding bidirectionally inside the transaction.
//  4. When the backend's ReadyForQuery returns tx_status='I' (idle) AND
//     the client isn't session-pinned (LISTEN / advisory_lock / temp
//     table / cursor), Release the backend back to the pool. The
//     client sees the RFQ as usual.
//  5. The next Query / Parse Acquires again — possibly a different
//     backend.
//
// MVP scope (M.9):
//   - txn mode + automatic session pinning for incompatible features
//   - bare Query / Parse / Bind / Execute / Sync; COPY data is forwarded
//     1:1 inside an implicit transaction.
//   - GUC replay on fresh-backend acquire (per-client GUC cache).
//   - DISCARD ALL on release by default; per-client prepared-statement
//     bookkeeping (Parse / Close('S', name) tracked).

package client

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/proto"
	"github.com/JustAnotherDevv/pgrouter/internal/stats"
	"github.com/JustAnotherDevv/pgrouter/internal/tracing"
	"github.com/JustAnotherDevv/pgrouter/internal/util"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// PooledConn is a transaction-mode pooled handler for one client.
type PooledConn struct {
	Log *slog.Logger

	// Pool is the (db, user) pool to Acquire / Release from.
	Pool *pool.Pool

	// Database + User + App are the labels used for per-(db, user, app)
	// Prometheus metrics. Production paths set these from the
	// StartupMessage; tests may leave them empty (metrics simply emit
	// empty labels). App is the StartupMessage `application_name`
	// parameter — empty when the client didn't supply one.
	Database string
	User     string
	App      string

	// CannedParams are the ParameterStatus values we report to clients
	// before any real backend is attached. Production code populates
	// these from the first successful upstream connect; tests can
	// pass a minimal viable set.
	CannedParams map[string]string

	// ResetOnRelease, when true, sends the pool's configured ResetQuery
	// on every Release (defaults to DISCARD ALL). True in production
	// (NewPooledConn) so a backend never carries session state across
	// clients. Tests may override.
	ResetOnRelease bool

	// WelcomePID + WelcomeSecret, if non-zero, are emitted in the
	// BackendKeyData portion of the welcome message. Callers wire the
	// cancel.Tracker here so subsequent CancelRequest packets can be
	// routed back to this client's currently-attached backend. Zero
	// values cause a one-shot random key to be generated locally.
	WelcomePID    uint32
	WelcomeSecret []byte

	// QueryTimeout, if > 0, caps the wall-clock time we'll wait for a
	// backend response between RFQ boundaries. Exceeding it closes the
	// backend (PG detects the FE drop and aborts the query) and sends
	// SQLSTATE 57014 "query timeout" to the client. The client connection
	// itself stays open so the user can retry.
	QueryTimeout time.Duration

	// ClientIdleTimeout, if > 0, closes the client connection after this
	// much wall-clock time with no client message AND no in-flight
	// transaction. Mirrors PgBouncer client_idle_timeout. 0 = disabled.
	ClientIdleTimeout time.Duration

	// IdleTxTimeout, if > 0, closes the client connection after this
	// much wall-clock time INSIDE a transaction with no client message.
	// Mirrors PgBouncer idle_transaction_timeout. 0 = disabled.
	IdleTxTimeout time.Duration

	// LogSQL is one of: "off" | "redacted" | "full". Controls the
	// per-query log emission. Empty defaults to "redacted".
	LogSQL string

	// SlowQuery, if > 0, emits a WARN log line for every query whose
	// duration exceeds this threshold. SQL is rendered through the
	// LogSQL mode (off/redacted/full).
	SlowQuery time.Duration

	// Audit is the optional per-query audit log sink. nil = off.
	Audit *AuditWriter

	// ReplicaPicker, if non-nil, returns a replica pool to acquire
	// READ-classified queries from. Returns nil to fall back to the
	// primary Pool. Called per-acquire when state is idle (mid-tx
	// reads stay on the currently-attached backend).
	ReplicaPicker func() *pool.Pool

	// StickyReadWindowFn, if non-nil, returns the sticky-read window
	// at the moment a routing decision is made. We call it per-message
	// (cheap closure) rather than capturing the value at PooledConn
	// construction time, so SIGHUP'd config changes apply to already
	// connected clients without requiring them to reconnect.
	// nil-returning-0 disables sticky-read for that client.
	StickyReadWindowFn func() time.Duration

	// PrimaryHealthy reports the current health of the primary backing
	// this conn's database. When false, new writes get 08006
	// connection_failure (failover state). Reads route to replicas via
	// ReplicaPicker. nil → always healthy.
	PrimaryHealthy func() bool

	// ReqID is the connection-scoped request ID (stamped into log lines
	// + audit records). Set by the dispatcher.
	ReqID string

	// QPSLimiter, if non-nil, is the shared per-(db, user) token bucket
	// consulted before forwarding each Query/Parse. Empty bucket →
	// reject with SQLSTATE 53300 ("too_many_connections" — closest
	// canonical code for transient overload).
	QPSLimiter *util.TokenBucket

	// PoolMode is one of: "session" | "transaction" | "statement". Empty
	// defaults to "transaction" (MVP default). In statement mode the
	// backend is released after EVERY ReadyForQuery — even ones with
	// TxStatus 'T' — and explicit BEGIN / START TRANSACTION statements
	// are rejected with SQLSTATE 25001 before reaching a backend.
	// "session" is treated as "force session-pin from the first message"
	// for clients that need it; the existing session-pin path covers
	// LISTEN / advisory_lock / temp table / cursor.
	PoolMode string

	// resetOnReleaseSet is true once a caller has explicitly written to
	// ResetOnRelease (including via the zero value of the bool — but
	// most production paths go via NewPooledConn which sets this).
	// Internal toggle; not exported.
	resetOnReleaseSet bool
}

// NewPooledConn returns a PooledConn with production defaults applied:
// ResetOnRelease=true. Use this from cmd/pgrouter and any orchestration
// code; direct struct literals are fine for tests that want to opt out.
//
// Database/User/timeouts are zero-valued — set them on the returned
// struct or use the dispatcher's wiring path (PooledHandler.servePooled)
// which fills them from config + StartupMessage.
func NewPooledConn(log *slog.Logger, p *pool.Pool, cannedParams map[string]string) *PooledConn {
	return &PooledConn{
		Log:               log,
		Pool:              p,
		CannedParams:      cannedParams,
		ResetOnRelease:    true,
		resetOnReleaseSet: true,
	}
}

// Serve runs the pooled handler against an already-authenticated client.
// Caller is responsible for the startup handshake + auth before invoking.
//
// The function returns when the client disconnects, the ctx is done,
// or an unrecoverable error occurs.
func (h *PooledConn) Serve(ctx context.Context, conn net.Conn) error {
	defer conn.Close()
	log := h.Log.With("remote", conn.RemoteAddr().String())

	be := pgproto3.NewBackend(conn, conn)
	clientSide := proto.WrapClientBackend(be)

	// 1. Send the synthetic welcome. May trigger an eager-warm dial so
	//    the ParameterStatus values reflect the real upstream.
	if err := h.sendWelcome(ctx, be); err != nil {
		return fmt.Errorf("welcome: %w", err)
	}
	log.Info("pooled client ready")

	state := NewClientState()
	gucCache := NewGUCCache()
	prepCache := NewPrepareCache()

	// lastSQL captures the SQL text of the most recent Query/Parse so
	// the drain loop can emit a slow_query WARN annotated with it.
	// Reset on every received Query/Parse.
	var lastSQL, lastKind, lastPrepName string
	// curSpan is the active per-query OTel span (no-op when tracing
	// isn't configured). Started on Query/Parse receipt, ended at RFQ.
	var curSpan trace.Span
	// lastWriteAt is the wall-clock timestamp of the most recent
	// observed Write-classified message; used by StickyReadWindow to
	// pin follow-up reads to the primary.
	var lastWriteAt time.Time

	// First synthetic RFQ → mark client idle.
	state.ObserveBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	var bConn *backend.Conn
	// bConnPool tracks which pool bConn was acquired from so Release
	// goes back to the same pool (replica routing → replica's pool).
	var bConnPool *pool.Pool
	// `sessionPinned` flips on once we see LISTEN / advisory_lock / temp
	// table / cursor — after which the backend stays attached for the
	// rest of the client's session.
	sessionPinned := false

	// Pending CloseComplete frames we injected via prepared-cache
	// evictions. The drain loop filters them out so the client never
	// sees a CloseComplete it didn't ask for.
	pendingEvictCloseCompletes := 0

	defer func() {
		if bConn != nil {
			// On client disconnect we always release. Reset is best-effort
			// per the user's ResetOnRelease config.
			releasePool(bConnPool, h.Pool).Release(bConn, h.ResetOnRelease)
			bConnPool = nil
		}
	}()

	for {
		// Honour cancellation between messages.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Apply idle-side deadline based on current tx state. We re-arm
		// on every iteration: a fresh message clears the previous
		// deadline; the deadline only fires while we're blocked here in
		// Receive (which is exactly "client is idle from our POV").
		h.applyIdleDeadline(conn, state)

		msg, err := clientSide.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Debug("client EOF")
				return nil
			}
			if isTimeoutErr(err) {
				inTx := !state.Tx().IsIdle()
				which := "client_idle_timeout"
				code := "57P05"
				if inTx {
					which = "idle_transaction_timeout"
					code = "25P03"
					stats.OnIdleTxTimeout(h.Database, h.User, h.App)
				} else {
					stats.OnClientIdleTimeout(h.Database, h.User, h.App)
				}
				log.Info("client closed by timeout", "kind", which)
				// Short write deadline so a wedged client can't keep
				// the Serve goroutine alive forever.
				h.sendFatalErrorWithWriteDeadline(be, conn, code,
					"pgrouter: "+which, 200*time.Millisecond)
				return nil
			}
			return fmt.Errorf("client recv: %w", err)
		}

		// Clear the read deadline so subsequent in-loop reads aren't
		// constrained by the idle limits.
		_ = conn.SetReadDeadline(time.Time{})

		// Terminate: tear down without a final round trip.
		if proto.IsTerminate(msg) {
			log.Info("client sent Terminate")
			return nil
		}

		state.ObserveClientMessage(msg)
		h.observeClientMessage(msg, gucCache, prepCache, &sessionPinned, log)
		// Unrecognized SET (outside the GUC replay whitelist) forces
		// session-pin: replaying an unknown variable across backends
		// would be incorrect, so we hold the current backend instead.
		if !sessionPinned && gucCache.HasUnrecognizedSet() {
			sessionPinned = true
			log.Info("session pinned (force-session)",
				"reason", "SET of GUC outside replayable whitelist")
		}
		// Statement-mode: reject explicit transaction openers. The
		// client gets a clean error and the connection stays up; PG
		// itself never sees the offending message.
		if h.isStatementMode() {
			if sql, isBegin := clientExplicitBegin(msg); isBegin {
				log.Info("statement_mode: rejecting explicit BEGIN",
					"sql_prefix", truncate(sql, 64))
				h.sendErrorWithRFQ(be, "25001",
					"pgrouter: explicit transactions are not allowed in statement-mode pool")
				continue
			}
		}
		// Per-(db, user) Query/Parse counter + slow-query stash +
		// SQL classification (cached for the rest of the iteration so
		// ClassifySQL doesn't run multiple times per message).
		//
		// curSQLOp + curROBegin flow into the lazy-acquire / routing
		// decision below and the lastWriteAt stamp; isReadMessage no
		// longer re-classifies.
		var curSQLOp SQLOp
		var curROBegin bool
		if sql, kind, prepName, ok := extractClientQuery(msg); ok {
			if !h.takeQPS() {
				log.Info("qps_limit: rejected", "kind", kind)
				h.sendErrorWithRFQ(be, "53300",
					fmt.Sprintf("pgrouter: per-tenant QPS cap exceeded (db=%s user=%s)",
						h.Database, h.User))
				continue
			}
			stats.OnQuery(h.Database, h.User, h.App)
			lastSQL, lastKind, lastPrepName = sql, kind, prepName
			curSpan = h.startQuerySpan(ctx, kind, sql, prepName)
			curSQLOp = ClassifySQL(sql)
			curROBegin = IsExplicitReadOnlyBeginSQL(sql)
			if curSQLOp == SQLOpWrite && !curROBegin {
				lastWriteAt = time.Now()
			}
		}

		// Acquire a backend lazily on the first traffic-generating message.
		needsBackend := messageNeedsBackend(msg)
		if needsBackend && bConn == nil {
			// Replica routing: at the start of a fresh tx, if the
			// triggering message is a Read AND a replica is available
			// AND no session-pin AND we are NOT inside the
			// read-your-own-writes sticky window, route to the replica
			// pool. Otherwise the primary pool wins.
			acquirePool := h.Pool
			// Read routing: SELECT/etc. OR an explicit READ ONLY tx.
			// curSQLOp / curROBegin were cached in the switch above
			// so we don't re-run ClassifySQL.
			isRead := curSQLOp == SQLOpRead || curROBegin
			if !sessionPinned && h.ReplicaPicker != nil &&
				isRead && !h.stickyToPrimary(lastWriteAt) {
				if rp := h.ReplicaPicker(); rp != nil {
					acquirePool = rp
				}
			}
			// Failover gate: when primary is down + we're about to
			// hit it, fail-fast with 08006 instead of blocking on
			// dial retries. Reads that already routed to a replica
			// (acquirePool != h.Pool) bypass this.
			if acquirePool == h.Pool && h.PrimaryHealthy != nil && !h.PrimaryHealthy() {
				log.Info("failover: rejecting write — primary unhealthy",
					"db", h.Database)
				h.sendErrorWithRFQ(be, "08006",
					fmt.Sprintf("pgrouter: primary for %q is unhealthy (failover); retry later", h.Database))
				continue
			}
			bConn, err = acquirePool.Acquire(ctx)
			if err != nil {
				h.sendFatalError(be, "08006",
					fmt.Sprintf("pgrouter: cannot acquire backend: %v", err))
				return err
			}
			bConnPool = acquirePool
			log.Debug("backend acquired", "backend_pid", bConn.PostgresPID)

			// Replay tracked GUCs on the fresh backend BEFORE the
			// client's message hits it. Skip if cache is empty (the
			// common case).
			if replay := gucCache.ReplayQuery(); replay != "" {
				if err := h.fireReplay(bConn, replay); err != nil {
					log.Warn("guc replay failed; treating backend as bad",
						"err", err)
					// Defensively discard the backend.
					_ = bConn.Close()
					bConn = nil
					h.sendFatalError(be, "57P03",
						fmt.Sprintf("pgrouter: backend replay failed: %v", err))
					return err
				}
			}
		}

		if bConn != nil {
			// Prepared-statement interception: cache-hit Parses are
			// synthesized locally (no backend round trip), Bind/Describe/
			// Close('S') get rewritten from client_name → pgr_<hash>.
			// suppressForward=true means we already emitted the
			// equivalent response to the client; skip forwarding.
			//
			// pendingEvictCloseCompletes counts CloseComplete frames the
			// backend will emit in response to LRU-evictions we injected;
			// we filter those out of the drain so the client doesn't see
			// CloseComplete it never requested.
			forwardMsg, suppressForward, err := h.prepareInterceptForward(
				msg, prepCache, bConn, be, &pendingEvictCloseCompletes, log)
			if err != nil {
				return err
			}

			// Forward client → server (unless intercept synthesized the
			// reply already).
			if !suppressForward {
				bConn.Frontend.Send(forwardMsg)
				if err := bConn.Frontend.Flush(); err != nil {
					return fmt.Errorf("server send: %w", err)
				}
			}

			// In extended-protocol mode the backend only emits responses
			// after Sync (or CopyDone/CopyFail at the end of a COPY) —
			// so draining is ONLY safe to do then. For simple Query and
			// these few sync-like messages we drain to the next stable
			// state. Otherwise loop back to receive the next client
			// message; backend responses are queued and drained when
			// Sync arrives.
			if !triggersBackendDrain(msg) {
				continue
			}

			// query_timeout: arm a read deadline on the backend socket
			// while we wait for ReadyForQuery. Clear on RFQ.
			queryStart := time.Now()
			if h.QueryTimeout > 0 {
				_ = bConn.NetConn.SetReadDeadline(time.Now().Add(h.QueryTimeout))
			}

			// Drain backend response up to ReadyForQuery (or CopyInResponse
			// — see drainReason).
			queryTimedOut := false
			for {
				bmsg, err := bConn.Frontend.Receive()
				if err != nil {
					if isTimeoutErr(err) && h.QueryTimeout > 0 {
						queryTimedOut = true
						stats.OnQueryTimeout(h.Database, h.User, h.App)
						log.Info("query_timeout fired; closing backend",
							"timeout", h.QueryTimeout,
						)
						break
					}
					return fmt.Errorf("server recv: %w", err)
				}
				// Filter out CloseComplete frames produced by our LRU
				// evictions — the client never asked for them.
				if _, isCC := bmsg.(*pgproto3.CloseComplete); isCC && pendingEvictCloseCompletes > 0 {
					pendingEvictCloseCompletes--
					continue
				}
				be.Send(bmsg.(pgproto3.BackendMessage))
				if err := be.Flush(); err != nil {
					return fmt.Errorf("client send: %w", err)
				}
				// Tx-state transitions → per-(db, user) counters.
				prevTx := state.Tx()
				if state.ObserveBackendMessage(bmsg) {
					newTx := state.Tx()
					switch {
					case prevTx != TxInBlock && prevTx != TxFailed && newTx == TxInBlock:
						stats.OnTxStart(h.Database, h.User, h.App)
					case prevTx == TxFailed && newTx == TxIdle:
						stats.OnTxRollback(h.Database, h.User, h.App)
					case prevTx == TxInBlock && newTx == TxIdle:
						stats.OnTxCommit(h.Database, h.User, h.App)
					}
				}
				// CopyInResponse — backend is now waiting for client
				// CopyData. Stop draining; loop back to receive client.
				if _, ok := bmsg.(*pgproto3.CopyInResponse); ok {
					if h.QueryTimeout > 0 {
						_ = bConn.NetConn.SetReadDeadline(time.Time{})
					}
					break
				}
				if _, ok := proto.IsReadyForQuery(bmsg); ok {
					if h.QueryTimeout > 0 {
						_ = bConn.NetConn.SetReadDeadline(time.Time{})
					}
					queryDur := time.Since(queryStart)
					stats.OnQueryDuration(h.Database, h.User, h.App,
						queryDur.Seconds())
					if h.SlowQuery > 0 && queryDur >= h.SlowQuery {
						log.Warn("slow_query",
							"kind", lastKind,
							"duration", queryDur,
							"threshold", h.SlowQuery,
							"prepared_name", lastPrepName,
							"sql", SQLForLog(h.LogSQL, lastSQL, 512),
						)
					}
					if h.Audit != nil && lastKind != "" {
						h.Audit.Write(h.ReqID, h.Database, h.User, h.App,
							lastKind,
							SQLForLog(h.LogSQL, lastSQL, 1024),
							queryDur)
					}
					if curSpan != nil {
						curSpan.SetAttributes(
							attribute.Float64("pgrouter.duration_ms",
								float64(queryDur.Microseconds())/1000.0))
						curSpan.End()
						curSpan = nil
					}
					// Release whenever the backend reports idle —
					// covers explicit COMMIT/ROLLBACK boundaries AND
					// implicit-transaction queries (e.g. bare SELECT
					// outside BEGIN). PgBouncer's transaction mode
					// behaves identically.
					//
					// EXCEPT when the client has triggered session-pin
					// (LISTEN, advisory_lock, temp table, cursor) —
					// then we hold the backend for the remainder of the
					// session.
					//
					// In statement-mode we release on EVERY RFQ —
					// including ones with TxStatus 'T' — because
					// statement mode by definition forbids
					// cross-statement state on the backend. Explicit
					// BEGIN is already rejected upstream so we'd never
					// observe 'T' in practice, but the guard makes the
					// invariant explicit.
					shouldRelease := !sessionPinned &&
						(h.isStatementMode() || state.Tx().IsIdle())
					if shouldRelease {
						releasePool(bConnPool, h.Pool).Release(bConn, h.ResetOnRelease)
			bConnPool = nil
						bConn = nil
					}
					break
				}
			}
			if queryTimedOut {
				// PG aborts the in-flight query when the FE socket
				// closes; that's sufficient — no separate CancelRequest
				// needed. The backend is now in an unknown state, so
				// close + drop it; the next message will Acquire a
				// fresh one.
				_ = bConn.Close()
				bConn = nil
				stats.OnQueryDuration(h.Database, h.User, h.App,
					time.Since(queryStart).Seconds())
				h.sendFatalErrorWithWriteDeadline(be, conn, "57014",
					fmt.Sprintf("pgrouter: query_timeout (%s) exceeded", h.QueryTimeout),
					200*time.Millisecond)
				// Connection survives; client may issue a new Query.
			}
			continue
		}

		// Backend not needed (e.g. Sync without prior Parse) — synthesize
		// a no-op response.
		if _, ok := msg.(*pgproto3.Sync); ok {
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			_ = be.Flush()
		}
	}
}

// observeClientMessage runs the per-message hooks that drive the GUC +
// prepare caches and the session-pin trigger.
func (h *PooledConn) observeClientMessage(
	msg pgproto3.FrontendMessage,
	gucCache *GUCCache,
	prepCache *PrepareCache,
	sessionPinned *bool,
	log *slog.Logger,
) {
	switch m := msg.(type) {
	case *pgproto3.Query:
		h.logSQL(log, "query", "", m.String)
		gucCache.ObserveQuery(m.String)
		if !*sessionPinned && needsSessionPin(m.String) {
			*sessionPinned = true
			log.Info("session pinned (force-session)",
				"reason", "incompatible feature in Query",
				"sql_prefix", truncate(m.String, 64),
			)
		}
	case *pgproto3.Parse:
		h.logSQL(log, "parse", m.Name, m.Query)
		prepCache.Observe(m.Name, m.Query, m.ParameterOIDs)
		if !*sessionPinned && needsSessionPin(m.Query) {
			*sessionPinned = true
			log.Info("session pinned (force-session)",
				"reason", "incompatible feature in Parse",
				"sql_prefix", truncate(m.Query, 64),
			)
		}
	case *pgproto3.Close:
		// Close('S', name) untracks a prepared statement.
		if m.ObjectType == 'S' {
			prepCache.Close(m.Name)
		}
	}
}

// logSQL emits a structured per-query log line obeying the LogSQL mode.
// kind is "query" (simple) or "parse" (extended). prepName is the
// client-supplied statement name for Parse, "" for Query.
//
// LogSQL=="off" still emits the line — so operators always see request
// flow — but with no `sql` field.
func (h *PooledConn) logSQL(log *slog.Logger, kind, prepName, sql string) {
	mode := h.LogSQL
	if mode == "" {
		mode = "redacted"
	}
	attrs := []any{"kind", kind}
	if prepName != "" {
		attrs = append(attrs, "prepared_name", prepName)
	}
	if rendered := SQLForLog(mode, sql, 256); rendered != "" {
		attrs = append(attrs, "sql", rendered)
	}
	log.Debug("client query", attrs...)
}

// truncate is a no-frills string slicer for log fields.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// fireReplay sends `sql` on the backend and drains the response up to
// ReadyForQuery. Returns an error on backend ErrorResponse — the caller
// should treat the backend as poisoned and discard it.
func (h *PooledConn) fireReplay(bConn *backend.Conn, sql string) error {
	bConn.Frontend.Send(&pgproto3.Query{String: sql})
	if err := bConn.Frontend.Flush(); err != nil {
		return fmt.Errorf("replay flush: %w", err)
	}
	for {
		msg, err := bConn.Frontend.Receive()
		if err != nil {
			return fmt.Errorf("replay recv: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("replay error %s: %s", m.Severity, m.Message)
		case *pgproto3.ReadyForQuery:
			return nil
		default:
			// CommandComplete, RowDescription, etc.: drain
		}
	}
}

// sendWelcome sends AuthOk + ParameterStatus + BackendKeyData +
// ReadyForQuery 'I'. ParameterStatus values come from the pool's
// captured upstream params merged over our canned defaults (real values
// win on collision). PID/secret come from WelcomePID/Secret if set,
// otherwise random one-shot values.
func (h *PooledConn) sendWelcome(ctx context.Context, be *pgproto3.Backend) error {
	be.Send(&pgproto3.AuthenticationOk{})

	params := h.welcomeParams(ctx)
	for k, v := range params {
		be.Send(&pgproto3.ParameterStatus{Name: k, Value: v})
	}

	pid := h.WelcomePID
	sec := h.WelcomeSecret
	if pid == 0 || len(sec) == 0 {
		p, s, err := randomBackendKey()
		if err != nil {
			return err
		}
		pid, sec = p, s
	}
	be.Send(&pgproto3.BackendKeyData{ProcessID: pid, SecretKey: sec})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return be.Flush()
}

// welcomeParams returns the merged ParameterStatus map for the welcome:
//
//	cached (real upstream values) over canned (our defaults).
//
// If the pool has never successfully dialed (cold start), we eagerly
// acquire+release one backend to capture params. If THAT also fails
// (e.g. upstream is down), fall back to the canned defaults so the
// client at least sees a valid pgwire welcome.
func (h *PooledConn) welcomeParams(ctx context.Context) map[string]string {
	cached := h.cachedOrWarm(ctx)
	if len(cached) == 0 {
		return h.CannedParams
	}
	merged := make(map[string]string, len(cached)+len(h.CannedParams))
	for k, v := range h.CannedParams {
		merged[k] = v
	}
	for k, v := range cached {
		merged[k] = v
	}
	return merged
}

// cachedOrWarm returns the pool's cached params if non-empty, otherwise
// performs a one-shot eager Acquire+Release to populate the cache.
//
// We skip the warm if:
//   - h.Pool is nil (some tests), or
//   - the pool reports a previous dial already attempted: either it
//     succeeded and populated the cache, or the upstream emitted no
//     ParameterStatus (in which case repeated warms wouldn't help).
//
// This caps the warm at one attempt per pool ever — keeps welcome
// latency O(RTT) only on the very first client.
func (h *PooledConn) cachedOrWarm(ctx context.Context) map[string]string {
	if h.Pool == nil {
		return nil
	}
	if cached := h.Pool.CachedParams(); len(cached) > 0 {
		return cached
	}
	if h.Pool.DialAttempted() {
		return nil
	}
	c, err := h.Pool.Acquire(ctx)
	if err != nil {
		return nil
	}
	cached := h.Pool.CachedParams()
	h.Pool.Release(c, false)
	return cached
}

// sendFatalError is a thin wrapper around proto.SendFatalError kept
// as a method so the existing call sites stay legible; the actual
// pgwire synth happens in internal/proto/synth.go (reusable from any
// pgrouter component that needs a synthetic FATAL).
func (h *PooledConn) sendFatalError(be *pgproto3.Backend, code, msg string) {
	proto.SendFatalError(be, code, msg)
}

// sendErrorWithRFQ wraps proto.SendErrorRFQ. Used for proxy-level
// rejections (statement-mode BEGIN guard, QPS cap hit, failover
// write reject) where the connection stays usable.
func (h *PooledConn) sendErrorWithRFQ(be *pgproto3.Backend, code, msg string) {
	proto.SendErrorRFQ(be, code, msg)
}

// isStatementMode returns true when the handler is configured for
// statement-mode pooling (release after every RFQ + reject BEGIN).
func (h *PooledConn) isStatementMode() bool {
	return h.PoolMode == "statement"
}

// takeQPS consumes one token from the per-tenant bucket if one is
// configured. Returns true when the request may proceed (or when no
// limiter is set). On reject, fires OnQPSReject.
func (h *PooledConn) takeQPS() bool {
	if h.QPSLimiter == nil {
		return true
	}
	if h.QPSLimiter.Take() {
		return true
	}
	stats.OnQPSReject("db", h.Database)
	stats.OnQPSReject("user", h.User)
	return false
}

// silence unused import linter when only ratelimit type is used via field
var _ = util.NewTokenBucket

// releasePool returns p if non-nil else fallback. Used to pick the
// pool to Release into (acquired-from pool — primary or replica).
func releasePool(p *pool.Pool, fallback *pool.Pool) *pool.Pool {
	if p != nil {
		return p
	}
	return fallback
}

// stickyToPrimary returns true when we should route this client's read
// to the PRIMARY because the sticky-read window hasn't elapsed since
// the last write on this conn. 0 lastWriteAt = no write seen = no
// stickiness.
//
// We re-resolve the window per call via StickyReadWindowFn so a SIGHUP
// reload of the per-db sticky_read_window takes effect on already
// connected clients.
func (h *PooledConn) stickyToPrimary(lastWrite time.Time) bool {
	if h.StickyReadWindowFn == nil || lastWrite.IsZero() {
		return false
	}
	window := h.StickyReadWindowFn()
	if window <= 0 {
		return false
	}
	return time.Since(lastWrite) < window
}

// extractClientQuery returns (sql, kind, prepName, true) for Query
// and Parse messages. kind is "query" or "parse"; prepName is the
// extended-protocol statement name (empty for simple Query).
//
// Other frontend messages → ok=false; Serve skips the per-query
// bookkeeping path entirely.
func extractClientQuery(msg pgproto3.FrontendMessage) (sql, kind, prepName string, ok bool) {
	switch m := msg.(type) {
	case *pgproto3.Query:
		return m.String, "query", "", true
	case *pgproto3.Parse:
		return m.Query, "parse", m.Name, true
	}
	return "", "", "", false
}

// isReadMessage is retained for the existing test surface (#128
// readonly_begin_test.go calls it directly). Serve's hot path uses
// the cached curSQLOp/curROBegin pair to avoid re-running the
// classifier 3× per message.
func isReadMessage(msg pgproto3.FrontendMessage) bool {
	sql, _, _, ok := extractClientQuery(msg)
	if !ok {
		return false
	}
	if IsExplicitReadOnlyBeginSQL(sql) {
		return true
	}
	return ClassifySQL(sql) == SQLOpRead
}

// startQuerySpan opens an OTel span for one Query/Parse. Returns a
// no-op span when tracing isn't configured. SQL is rendered through
// LogSQL mode so PII doesn't leak to traces; trace exporters often
// archive longer than logs.
//
// Cleanup happens in the drain loop's RFQ branch (curSpan.End()).
// On error paths (timeout, backend close) the deferred SetStatus +
// End in the surrounding goroutine handle it.
func (h *PooledConn) startQuerySpan(ctx context.Context, kind, sql, prepName string) trace.Span {
	_, span := tracing.Tracer().Start(ctx, "pgrouter."+kind,
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.name", h.Database),
			attribute.String("db.user", h.User),
			attribute.String("pgrouter.req_id", h.ReqID),
			attribute.String("pgrouter.app", h.App),
			attribute.String("pgrouter.kind", kind),
			attribute.String("pgrouter.prepared_name", prepName),
			attribute.String("db.statement", SQLForLog(h.LogSQL, sql, 512)),
		),
	)
	return span
}

// markSpanFailed sets a span's status to error + an attribute and ends.
// Defensive helper for query_timeout / connection_drop paths.
func (h *PooledConn) markSpanFailed(span trace.Span, code, msg string) {
	if span == nil {
		return
	}
	span.SetStatus(codes.Error, msg)
	span.SetAttributes(attribute.String("pgrouter.error_code", code))
	span.End()
}

// clientExplicitBegin recognises an explicit transaction-open coming
// from the client.
//
// Both simple Query and extended-protocol Parse paths are checked. The
// returned string is the SQL we matched against (for logging); the
// boolean is the verdict.
func clientExplicitBegin(msg pgproto3.FrontendMessage) (string, bool) {
	switch m := msg.(type) {
	case *pgproto3.Query:
		if IsExplicitBeginSQL(m.String) {
			return m.String, true
		}
	case *pgproto3.Parse:
		if IsExplicitBeginSQL(m.Query) {
			return m.Query, true
		}
	}
	return "", false
}

// sendFatalErrorWithWriteDeadline is sendFatalError variant that caps the
// blocking flush time. Used from timeout-driven exit paths where the
// client may have disappeared and we don't want the goroutine to hang.
func (h *PooledConn) sendFatalErrorWithWriteDeadline(
	be *pgproto3.Backend, conn net.Conn, code, msg string, d time.Duration,
) {
	_ = conn.SetWriteDeadline(time.Now().Add(d))
	defer func() { _ = conn.SetWriteDeadline(time.Time{}) }()
	h.sendFatalError(be, code, msg)
}

// messageNeedsBackend returns true if the client message implies we
// must hold a real backend to satisfy it.
//
// Bare Sync without a preceding Parse/Bind/Execute is a no-op the
// proxy can answer itself with ReadyForQuery (matches PgBouncer).
func messageNeedsBackend(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Query,
		*pgproto3.Parse,
		*pgproto3.Bind,
		*pgproto3.Execute,
		*pgproto3.Describe,
		*pgproto3.Close,
		*pgproto3.CopyData,
		*pgproto3.CopyDone,
		*pgproto3.CopyFail,
		*pgproto3.Flush:
		return true
	case *pgproto3.Sync, *pgproto3.Terminate:
		return false
	default:
		// Safe default: forward to a backend rather than synthesize.
		return true
	}
}

// applyIdleDeadline sets a SetReadDeadline on `conn` based on the
// current tx state + the handler's two idle limits:
//
//	state.Tx() == 'I' → ClientIdleTimeout (PgBouncer client_idle_timeout)
//	state.Tx() == 'T' or 'E' → IdleTxTimeout (idle_transaction_timeout)
//
// 0 (disabled) → clear any prior deadline. The deadline is re-armed on
// every Serve-loop iteration so a fresh client message keeps the
// connection alive.
func (h *PooledConn) applyIdleDeadline(conn net.Conn, state *ClientState) {
	if h.ClientIdleTimeout <= 0 && h.IdleTxTimeout <= 0 {
		_ = conn.SetReadDeadline(time.Time{})
		return
	}
	var d time.Duration
	if state.Tx().IsIdle() {
		d = h.ClientIdleTimeout
	} else {
		d = h.IdleTxTimeout
	}
	if d <= 0 {
		_ = conn.SetReadDeadline(time.Time{})
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(d))
}

// isTimeoutErr reports whether err is a deadline / i/o-timeout error.
// Both net.Error.Timeout() and errors.Is(os.ErrDeadlineExceeded) cover
// the wrapped variants pgproto3 produces.
func isTimeoutErr(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}

// triggersBackendDrain returns true if `msg` is the kind of frontend
// message that should cause us to read backend responses until we hit a
// stable state (RFQ or CopyInResponse).
//
// In extended-protocol mode the backend buffers Parse/Bind/Describe/
// Execute responses until Sync — only then does it flush. So draining
// after a non-sync frontend message would block forever. Other code
// paths (simple Query, end-of-COPY) DO trigger backend responses
// immediately, so drain after those.
func triggersBackendDrain(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Query, *pgproto3.Sync,
		*pgproto3.CopyDone, *pgproto3.CopyFail:
		return true
	}
	return false
}

// prepareInterceptForward implements the cross-backend prepared-stmt
// cache (M.11.2/M.11.3). It is called for every client frontend
// message that has a backend attached.
//
// Returns (forwardMsg, suppress, err):
//
//   - suppress=true: the call has already emitted the equivalent response
//     to the client; the Serve loop must NOT forward to the backend.
//   - suppress=false + forwardMsg=msg: pass-through, forward as-is.
//   - suppress=false + forwardMsg!=msg: rewritten variant, forward instead.
//
// Side effects:
//   - Parse records {client-name → server-name=pgr_<hash(sql)>} in the
//     per-client PrepareCache.
//   - Bind, Describe('S'), Close('S') rewrite their statement-name
//     field to the cached server-name.
//   - Close('S') is SUPPRESSED — we keep the statement on the backend
//     for the next client (pgcat-style cross-client reuse). The client
//     gets a synthesized CloseComplete immediately.
//   - On Parse cache hit we synthesize ParseComplete to the client.
//   - On Parse cache miss with LRU pressure we inject Close('S',
//     evictedName) into the backend stream; the resulting CloseComplete
//     is filtered out by the drain loop via *pendingEvictCloseCompletes.
//
// Unnamed prepared statements (Name="") bypass the whole cache and
// pass through unchanged — they're meant to be one-shot.
//
// If bConn.Prepared is nil the cache is disabled; all messages pass
// through unmodified except Bind/Describe/Close which still get
// rewritten (in case Parse was rewritten earlier on this client).
func (h *PooledConn) prepareInterceptForward(
	msg pgproto3.FrontendMessage,
	clientPrep *PrepareCache,
	bConn *backend.Conn,
	be *pgproto3.Backend,
	pendingEvictCloseCompletes *int,
	log *slog.Logger,
) (pgproto3.FrontendMessage, bool, error) {
	switch m := msg.(type) {
	case *pgproto3.Parse:
		if m.Name == "" {
			return msg, false, nil
		}
		// observeClientMessage already called Observe; that populates
		// the ServerName field. We re-derive here defensively in case
		// the caller bypassed observeClientMessage.
		server := clientPrep.ServerNameOf(m.Name)
		if server == "" {
			server = ServerNameFor(m.Query)
			clientPrep.Observe(m.Name, m.Query, m.ParameterOIDs)
		}
		if bConn.Prepared != nil && bConn.Prepared.Has(server) {
			// CACHE HIT — backend already has this Parse; synthesize
			// ParseComplete for the client and skip the round trip.
			bConn.Prepared.Touch(server)
			stats.OnPreparedHit(h.Database, h.User, h.App)
			be.Send(&pgproto3.ParseComplete{})
			return nil, true, nil
		}
		// CACHE MISS — rewrite Name and forward.
		stats.OnPreparedMiss(h.Database, h.User, h.App)
		if bConn.Prepared != nil {
			if evicted := bConn.Prepared.Add(server); evicted != "" {
				// LRU pushed an entry out. Tell the backend to drop the
				// old prepared statement via an extended-protocol
				// Close('S', evicted) so the planner reclaims memory.
				// The CloseComplete that comes back is filtered out in
				// the next drain via pendingEvictCloseCompletes.
				bConn.Frontend.Send(&pgproto3.Close{
					ObjectType: 'S',
					Name:       evicted,
				})
				*pendingEvictCloseCompletes++
				stats.OnPreparedEviction(h.Database, h.User, h.App)
				log.Debug("prepared cache LRU eviction",
					"evicted", evicted, "incoming", server)
			}
		}
		// Shallow copy so we don't mutate the caller's struct (msg
		// might still be referenced by the pgproto3 read buffer
		// internals; pooled-buffer corruption is a known pitfall).
		out := *m
		out.Name = server
		return &out, false, nil

	case *pgproto3.Bind:
		if m.PreparedStatement == "" {
			return msg, false, nil
		}
		server := clientPrep.ServerNameOf(m.PreparedStatement)
		if server == "" {
			return msg, false, nil
		}
		out := *m
		out.PreparedStatement = server
		return &out, false, nil

	case *pgproto3.Describe:
		// Describe('S', name) inspects a prepared statement.
		// Describe('P', name) inspects a portal — pass through.
		if m.ObjectType != 'S' || m.Name == "" {
			return msg, false, nil
		}
		server := clientPrep.ServerNameOf(m.Name)
		if server == "" {
			return msg, false, nil
		}
		out := *m
		out.Name = server
		return &out, false, nil

	case *pgproto3.Close:
		// Close('S', name) — suppress: keep statement on backend for
		// the next client. Synthesize CloseComplete locally.
		// Close('P', name) — closes a portal; pass through.
		if m.ObjectType != 'S' || m.Name == "" {
			return msg, false, nil
		}
		clientPrep.Close(m.Name)
		be.Send(&pgproto3.CloseComplete{})
		return nil, true, nil
	}
	return msg, false, nil
}

// randomBackendKey is a copy of the helper in conn.go; kept here so
// pooled.go is independently testable.
func randomBackendKey() (uint32, []byte, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, nil, err
	}
	pid := binary.BigEndian.Uint32(buf[0:4])
	if pid == 0 {
		pid = 1
	}
	sec := make([]byte, 4)
	copy(sec, buf[4:8])
	return pid, sec, nil
}
