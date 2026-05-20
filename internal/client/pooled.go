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
	"github.com/JustAnotherDevv/pgrouter/internal/wire/rawconn"
	"github.com/JustAnotherDevv/pgrouter/internal/wire/splice"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// PooledConfig is the immutable per-handler configuration shared
// across every PooledConn served from a single PooledHandler.
//
// Extracted from PooledConn during the AL1 refactor: previously the
// 21+ fields lived directly on PooledConn, conflating per-conn state
// (Database, User, ReqID, callbacks) with shared config. Now Config
// is one struct that PooledHandler can store + clone into each
// PooledConn at construction time.
//
// Field access from inside PooledConn methods is unchanged thanks to
// Go's field promotion: h.LogSQL still works as before.
type PooledConfig struct {
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

	// PoolMode is one of: "session" | "transaction" | "statement". Empty
	// defaults to "transaction" (MVP default). In statement mode the
	// backend is released after EVERY ReadyForQuery — even ones with
	// TxStatus 'T' — and explicit BEGIN / START TRANSACTION statements
	// are rejected with SQLSTATE 25001 before reaching a backend.
	// "session" is treated as "force session-pin from the first message"
	// for clients that need it; the existing session-pin path covers
	// LISTEN / advisory_lock / temp table / cursor.
	PoolMode string

	// Hooks are user-provided callbacks invoked once per completed
	// Query/Parse, AFTER the built-in stats + slow_query + audit + OTel
	// sinks have run. See QueryHook in query_event.go. Empty by default.
	//
	// Hooks must be cheap; a slow hook blocks the per-conn drain. For
	// external dispatch, push into a buffered channel and drain
	// elsewhere.
	Hooks []QueryHook

	// Splice tunes the splice forwarder for the backend→client drain
	// path. nil = disabled (use the pgproto3 decode/re-encode path for
	// every message). When set with Enabled=true, the drain loop
	// bypasses pgproto3 for "boring" messages (DataRow, RowDescription,
	// CommandComplete, ParseComplete, BindComplete, NoData, EmptyQuery,
	// PortalSuspended) and only switches to the decoded path for
	// messages that need observation (ParameterStatus, BackendKeyData,
	// errors, RFQ, Copy*, etc).
	//
	// Phase A optimization; see internal/wire/splice/splice.go for
	// the classification table + drain loop.
	Splice *splice.SpliceConfig

	// PreparedCache enables the cross-backend prepared-statement
	// cache. When false, the per-client PrepareCache is left nil and
	// the per-message interception + rewrite is skipped entirely —
	// Parse/Bind/Close pass through to the backend with the client's
	// original names. The per-backend LRU is also left unallocated.
	//
	// Default true. Disable on workloads dominated by unnamed
	// extended, simple Query, or one-shot Parse/Bind pairs where the
	// per-Parse hash + map lookup overhead outweighs the cache-hit
	// savings. Mirrors cfg.Wire.PreparedCache.
	PreparedCache bool

	// RawPassthrough, when true, bypasses pgproto3 for client→backend
	// message reading. Instead of decoding each frontend message into a
	// Go struct and re-encoding it for the backend, raw bytes are read
	// directly from the client socket and forwarded to the backend. Only
	// Query and Parse messages have their SQL extracted for GUC/pin/
	// classification — everything else is pure passthrough.
	//
	// This eliminates per-message struct allocations and encode/decode
	// overhead on the client→backend hot path. Backend→client splice
	// (Phase A) continues to work independently.
	//
	// Trade-off: prepared-cache interception is disabled when raw
	// passthrough is active (messages can't be rewritten without decode).
	RawPassthrough bool
}

// PooledConn is a transaction-mode pooled handler for one client.
//
// AL1 refactor: shared/immutable config lives on the embedded
// PooledConfig; per-conn identity + routing state lives directly here.
// Field promotion keeps existing call sites + struct literals working
// for the embedded fields without the `Config: PooledConfig{...}`
// wrap (struct literal assignment to embedded fields by name was
// preserved in the migration).
type PooledConn struct {
	// PooledConfig holds the shared/immutable config copied from the
	// owning PooledHandler. Read-only after Serve starts.
	PooledConfig

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

	// WelcomePID + WelcomeSecret, if non-zero, are emitted in the
	// BackendKeyData portion of the welcome message. Callers wire the
	// cancel.Tracker here so subsequent CancelRequest packets can be
	// routed back to this client's currently-attached backend. Zero
	// values cause a one-shot random key to be generated locally.
	WelcomePID    uint32
	WelcomeSecret []byte

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

	// resetOnReleaseSet is true once a caller has explicitly written to
	// ResetOnRelease (including via the zero value of the bool — but
	// most production paths go via NewPooledConn which sets this).
	// Internal toggle; not exported.
	resetOnReleaseSet bool

	// counters caches the per-(db, user, app) Prometheus handles so the
	// per-message stats path skips the WithLabelValues mutex + map
	// lookup. Lazy-initialised on first Serve so tests that build a
	// PooledConn struct literal don't have to populate it.
	counters *stats.TenantCounters

	// bConnSpliceReader is the *splice.RawReader that wraps the
	// bConn.Frontend's internal chunkReader (via reflection). Both
	// bConn.Frontend.Receive and splice.DrainSplice read from the
	// SAME chunkReader, so byte positions stay consistent and the
	// chunkReader's over-read buf is shared. Set when a backend is
	// acquired and Splice is enabled; nil when no backend is attached
	// or Splice is disabled. See setupSplice.
	bConnSpliceReader *splice.RawReader

	// hasIdleDeadline is true when either ClientIdleTimeout or
	// IdleTxTimeout is configured. When false, the two per-message
	// SetReadDeadline syscalls are eliminated entirely.
	hasIdleDeadline bool

	// pendingEvictCC is a reusable counter for filtering
	// CloseComplete frames during drain. Previously allocated via
	// new(int) per drain cycle; now a field on PooledConn to avoid
	// per-query heap allocation.
	pendingEvictCC int
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
		PooledConfig: PooledConfig{
			CannedParams:   cannedParams,
			ResetOnRelease: true,
		},
		Log:               log,
		Pool:              p,
		resetOnReleaseSet: true,
	}
}

// setupSplice wires the splice forwarder onto a freshly acquired
// backend. The key insight: the splice drain loop and the caller's
// bConn.Frontend.Receive() must SHARE the pgproto3 chunkReader so that
// bytes the chunkReader over-reads into its 8KB buf are visible to both
// readers — otherwise DrainSplice blocks on bytes that Frontend has
// already consumed.
//
// The wiring is:
//   - bConn.Frontend is reconstructed with bConn.NetConn (the raw
//     backend conn) as its reader. Internally pgproto3 builds a
//     chunkReader that reads from bConn.NetConn.
//   - splice.RawReader (reflection) reaches into that chunkReader so
//     DrainSplice reads from the SAME chunkReader.
//   - splice.PutbackReader wraps RawReader with a 5-byte putback
//     buffer; DrainSplice uses PutbackReader.Putback to "unread" the
//     header of a non-boring message. Subsequent reads (from either
//     DrainSplice or Frontend.Receive) flow through the putback buffer
//     first, then through chunkReader.
//
// Safe to call when Splice is nil or disabled — it's a no-op in that
// case (and clears any stale PutbackReader). The previous bConn.Frontend
// is left intact for callers that still reference it.
//
// PooledConn owners MUST call this every time a NEW bConn is acquired
// (not on every drain call), because the new PutbackReader needs an
// empty putback state and a matching bConn.Frontend. For transaction
// mode this is "every query"; for session-pinned mode it's "the first
// acquire" and we re-use the PutbackReader for the session.
func (h *PooledConn) setupSplice(bConn *backend.Conn) {
	if h.Splice == nil || !h.Splice.Enabled || bConn == nil {
		h.bConnSpliceReader = nil
		return
	}
	// Replace bConn.Frontend so its chunkReader reads from bConn.NetConn
	// directly. We will then reach into that chunkReader via reflection.
	bConn.Frontend = pgproto3.NewFrontend(bConn.NetConn, bConn.NetConn)
	raw, err := splice.NewRawReader(bConn.Frontend)
	if err != nil {
		// Fall back to non-splice: log a warning and leave Frontend
		// with the original reader. Better to log+regress than crash.
		if h.Log != nil {
			h.Log.Warn("splice: reflection init failed; splice disabled for this conn", "err", err)
		}
		h.bConnSpliceReader = nil
		return
	}
	h.bConnSpliceReader = raw
}

// spliceBufSize returns the configured splice buffer size, with a sane
// fallback to the wire package's default when the config is missing.
func spliceBufSize(cfg *splice.SpliceConfig) int {
	if cfg == nil || cfg.BufferSize <= 0 {
		return splice.DefaultSpliceConfig().BufferSize
	}
	return cfg.BufferSize
}

// Serve runs the pooled handler against an already-authenticated client.
// Caller is responsible for the startup handshake + auth before invoking.
//
// The function returns when the client disconnects, the ctx is done,
// or an unrecoverable error occurs.
func (h *PooledConn) Serve(ctx context.Context, conn net.Conn) error {
	defer conn.Close()
	log := h.Log.With("remote", conn.RemoteAddr().String())

	// Resolve per-tenant Prometheus handles ONCE. Database/User/App are
	// fixed for the lifetime of this conn; per-message WithLabelValues
	// lookups are pure overhead under high QPS. Safe when Active==nil
	// (returns nil; all methods are no-ops).
	if h.counters == nil {
		// SanitizeAppName caps the client-controlled label cardinality
		// to prevent a Prometheus head-block blowup from hostile
		// StartupMessage values (HB2).
		h.counters = stats.NewTenantCounters(h.Database, h.User,
			stats.SanitizeAppName(h.App))
	}

	// Raw passthrough: bypass pgproto3 for client→backend hot path.
	if h.RawPassthrough {
		return h.serveRaw(ctx, conn)
	}
	// Cache whether idle timeouts are active so the per-message
	// applyIdleDeadline can skip 2 SetReadDeadline syscalls when not
	// configured.
	h.hasIdleDeadline = h.ClientIdleTimeout > 0 || h.IdleTxTimeout > 0

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
	// prepCache is the per-client (clientName → serverName) map for the
	// cross-backend prepared-statement cache. Starts nil and is lazy-
	// allocated on the first Parse message. When PreparedCache=false,
	// it stays nil permanently — the per-message intercept and the
	// observeClientMessage prepCache calls then become no-ops, so the
	// cache overhead (per-Parse hash + RWMutex + map) is paid zero.
	//
	// Lazy init: connections that never Parse (storm, contention,
	// simple-query-only) skip the allocation entirely, eliminating
	// GC pressure from rapid connection cycling.
	var prepCache *PrepareCache
	if h.PreparedCache {
		prepCache = NewPrepareCache()
	}

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
		// End any in-flight OTel span: error paths (Acquire fail, drain
		// error, client recv error) return before onQueryComplete runs,
		// so without this the span lives until GC with no telemetry End.
		// Trace shows "hanging forever" in Jaeger/Tempo otherwise.
		if curSpan != nil {
			h.markSpanFailed(curSpan, "PGROUTER_ABORTED",
				"serve loop aborted before query completion")
			curSpan = nil
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
					h.counters.OnIdleTxTimeout()
				} else {
					h.counters.OnClientIdleTimeout()
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
		if h.hasIdleDeadline {
			_ = conn.SetReadDeadline(time.Time{})
		}

		// Terminate: tear down without a final round trip.
		if proto.IsTerminate(msg) {
			log.Info("client sent Terminate")
			return nil
		}

		state.ObserveClientMessage(msg)
		// Compute SQL analysis once per message. Used by observeClientMessage
		// (GUC + pin check) and by classification below.
		var sqlInfo *SQLInfo
		if q, ok := msg.(*pgproto3.Query); ok {
			info := AnalyzeSQL(q.String)
			sqlInfo = &info
		} else if p, ok := msg.(*pgproto3.Parse); ok {
			info := AnalyzeSQL(p.Query)
			sqlInfo = &info
		}
		h.observeClientMessage(msg, gucCache, prepCache, &sessionPinned, log, sqlInfo)
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
			h.counters.OnQuery()
			lastSQL, lastKind, lastPrepName = sql, kind, prepName
			// End any prior span before overwriting. Extended-protocol
			// clients can send Parse-Parse without an intervening Sync
			// (drain never fires); without this the first span never
			// ends.
			if curSpan != nil {
				h.markSpanFailed(curSpan, "PGROUTER_SUPERSEDED",
					"replaced by next Query/Parse before drain")
			}
			curSpan = h.startQuerySpan(ctx, kind, sql, prepName)
			// Use pre-computed SQLInfo when available (eliminates
			// redundant keyword extraction + regex from ClassifyDetail).
			if sqlInfo != nil {
				curSQLOp = sqlInfo.Op
				curROBegin = sqlInfo.IsROBegin
			} else {
				curSQLOp, curROBegin = ClassifyDetail(sql)
			}
			if curSQLOp == SQLOpWrite && !curROBegin {
				lastWriteAt = time.Now()
			}
		}

		// Acquire a backend lazily on the first traffic-generating message.
		needsBackend := messageNeedsBackend(msg)
		if needsBackend && bConn == nil {
			acquirePool := h.selectPoolForMsg(sessionPinned,
				curSQLOp, curROBegin, lastWriteAt)
			// Failover gate: when primary is down + we're about to
			// hit it, fail-fast with 08006 instead of blocking on
			// dial retries. Reads that already routed to a replica
			// (acquirePool != h.Pool) bypass this.
			if acquirePool == h.Pool && h.PrimaryHealthy != nil && !h.PrimaryHealthy() {
				log.Info("failover: rejecting write — primary unhealthy",
					"db", h.Database)
				h.sendErrorWithRFQ(be, "08006",
					fmt.Sprintf("pgrouter: primary for %q is unhealthy (failover); retry later", h.Database))
				// End the span we just opened for this rejected query —
				// otherwise sustained failover rejections leak spans.
				if curSpan != nil {
					h.markSpanFailed(curSpan, "08006", "primary unhealthy (failover)")
					curSpan = nil
				}
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

			// Phase A: wire splice forwarder onto the freshly acquired
			// backend. The new bConn.Frontend reads through the
			// PutbackReader so splice's putback is consumed by
			// Receive() next iteration.
			h.setupSplice(bConn)

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
			//
			// Skipped entirely when prepCache is nil (cfg.Wire.PreparedCache=false)
			// — the message goes straight to the backend unchanged.
			forwardMsg := msg
			suppressForward := false
			if prepCache != nil {
				var err error
				forwardMsg, suppressForward, err = h.prepareInterceptForward(
					msg, prepCache, bConn, be, &pendingEvictCloseCompletes, log)
				if err != nil {
					return err
				}
			}

			// Forward client → server (unless intercept synthesized the
			// reply already). Send() buffers; we only Flush() when
			// triggersBackendDrain is true (Sync/Query/CopyDone/CopyFail)
			// so Parse/Bind/Execute/Describe/Close are batched into a
			// single write — reducing syscalls from 4 to 1 per extended
			// query.
			if !suppressForward {
				bConn.Frontend.Send(forwardMsg)
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

			// Flush all buffered client→backend messages (Parse/Bind/Execute
			// may have been buffered above) before draining backend responses.
			if err := bConn.Frontend.Flush(); err != nil {
				return fmt.Errorf("server send: %w", err)
			}

			// query_timeout: arm a read deadline on the backend socket
			// while we wait for ReadyForQuery. Clear on RFQ.
			queryStart := time.Now()
			if h.QueryTimeout > 0 {
				_ = bConn.NetConn.SetReadDeadline(time.Now().Add(h.QueryTimeout))
			}

			out, updatedSpan, err := h.drainBackendUntilRFQ(drainInput{
				bConn:          bConn,
				be:             be,
				clientConn:     conn,
				log:            log,
				state:          state,
				pendingEvictCC: &pendingEvictCloseCompletes,
				queryStart:     queryStart,
				lastSQL:        lastSQL,
				lastKind:       lastKind,
				lastPrepName:   lastPrepName,
				curSpan:        curSpan,
				sessionPinned:  sessionPinned,
				spliceReader:   h.bConnSpliceReader,
				spliceBufSize:  spliceBufSize(h.Splice),
			})
			if err != nil {
				return err
			}
			curSpan = updatedSpan
			if out.shouldRelease {
				releasePool(bConnPool, h.Pool).Release(bConn, h.ResetOnRelease)
				bConnPool = nil
				bConn = nil
			}
			queryTimedOut := out.queryTimedOut
			if queryTimedOut {
				// PG aborts the in-flight query when the FE socket
				// closes; that's sufficient — no separate CancelRequest
				// needed. The backend is now in an unknown state, so
				// close + drop it; the next message will Acquire a
				// fresh one.
				_ = bConn.Close()
				bConn = nil
				h.counters.OnQueryDuration(time.Since(queryStart).Seconds())
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

// selectPoolForMsg returns the pool to Acquire from for the first
// traffic-generating message of a new transaction.
//
// Primary wins by default. Replica wins when ALL of:
//   - session not pinned (LISTEN/temp/lock etc.)
//   - ReplicaPicker callback is configured + returns a non-nil pool
//   - the message is read-classified or opens a READ ONLY tx
//   - we're outside the read-your-own-writes sticky window
//     (StickyReadWindow elapsed since the last observed write)
func (h *PooledConn) selectPoolForMsg(
	sessionPinned bool, curSQLOp SQLOp, curROBegin bool, lastWriteAt time.Time,
) *pool.Pool {
	isRead := curSQLOp == SQLOpRead || curROBegin
	if sessionPinned || h.ReplicaPicker == nil || !isRead {
		return h.Pool
	}
	if h.stickyToPrimary(lastWriteAt) {
		return h.Pool
	}
	if rp := h.ReplicaPicker(); rp != nil {
		return rp
	}
	return h.Pool
}

// drainInput collapses the dozen-plus parameters drainBackendUntilRFQ
// needs into a single struct.
type drainInput struct {
	bConn          *backend.Conn
	be             *pgproto3.Backend
	// clientConn is the raw client-facing net.Conn (may be wrapped in
	// CountingConn for byte accounting). DrainSplice writes the
	// spliced boring bytes directly to it (bypassing pgproto3.Backend's
	// internal buffer) so the entire payload goes out in one write.
	// We flush be (which uses the SAME clientConn) before AND after the
	// splice to preserve message ordering with any pending decoded
	// data.
	clientConn     net.Conn
	log            *slog.Logger
	state          *ClientState
	pendingEvictCC *int
	queryStart     time.Time
	lastSQL        string
	lastKind       string
	lastPrepName   string
	curSpan        trace.Span
	sessionPinned  bool

	// spliceReader, when non-nil, is the RawReader that wraps
	// bConn.Frontend's internal chunkReader (via reflection). Both
	// bConn.Frontend.Receive and the splice drain loop read from the
	// SAME chunkReader, so the chunkReader's over-read buf is shared
	// and the splice loop can "rewind" 5 bytes to put a header back
	// for the next Frontend.Receive call. nil = splice disabled.
	spliceReader *splice.RawReader
	// spliceBufSize is the buffer size hint passed to DrainSplice.
	spliceBufSize int
	// spliceCoalesceSize is the max bytes to accumulate in the splice
	// buffer before flushing (batch writes for fewer syscalls).
	spliceCoalesceSize int
}

// drainOutcome is what the outer loop needs to act on:
//   - queryTimedOut: caller should Close + nil bConn + emit 57014
//   - shouldRelease: caller should Release bConn back to its pool
//   - sawRFQ:        diagnostic; both timeout + RFQ exit set it false/true
type drainOutcome struct {
	queryTimedOut bool
	shouldRelease bool
}

// drainBackendUntilRFQ pulls backend frames until ReadyForQuery (or
// CopyInResponse, where we stop and return control to the outer loop
// so it can receive CopyData from the client). For every frame:
//
//   - CloseComplete frames produced by LRU evictions are filtered out
//   - all other frames are forwarded to the client
//   - tx-state transitions emit the the appropriate counter
//   - RFQ triggers onQueryComplete (which ends curSpan)
//
// Returns the updated curSpan (nil after RFQ-driven onQueryComplete).
// The outer loop owns the bConn/bConnPool Release decision; this
// helper only signals whether release is safe via outcome.shouldRelease.
//
// Phase A splice forwarder:
//   - If in.spliceReader is non-nil, the loop first calls
//     splice.DrainSplice to forward "boring" messages (DataRow,
//     RowDescription, CommandComplete, ParseComplete, BindComplete,
//     NoData, EmptyQuery, PortalSuspended) as raw bytes — bypassing
//     pgproto3 decode/re-encode for the hot path.
//   - DrainSplice stops when it hits a non-boring message and puts
//     the 5-byte header back via the PutbackReader. The next
//     bConn.Frontend.Receive() picks up the putback bytes and
//     decodes the interesting message the normal way.
//   - DrainSplice writes directly to bConn.NetConn (via in.be's
//     writer side), so we Flush in.be before splice starts to
//     preserve message ordering with any pending decoded data.
//   - On any I/O error, splice returns the error verbatim; the loop
//     exits and the caller closes the backend.
func (h *PooledConn) drainBackendUntilRFQ(in drainInput) (drainOutcome, trace.Span, error) {
	out := drainOutcome{}
	curSpan := in.curSpan
	for {
		// Phase A: splice forward boring messages. On ErrSpliceStop
		// the next bmsg is the non-boring message the splice put
		// back into the reader — fall through to Receive() to
		// decode it the normal way.
		if in.spliceReader != nil {
			if err := in.be.Flush(); err != nil {
				return out, curSpan, fmt.Errorf("client flush pre-splice: %w", err)
			}
			serr := splice.DrainSplice(in.clientConn, in.spliceReader, in.spliceBufSize)
			if serr != nil {
				if errors.Is(serr, splice.ErrSpliceStop) {
					// Fall through to Receive() to decode the
					// interesting message that was put back.
				} else if errors.Is(serr, io.EOF) {
					// Backend closed the conn mid-drain.
					return out, curSpan, nil
				} else if isTimeoutErr(serr) && h.QueryTimeout > 0 {
					out.queryTimedOut = true
					h.counters.OnQueryTimeout()
					in.log.Info("query_timeout fired; closing backend",
						"timeout", h.QueryTimeout,
					)
					return out, curSpan, nil
				} else {
					return out, curSpan, fmt.Errorf("splice drain: %w", serr)
				}
			}
		}

		bmsg, err := in.bConn.Frontend.Receive()
		if err != nil {
			if isTimeoutErr(err) && h.QueryTimeout > 0 {
				out.queryTimedOut = true
				h.counters.OnQueryTimeout()
				in.log.Info("query_timeout fired; closing backend",
					"timeout", h.QueryTimeout,
				)
				return out, curSpan, nil
			}
			return out, curSpan, fmt.Errorf("server recv: %w", err)
		}
		// Filter out CloseComplete frames produced by our LRU
		// evictions — the client never asked for them.
		if _, isCC := bmsg.(*pgproto3.CloseComplete); isCC && *in.pendingEvictCC > 0 {
			*in.pendingEvictCC--
			continue
		}
		in.be.Send(bmsg.(pgproto3.BackendMessage))
		if err := in.be.Flush(); err != nil {
			return out, curSpan, fmt.Errorf("client send: %w", err)
		}
		// Tx-state transitions → per-(db, user) counters.
		prevTx := in.state.Tx()
		if in.state.ObserveBackendMessage(bmsg) {
			newTx := in.state.Tx()
			switch {
			case prevTx != TxInBlock && prevTx != TxFailed && newTx == TxInBlock:
				h.counters.OnTxStart()
			case prevTx == TxFailed && newTx == TxIdle:
				h.counters.OnTxRollback()
			case prevTx == TxInBlock && newTx == TxIdle:
				h.counters.OnTxCommit()
			}
		}
		// CopyInResponse — backend is now waiting for client
		// CopyData. Stop draining; outer loop will receive CopyData.
		if _, ok := bmsg.(*pgproto3.CopyInResponse); ok {
			if h.QueryTimeout > 0 {
				_ = in.bConn.NetConn.SetReadDeadline(time.Time{})
			}
			return out, curSpan, nil
		}
		if _, ok := proto.IsReadyForQuery(bmsg); ok {
			if h.QueryTimeout > 0 {
				_ = in.bConn.NetConn.SetReadDeadline(time.Time{})
			}
			queryDur := time.Since(in.queryStart)
			h.onQueryComplete(in.log, in.lastKind, in.lastSQL,
				in.lastPrepName, queryDur, curSpan)
			curSpan = nil
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
			out.shouldRelease = !in.sessionPinned &&
				(h.isStatementMode() || in.state.Tx().IsIdle())
			return out, curSpan, nil
		}
	}
}

// observeClientMessage runs the per-message hooks that drive the GUC +
// prepare caches and the session-pin trigger. info, if non-nil, holds
// pre-computed SQL analysis (from AnalyzeSQL) that replaces redundant
// keyword extraction + regex evals.
func (h *PooledConn) observeClientMessage(
	msg pgproto3.FrontendMessage,
	gucCache *GUCCache,
	prepCache *PrepareCache,
	sessionPinned *bool,
	log *slog.Logger,
	info *SQLInfo,
) {
	switch m := msg.(type) {
	case *pgproto3.Query:
		h.logSQL(log, "query", "", m.String)
		if info != nil {
			gucCache.ObserveQueryWithInfo(m.String, info)
			if !*sessionPinned && info.NeedsPin {
				*sessionPinned = true
				log.Info("session pinned (force-session)",
					"reason", "incompatible feature in Query",
					"sql_prefix", truncate(m.String, 64),
				)
			}
		} else {
			gucCache.ObserveQuery(m.String)
			if !*sessionPinned && needsSessionPin(m.String) {
				*sessionPinned = true
				log.Info("session pinned (force-session)",
					"reason", "incompatible feature in Query",
					"sql_prefix", truncate(m.String, 64),
				)
			}
		}
	case *pgproto3.Parse:
		h.logSQL(log, "parse", m.Name, m.Query)
		if prepCache == nil && h.PreparedCache {
			prepCache = NewPrepareCache()
		}
		if prepCache != nil {
			prepCache.Observe(m.Name, m.Query, m.ParameterOIDs)
		}
		if info != nil {
			if !*sessionPinned && info.NeedsPin {
				*sessionPinned = true
				log.Info("session pinned (force-session)",
					"reason", "incompatible feature in Parse",
					"sql_prefix", truncate(m.Query, 64),
				)
			}
		} else {
			if !*sessionPinned && needsSessionPin(m.Query) {
				*sessionPinned = true
				log.Info("session pinned (force-session)",
					"reason", "incompatible feature in Parse",
					"sql_prefix", truncate(m.Query, 64),
				)
			}
		}
	case *pgproto3.Close:
		// Close('S', name) untracks a prepared statement.
		if m.ObjectType == 'S' && prepCache != nil {
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
	if err := proto.DrainSimpleQuery(bConn.Frontend, sql, nil); err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	return nil
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

// onQueryComplete is the single fan-out point for all per-query
// observability after the backend reports ReadyForQuery:
//
//   1. stats.OnQueryDuration  (Prometheus histogram, always on)
//   2. slow_query log         (when h.SlowQuery > 0 and dur >= it)
//   3. audit JSON line        (when h.Audit != nil)
//   4. OTel span end          (when curSpan != nil)
//
// The redacted SQL is rendered ONCE at the longest cap any sink needs
// (1024 bytes for audit) and reused — previously slow_query + audit
// each ran SQLForLog independently, doing the regex/scan twice per
// query at the cost of an extra allocation.
func (h *PooledConn) onQueryComplete(log *slog.Logger, kind, sql, prepName string,
	dur time.Duration, span trace.Span,
) {
	h.counters.OnQueryDuration(dur.Seconds())

	wantSlow := h.SlowQuery > 0 && dur >= h.SlowQuery && kind != ""
	wantAudit := h.Audit != nil && kind != ""

	// Render SQL once if at least one sink will consume it. 1024 covers
	// the audit cap; the slow_query log truncates further at emit time
	// if needed.
	var renderedSQL string
	if wantSlow || wantAudit {
		renderedSQL = SQLForLog(h.LogSQL, sql, 1024)
	}

	if wantSlow {
		// Re-truncate to the slow_query cap (512) for terser log lines.
		slowSQL := renderedSQL
		if len(slowSQL) > 512 {
			slowSQL = slowSQL[:512] + "…"
		}
		log.Warn("slow_query",
			"kind", kind,
			"duration", dur,
			"threshold", h.SlowQuery,
			"prepared_name", prepName,
			"sql", slowSQL,
		)
	}
	if wantAudit {
		h.Audit.Write(h.ReqID, h.Database, h.User, h.App,
			kind, renderedSQL, dur)
	}
	if span != nil {
		span.SetAttributes(
			attribute.Float64("pgrouter.duration_ms",
				float64(dur.Microseconds())/1000.0))
		span.End()
	}
	// User-provided extensibility hooks run last so a panicking hook
	// can't corrupt the built-in observability state. RenderedSQL is
	// reused so hooks don't re-run the LogSQL pipeline.
	if len(h.Hooks) > 0 && kind != "" {
		ev := QueryEvent{
			Kind: kind, SQL: sql, RenderedSQL: renderedSQL,
			PrepName: prepName, Duration: dur,
			Database: h.Database, User: h.User, App: h.App, ReqID: h.ReqID,
		}
		for _, hook := range h.Hooks {
			hook(ev)
		}
	}
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
	// Fast path: skip span creation entirely when tracing is not
	// configured. Saves attribute-build + Start() overhead on the
	// hot per-Query/Parse path (no-op tracer still has measurable
	// cost at high QPS).
	if !tracing.Enabled() {
		return nil
	}
	attrs := []attribute.KeyValue{
		attribute.String("db.system", "postgresql"),
		attribute.String("db.name", h.Database),
		attribute.String("db.user", h.User),
		attribute.String("pgrouter.req_id", h.ReqID),
		attribute.String("pgrouter.app", h.App),
		attribute.String("pgrouter.kind", kind),
		attribute.String("pgrouter.prepared_name", prepName),
	}
	// EFF1: skip the SQLForLog call + the db.statement attribute when
	// logging is off. Both wire payload (per-span attribute) and CPU
	// (RedactSQL pass under "redacted", truncate under "full") are
	// saved on the hot per-Query/Parse path.
	if h.LogSQL != "off" {
		if rendered := SQLForLog(h.LogSQL, sql, 512); rendered != "" {
			attrs = append(attrs, attribute.String("db.statement", rendered))
		}
	}
	_, span := tracing.Tracer().Start(ctx, "pgrouter."+kind,
		trace.WithAttributes(attrs...))
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
	if !h.hasIdleDeadline {
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
//
// If clientPrep is nil (cfg.Wire.PreparedCache=false), the entire
// interception is skipped — the message is forwarded as-is. This is
// the configured-disable path that recovers the regressions seen on
// workloads with low cache-hit rates.
func (h *PooledConn) prepareInterceptForward(
	msg pgproto3.FrontendMessage,
	clientPrep *PrepareCache,
	bConn *backend.Conn,
	be *pgproto3.Backend,
	pendingEvictCloseCompletes *int,
	log *slog.Logger,
) (pgproto3.FrontendMessage, bool, error) {
	if clientPrep == nil {
		return msg, false, nil
	}
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
			h.counters.OnPreparedHit()
			be.Send(&pgproto3.ParseComplete{})
			return nil, true, nil
		}
		// CACHE MISS — rewrite Name and forward.
		h.counters.OnPreparedMiss()
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
				h.counters.OnPreparedEviction()
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

// serveRaw is the raw-passthrough variant of Serve. When
// PooledConfig.RawPassthrough is true, this method handles the client
// connection by reading raw bytes (bypassing pgproto3 decode/re-encode)
// and forwarding them directly to the backend. Only Query and Parse
// messages have their SQL extracted for GUC/pin/classification.
//
// Backend→client splice (Phase A) continues to work independently.
// Prepared-cache interception is NOT supported in raw mode (messages
// can't be rewritten without decode).
func (h *PooledConn) serveRaw(ctx context.Context, conn net.Conn) error {
	log := h.Log.With("remote", conn.RemoteAddr().String(), "mode", "raw")

	h.hasIdleDeadline = h.ClientIdleTimeout > 0 || h.IdleTxTimeout > 0

	be := pgproto3.NewBackend(conn, conn)
	rawClient := rawconn.New(conn)

	// 1. Send the synthetic welcome.
	if err := h.sendWelcome(ctx, be); err != nil {
		return fmt.Errorf("welcome: %w", err)
	}
	log.Info("pooled client ready (raw passthrough)")

	state := NewClientState()
	gucCache := NewGUCCache()

	var lastSQL, lastKind string
	var curSpan trace.Span
	var lastWriteAt time.Time

	// First synthetic RFQ → mark client idle.
	state.ObserveBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	var bConn *backend.Conn
	var bConnPool *pool.Pool
	sessionPinned := false

	// Write buffer for batching client→backend messages. Messages are
	// accumulated here and flushed to bConn.NetConn when a drain
	// trigger (Sync/Query) arrives, coalescing multiple small writes
	// into a single write syscall.
	var writeBuf []byte

	defer func() {
		if bConn != nil {
			releasePool(bConnPool, h.Pool).Release(bConn, h.ResetOnRelease)
			bConnPool = nil
		}
		if curSpan != nil {
			h.markSpanFailed(curSpan, "PGROUTER_ABORTED",
				"serve loop aborted before query completion")
			curSpan = nil
		}
	}()

	for {
		// Honour cancellation between messages.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		h.applyIdleDeadline(conn, state)

		tag, raw, err := rawClient.ReadMessage()
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
					h.counters.OnIdleTxTimeout()
				} else {
					h.counters.OnClientIdleTimeout()
				}
				log.Info("client closed by timeout", "kind", which)
				h.sendFatalErrorWithWriteDeadline(be, conn, code,
					"pgrouter: "+which, 200*time.Millisecond)
				return nil
			}
			return fmt.Errorf("client recv: %w", err)
		}

		// Clear the read deadline.
		if h.hasIdleDeadline {
			_ = conn.SetReadDeadline(time.Time{})
		}

		// Terminate.
		if rawconn.IsTerminate(tag) {
			log.Info("client sent Terminate")
			return nil
		}

		// Track QueriesIssued for Query/Parse (mirrors ObserveClientMessage).
		if tag == rawconn.TagQuery || tag == rawconn.TagParse {
			state.QueriesIssued++
		}

		// Extract SQL + compute SQLInfo for Query/Parse.
		var sql string
		var sqlInfo *SQLInfo
		switch tag {
		case rawconn.TagQuery:
			sql = rawconn.ExtractQuerySQL(raw)
			info := AnalyzeSQL(sql)
			sqlInfo = &info
			h.logSQL(log, "query", "", sql)
		case rawconn.TagParse:
			_, sql = rawconn.ExtractParseFields(raw)
			info := AnalyzeSQL(sql)
			sqlInfo = &info
			h.logSQL(log, "parse", "", sql)
		}

		// GUC + session-pin observation.
		if sqlInfo != nil {
			gucCache.ObserveQueryWithInfo(sql, sqlInfo)
			if !sessionPinned && sqlInfo.NeedsPin {
				sessionPinned = true
				log.Info("session pinned (force-session)",
					"reason", "incompatible feature",
					"sql_prefix", truncate(sql, 64))
			}
		}

		// Unrecognized SET → force session-pin.
		if !sessionPinned && gucCache.HasUnrecognizedSet() {
			sessionPinned = true
			log.Info("session pinned (force-session)",
				"reason", "SET of GUC outside replayable whitelist")
		}

		// Statement-mode: reject explicit BEGIN.
		if h.isStatementMode() {
			if tag == rawconn.TagQuery {
				if IsExplicitBeginSQL(sql) {
					log.Info("statement_mode: rejecting explicit BEGIN",
						"sql_prefix", truncate(sql, 64))
					h.sendErrorWithRFQ(be, "25001",
						"pgrouter: explicit transactions are not allowed in statement-mode pool")
					continue
				}
			}
		}

		// Per-query counters + slow-query stash + SQL classification.
		var curSQLOp SQLOp
		var curROBegin bool
		if tag == rawconn.TagQuery || tag == rawconn.TagParse {
			if !h.takeQPS() {
				log.Info("qps_limit: rejected", "kind", "raw")
				h.sendErrorWithRFQ(be, "53300",
					fmt.Sprintf("pgrouter: per-tenant QPS cap exceeded (db=%s user=%s)",
						h.Database, h.User))
				continue
			}
			h.counters.OnQuery()
			lastSQL = sql
			if tag == rawconn.TagQuery {
				lastKind = "query"
			} else {
				lastKind = "parse"
			}
			if curSpan != nil {
				h.markSpanFailed(curSpan, "PGROUTER_SUPERSEDED",
					"replaced by next Query/Parse before drain")
			}
			curSpan = h.startQuerySpan(ctx, lastKind, sql, "")
			if sqlInfo != nil {
				curSQLOp = sqlInfo.Op
				curROBegin = sqlInfo.IsROBegin
			} else {
				curSQLOp, curROBegin = ClassifyDetail(sql)
			}
			if curSQLOp == SQLOpWrite && !curROBegin {
				lastWriteAt = time.Now()
			}
		}

		// Acquire a backend lazily on the first traffic-generating message.
		needsBackend := rawconn.NeedsBackend(tag)
		if needsBackend && bConn == nil {
			acquirePool := h.selectPoolForMsg(sessionPinned,
				curSQLOp, curROBegin, lastWriteAt)
			if acquirePool == h.Pool && h.PrimaryHealthy != nil && !h.PrimaryHealthy() {
				log.Info("failover: rejecting write — primary unhealthy",
					"db", h.Database)
				h.sendErrorWithRFQ(be, "08006",
					fmt.Sprintf("pgrouter: primary for %q is unhealthy (failover); retry later", h.Database))
				if curSpan != nil {
					h.markSpanFailed(curSpan, "08006", "primary unhealthy (failover)")
					curSpan = nil
				}
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

			// Phase A splice on the fresh backend.
			h.setupSplice(bConn)

			// Replay GUCs.
			if replay := gucCache.ReplayQuery(); replay != "" {
				if err := h.fireReplay(bConn, replay); err != nil {
					log.Warn("guc replay failed; treating backend as bad", "err", err)
					_ = bConn.Close()
					bConn = nil
					h.sendFatalError(be, "57P03",
						fmt.Sprintf("pgrouter: backend replay failed: %v", err))
					return err
				}
			}
		}

		if bConn != nil {
			// Forward raw bytes to backend. Boring messages
			// (Execute/Sync/Flush) go directly; Query/Parse go
			// with their original raw bytes (no rewrite in raw mode).
			writeBuf = append(writeBuf, raw...)

			// Flush when a drain trigger arrives.
			if rawconn.IsDrainTrigger(tag) {
				if _, err := bConn.NetConn.Write(writeBuf); err != nil {
					return fmt.Errorf("server send: %w", err)
				}
				writeBuf = writeBuf[:0]

				queryStart := time.Now()
				if h.QueryTimeout > 0 {
					_ = bConn.NetConn.SetReadDeadline(time.Now().Add(h.QueryTimeout))
				}

				out, updatedSpan, err := h.drainBackendUntilRFQ(drainInput{
					bConn:          bConn,
					be:             be,
					clientConn:     conn,
					log:            log,
					state:          state,
					pendingEvictCC: &h.pendingEvictCC,
					queryStart:     queryStart,
					lastSQL:        lastSQL,
					lastKind:       lastKind,
					lastPrepName:   "",
					curSpan:        curSpan,
					sessionPinned:  sessionPinned,
					spliceReader:   h.bConnSpliceReader,
					spliceBufSize:  spliceBufSize(h.Splice),
				})
				if err != nil {
					return err
				}
				curSpan = updatedSpan
				if out.shouldRelease {
					releasePool(bConnPool, h.Pool).Release(bConn, h.ResetOnRelease)
					bConnPool = nil
					bConn = nil
				}
				if out.queryTimedOut {
					_ = bConn.Close()
					bConn = nil
					h.counters.OnQueryDuration(time.Since(queryStart).Seconds())
					h.sendFatalErrorWithWriteDeadline(be, conn, "57014",
						fmt.Sprintf("pgrouter: query_timeout (%s) exceeded", h.QueryTimeout),
						200*time.Millisecond)
				}
			}
			continue
		}

		// Backend not needed — synthesize no-op response.
		if tag == rawconn.TagSync {
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			_ = be.Flush()
		}
	}
}
