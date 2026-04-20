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
)

// PooledConn is a transaction-mode pooled handler for one client.
type PooledConn struct {
	Log *slog.Logger

	// Pool is the (db, user) pool to Acquire / Release from.
	Pool *pool.Pool

	// Database + User are the labels used for per-(db, user) Prometheus
	// metrics. Production paths set these from the StartupMessage; tests
	// may leave them empty (metrics simply emit empty labels).
	Database string
	User     string

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

	// First synthetic RFQ → mark client idle.
	state.ObserveBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	var bConn *backend.Conn
	// `sessionPinned` flips on once we see LISTEN / advisory_lock / temp
	// table / cursor — after which the backend stays attached for the
	// rest of the client's session.
	sessionPinned := false

	defer func() {
		if bConn != nil {
			// On client disconnect we always release. Reset is best-effort
			// per the user's ResetOnRelease config.
			h.Pool.Release(bConn, h.ResetOnRelease)
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
					stats.OnIdleTxTimeout(h.Database, h.User)
				} else {
					stats.OnClientIdleTimeout(h.Database, h.User)
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
		// Per-(db, user) Query/Parse counter.
		switch msg.(type) {
		case *pgproto3.Query, *pgproto3.Parse:
			stats.OnQuery(h.Database, h.User)
		}

		// Acquire a backend lazily on the first traffic-generating message.
		needsBackend := messageNeedsBackend(msg)
		if needsBackend && bConn == nil {
			bConn, err = h.Pool.Acquire(ctx)
			if err != nil {
				h.sendFatalError(be, "08006",
					fmt.Sprintf("pgrouter: cannot acquire backend: %v", err))
				return err
			}
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
			// Forward client → server.
			bConn.Frontend.Send(msg.(pgproto3.FrontendMessage))
			if err := bConn.Frontend.Flush(); err != nil {
				return fmt.Errorf("server send: %w", err)
			}

			// query_timeout: arm a read deadline on the backend socket
			// while we wait for ReadyForQuery. Clear on RFQ.
			queryStart := time.Now()
			if h.QueryTimeout > 0 {
				_ = bConn.NetConn.SetReadDeadline(time.Now().Add(h.QueryTimeout))
			}

			// Drain backend response up to ReadyForQuery, forwarding each.
			queryTimedOut := false
			for {
				bmsg, err := bConn.Frontend.Receive()
				if err != nil {
					if isTimeoutErr(err) && h.QueryTimeout > 0 {
						queryTimedOut = true
						stats.OnQueryTimeout(h.Database, h.User)
						log.Info("query_timeout fired; closing backend",
							"timeout", h.QueryTimeout,
						)
						break
					}
					return fmt.Errorf("server recv: %w", err)
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
						stats.OnTxStart(h.Database, h.User)
					case prevTx == TxFailed && newTx == TxIdle:
						stats.OnTxRollback(h.Database, h.User)
					case prevTx == TxInBlock && newTx == TxIdle:
						stats.OnTxCommit(h.Database, h.User)
					}
				}
				if _, ok := proto.IsReadyForQuery(bmsg); ok {
					if h.QueryTimeout > 0 {
						_ = bConn.NetConn.SetReadDeadline(time.Time{})
					}
					stats.OnQueryDuration(h.Database, h.User,
						time.Since(queryStart).Seconds())
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
					if state.Tx().IsIdle() && !sessionPinned {
						h.Pool.Release(bConn, h.ResetOnRelease)
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
				stats.OnQueryDuration(h.Database, h.User,
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
		gucCache.ObserveQuery(m.String)
		if !*sessionPinned && needsSessionPin(m.String) {
			*sessionPinned = true
			log.Info("session pinned (force-session)",
				"reason", "incompatible feature in Query",
				"sql_prefix", truncate(m.String, 64),
			)
		}
	case *pgproto3.Parse:
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

func (h *PooledConn) sendFatalError(be *pgproto3.Backend, code, msg string) {
	be.Send(&pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     code,
		Message:  msg,
	})
	_ = be.Flush()
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
