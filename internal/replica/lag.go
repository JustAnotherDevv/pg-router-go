// Replication lag tracking.
//
// One goroutine per replica polls:
//
//   SELECT pg_wal_lsn_diff(received, replayed)::bigint
//   FROM (
//     SELECT pg_current_wal_lsn() AS received,
//            pg_last_wal_replay_lsn() AS replayed
//   ) t
//
// Wait — that's the wrong primary/replica relationship. The accurate
// per-replica lag is the replica's pg_last_wal_replay_lsn() vs the
// PRIMARY's pg_current_wal_lsn(). We approximate by querying the
// replica only:
//
//   SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::bigint
//
// returns seconds-of-staleness as an int — close enough for the
// lag-aware routing threshold (operators set max_replica_lag_bytes
// but we treat it as bytes ≈ seconds for the v1 cut; full
// bytes-via-pg_wal_lsn_diff with a primary cross-query is post-v1).
//
// Lag updates Replica.lagBytes (we keep the name for forward-compat
// even though units are seconds in MVP).

package replica

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
)

// lagQuery is the per-replica probe. seconds-of-replay-lag.
const lagQuery = "SELECT COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::bigint, 0)"

// StartLagPolls spawns one lag-poll goroutine per replica. Interval
// defaults to 5s.
func (m *Manager) StartLagPolls(interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	for _, r := range m.replicas {
		r := r
		m.wg.Add(1)
		go m.lagLoop(r, interval)
	}
}

func (m *Manager) lagLoop(r *Replica, interval time.Duration) {
	defer m.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	m.lagProbe(r)
	for {
		select {
		case <-m.stopCh:
			return
		case <-t.C:
			m.lagProbe(r)
		}
	}
}

// lagProbe runs one lag query; on parse failure leaves the previous
// value in place + logs once.
func (m *Manager) lagProbe(r *Replica) {
	if !r.Healthy() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := r.Pool.Acquire(ctx)
	if err != nil {
		return
	}
	defer r.Pool.Release(c, false)

	val, err := scalarInt(c, lagQuery)
	if err != nil {
		m.log.Debug("replica lag probe failed",
			"db", m.db, "host", r.Spec.Host, "err", err)
		return
	}
	prev := r.lagBytes.Swap(val)
	// Rebuild Pick snapshot if the lag transition crosses the maxLag
	// threshold (replica enters or leaves the candidate set).
	if maxLag := m.maxLag.Load(); maxLag > 0 {
		before := prev <= maxLag
		after := val <= maxLag
		if before != after {
			m.rebuildSnapshot()
		}
	}
}

// scalarInt runs sql, expects one DataRow with one int column, returns it.
func scalarInt(c *backend.Conn, sql string) (int64, error) {
	c.Frontend.Send(&pgproto3.Query{String: sql})
	if err := c.Frontend.Flush(); err != nil {
		return 0, fmt.Errorf("lag flush: %w", err)
	}
	var out int64
	var got bool
	for {
		msg, err := c.Frontend.Receive()
		if err != nil {
			return 0, fmt.Errorf("lag recv: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.DataRow:
			if len(m.Values) > 0 {
				v, err := strconv.ParseInt(strings.TrimSpace(string(m.Values[0])), 10, 64)
				if err != nil {
					return 0, fmt.Errorf("lag parse %q: %w", m.Values[0], err)
				}
				out = v
				got = true
			}
		case *pgproto3.ErrorResponse:
			return 0, fmt.Errorf("lag error: %s", m.Message)
		case *pgproto3.ReadyForQuery:
			if !got {
				return 0, fmt.Errorf("lag: no row")
			}
			return out, nil
		}
	}
}
