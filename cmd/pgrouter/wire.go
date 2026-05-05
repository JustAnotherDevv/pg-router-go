// cmd-only wiring helpers. Most builders (TLS, auth, pool, replica,
// primary monitors, PooledHandler) moved to internal/wire and are
// shared with pkg/pgrouter (library mode). Only the admin HTTP API —
// which exists in cmd-mode and binds to the SIGHUP reload channel —
// stays here.

package main

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/stats"
)

// buildAdminAPI wires the HTTP admin endpoint to the live pool
// manager + reload channel. Drains never run synchronously on the
// HTTP request path; Drain just calls CloseWithDeadline which
// internally honours its own deadline.
func buildAdminAPI(mgr *pool.Manager, adminReloadCh chan os.Signal,
	startTime time.Time,
) *stats.AdminAPI {
	return &stats.AdminAPI{
		Pools: func() ([]stats.PoolSnapshot, error) {
			out := []stats.PoolSnapshot{}
			for _, ps := range mgr.AllStats() {
				db, user, _ := splitPoolName(ps.Name)
				out = append(out, stats.PoolSnapshot{
					Name:    ps.Name,
					DB:      db,
					User:    user,
					Idle:    ps.Idle,
					Active:  ps.Active,
					Waiters: ps.Waiters,
				})
			}
			return out, nil
		},
		Stats: func() (stats.StatsSnapshot, error) {
			return stats.SnapshotFromRegistry(time.Since(startTime)), nil
		},
		Drain: func(deadline time.Duration) error {
			return mgr.CloseWithDeadline(time.Now().Add(deadline))
		},
		Reload: func() error {
			select {
			case adminReloadCh <- syscall.SIGHUP:
				return nil
			default:
				return fmt.Errorf("reload channel busy")
			}
		},
	}
}
