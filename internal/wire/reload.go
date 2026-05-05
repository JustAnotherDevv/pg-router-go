// SIGHUP fan-in + config-reload pump. Shared by cmd/pgrouter (OS
// signal-driven) and library mode (Server.Reload triggers the same
// pump path).
//
// Previously these lived in cmd/pgrouter/main.go so library users had
// no canonical reload semantics. Lifted into internal/wire so both
// paths share the same observability + ApplyDefaultSize plumbing.

package wire

import (
	"context"
	"log/slog"
	"os"

	"github.com/JustAnotherDevv/pgrouter/internal/auth"
	"github.com/JustAnotherDevv/pgrouter/internal/config"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/stats"
)

// FanInSignals merges N source channels into dst until ctx fires. Used
// by cmd-mode to combine OS SIGHUP + admin-API /reload POST into one
// signal stream.
//
// Non-blocking on dst: if dst is full, the signal is dropped. Pair with
// a `chan os.Signal` of capacity ≥ 1 in the caller.
func FanInSignals(ctx context.Context, dst chan<- os.Signal, sources ...<-chan os.Signal) {
	for _, src := range sources {
		go func(c <-chan os.Signal) {
			for {
				select {
				case <-ctx.Done():
					return
				case s, ok := <-c:
					if !ok {
						return
					}
					select {
					case dst <- s:
					default:
					}
				}
			}
		}(src)
	}
}

// RunSighupReloader drains hupCh, re-reads the YAML at path on every
// signal, logs the diff, applies the new DefaultPoolSize live, and
// reloads the userlist file (if configured). Returns when ctx fires.
//
// `current` is the live *config.Config that will be UPDATED IN PLACE
// to point at the new tree on success — callers holding a *Config
// reference (e.g. the cfgRouter) see the new values without restart.
func RunSighupReloader(ctx context.Context, hupCh <-chan os.Signal,
	path string, current *config.Config, userlist *auth.Userlist,
	mgr *pool.Manager, log *slog.Logger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-hupCh:
			if !ok {
				return
			}
			next, err := config.Load(path)
			if err != nil {
				log.Error("SIGHUP reload failed", "path", path, "err", err)
				stats.OnSighupReload("fail")
				// Even if the YAML failed, still attempt the userlist
				// reload — its file is independent and may be valid.
				reloadUserlist(userlist, log)
				continue
			}
			log.Info("SIGHUP reload",
				"path", path,
				"databases_before", len(current.Databases),
				"databases_after", len(next.Databases),
				"default_pool_size_before", current.Pool.DefaultPoolSize,
				"default_pool_size_after", next.Pool.DefaultPoolSize,
				"pool_mode_before", string(current.Pool.Mode),
				"pool_mode_after", string(next.Pool.Mode),
				"query_timeout_before", current.Pool.QueryTimeout,
				"query_timeout_after", next.Pool.QueryTimeout,
			)
			if mgr != nil {
				changes := mgr.ApplyDefaultSize(next.Pool.DefaultPoolSize,
					func(k pool.Key) int {
						if d, ok := next.Databases[k.DB]; ok && d.PoolSize > 0 {
							return d.PoolSize
						}
						return 0
					})
				for _, c := range changes {
					log.Info("pool resized",
						"pool", c.Key.String(),
						"from", c.From, "to", c.To)
				}
			}
			*current = *next
			stats.OnSighupReload("ok")
			reloadUserlist(userlist, log)
		}
	}
}

// reloadUserlist re-reads the in-memory userlist (if one is configured)
// and logs the diff. No-op when no userlist_file was set.
func reloadUserlist(ul *auth.Userlist, log *slog.Logger) {
	if ul == nil {
		stats.OnSighupUserlistReload("skip")
		return
	}
	diff, err := ul.ReloadDiff()
	if err != nil {
		log.Error("SIGHUP userlist reload failed", "err", err)
		stats.OnSighupUserlistReload("fail")
		return
	}
	log.Info("SIGHUP userlist reload",
		"before", diff.Before,
		"after", diff.After,
		"added", len(diff.Added),
		"removed", len(diff.Removed),
		"rotated", len(diff.Rotated),
	)
	stats.OnSighupUserlistReload("ok")
}
