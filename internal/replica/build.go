// Shared replica-manager construction.
//
// cmd/pgrouter/main.go and pkg/pgrouter/server.go both need to walk
// cfg.Databases and stand up one replica.Manager per database that
// has replicas configured. The two implementations had drifted
// (~40 lines duplicated); now they both call BuildManagersFromConfig.
//
// The caller supplies:
//
//   - cfg.Databases-equivalent map (we don't depend on config; the
//     caller projects what we need)
//   - a poolFactory that knows how to dial against backend.DialOptions
//     (so this package doesn't have to import backend internals beyond
//     the existing Replica.Pool field)
//   - a defaultPoolCfg copy to apply to each replica pool

package replica

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
)

// ReplicaDef is the per-replica config the caller projects from its
// own config schema (avoid depending on internal/config here, which
// would create an import cycle with the cmd/pkg wiring).
type ReplicaDef struct {
	Host   string
	Port   int
	Weight int
}

// DBDef is the per-database projection: primary upstream + replica
// list + the dbname + auth bits needed to dial.
type DBDef struct {
	Name               string // the YAML map key — what clients see
	DBName             string // the upstream PG database name
	User               string
	Password           string
	Replicas           []ReplicaDef
	MaxReplicaLagBytes int64
}

// BuildManagersFromConfig walks dbs and returns one started-but-not-yet
// running replica.Manager per database that has replicas configured.
// Callers should `.Start()` + `.StartLagPolls(...)` on each returned
// manager once the surrounding lifecycle is ready.
//
// The pool.Pool per replica is created via pool.New using the supplied
// defaultCfg (so per-pool sizing/timeouts match the primary path).
func BuildManagersFromConfig(
	dbs []DBDef,
	defaultPoolCfg pool.Config,
	dial func(addr, user, dbname, password string) backend.DialOptions,
	healthInterval time.Duration,
	checkQuery string,
	log *slog.Logger,
) map[string]*Manager {
	out := map[string]*Manager{}
	for _, db := range dbs {
		if len(db.Replicas) == 0 {
			continue
		}
		reps := make([]*Replica, 0, len(db.Replicas))
		for _, rspec := range db.Replicas {
			rspec := rspec
			addr := net.JoinHostPort(rspec.Host, strconv.Itoa(rspec.Port))
			user := db.User
			if user == "" {
				user = "pgrouter"
			}
			opts := dial(addr, user, db.DBName, db.Password)
			poolName := fmt.Sprintf("%s-replica-%s:%d",
				db.Name, rspec.Host, rspec.Port)
			dialer := func(ctx context.Context) (*backend.Conn, error) {
				return backend.Dial(ctx, opts)
			}
			p := pool.New(poolName, dialer, defaultPoolCfg)
			reps = append(reps, &Replica{
				Spec: ReplicaSpec{
					Host: rspec.Host, Port: rspec.Port, Weight: rspec.Weight,
				},
				Pool: p,
			})
		}
		m := NewManager(db.Name, reps, healthInterval, checkQuery, log)
		m.SetMaxLag(db.MaxReplicaLagBytes)
		out[db.Name] = m
	}
	return out
}
