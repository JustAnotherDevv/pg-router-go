package replica

import (
	"testing"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
	"github.com/stretchr/testify/require"
)

func makeReplica(host string, port, weight int) *Replica {
	r := &Replica{Spec: ReplicaSpec{Host: host, Port: port, Weight: weight}}
	r.healthy.Store(true)
	return r
}

// TestPick consolidates Manager.Pick() coverage across the lag-cap and
// health-filter dimensions. Each case configures replicas (host, lag,
// healthy), optionally caps max-lag, then asserts the Pick() outcome.
//
// Earlier history: this table collapsed
//   - TestPickRespectsMaxLag, TestPickAllOverCapReturnsErr,
//     TestPickMaxLagZeroMeansUnbounded (lag_filter_test.go)
//   - TestPickSkipsUnhealthy, TestPickAllUnhealthyReturnsErr
//     (manager_test.go)
// into one driver. Weighted-RR distribution stays separate (it asserts
// a count distribution, not a Pick-vs-expected-host equality).
func TestPick(t *testing.T) {
	type replicaSpec struct {
		host    string
		port    int
		weight  int
		lag     int64
		healthy bool
	}
	cases := []struct {
		name     string
		replicas []replicaSpec
		maxLag   int64 // 0 = unbounded
		wantHost string
		wantErr  error
	}{
		{
			name: "lag under cap picks the under-cap replica",
			replicas: []replicaSpec{
				{"a", 5432, 1, 50, true},
				{"b", 5432, 1, 500, true},
			},
			maxLag:   100,
			wantHost: "a",
		},
		{
			name: "all replicas over lag cap returns ErrNoHealthyReplica",
			replicas: []replicaSpec{
				{"a", 5432, 1, 500, true},
			},
			maxLag:  100,
			wantErr: ErrNoHealthyReplica,
		},
		{
			name: "max_lag=0 means unbounded — huge lag still picked",
			replicas: []replicaSpec{
				{"a", 5432, 1, 9999, true},
			},
			wantHost: "a",
		},
		{
			name: "skips unhealthy replica",
			replicas: []replicaSpec{
				{"a", 5432, 1, 0, true},
				{"b", 5432, 1, 0, false},
			},
			wantHost: "a",
		},
		{
			name: "all unhealthy returns ErrNoHealthyReplica",
			replicas: []replicaSpec{
				{"a", 5432, 1, 0, false},
			},
			wantErr: ErrNoHealthyReplica,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reps := make([]*Replica, 0, len(tc.replicas))
			for _, s := range tc.replicas {
				r := makeReplica(s.host, s.port, s.weight)
				if s.lag != 0 {
					r.lagBytes.Store(s.lag)
				}
				if !s.healthy {
					r.healthy.Store(false)
				}
				reps = append(reps, r)
			}
			m := NewManager("appdb", reps, time.Hour, "SELECT 1", testutil.Discard)
			if tc.maxLag > 0 {
				m.SetMaxLag(tc.maxLag)
			}
			// Run many picks so non-skip cases verify the choice is
			// stable (and skip cases keep skipping, not just-once).
			for i := 0; i < 10; i++ {
				r, err := m.Pick()
				if tc.wantErr != nil {
					require.ErrorIs(t, err, tc.wantErr)
					continue
				}
				require.NoError(t, err)
				require.Equal(t, tc.wantHost, r.Spec.Host)
			}
		})
	}
}

func TestPickRoundRobinWeighted(t *testing.T) {
	a := makeReplica("a", 5432, 1)
	b := makeReplica("b", 5432, 3) // 3x weight
	m := NewManager("appdb", []*Replica{a, b}, time.Hour, "SELECT 1",
		testutil.Discard)

	counts := map[string]int{}
	for i := 0; i < 400; i++ {
		r, err := m.Pick()
		require.NoError(t, err)
		counts[r.Spec.Host]++
	}
	// 100 vs 300 expected.
	require.InDelta(t, 100, counts["a"], 5)
	require.InDelta(t, 300, counts["b"], 5)
}

func TestReplicaHealthyAndLagAtomicAccessors(t *testing.T) {
	r := makeReplica("a", 5432, 1)
	require.True(t, r.Healthy())
	r.healthy.Store(false)
	require.False(t, r.Healthy())
	r.lagBytes.Store(1024)
	require.Equal(t, int64(1024), r.LagBytes())
}
