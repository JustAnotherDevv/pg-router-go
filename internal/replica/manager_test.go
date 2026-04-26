package replica

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func makeReplica(host string, port, weight int) *Replica {
	r := &Replica{Spec: ReplicaSpec{Host: host, Port: port, Weight: weight}}
	r.healthy.Store(true)
	return r
}

func TestPickRoundRobinWeighted(t *testing.T) {
	a := makeReplica("a", 5432, 1)
	b := makeReplica("b", 5432, 3) // 3x weight
	m := NewManager("appdb", []*Replica{a, b}, time.Hour, "SELECT 1",
		slog.New(slog.DiscardHandler))

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

func TestPickSkipsUnhealthy(t *testing.T) {
	a := makeReplica("a", 5432, 1)
	b := makeReplica("b", 5432, 1)
	b.healthy.Store(false)
	m := NewManager("appdb", []*Replica{a, b}, time.Hour, "SELECT 1",
		slog.New(slog.DiscardHandler))

	for i := 0; i < 10; i++ {
		r, err := m.Pick()
		require.NoError(t, err)
		require.Equal(t, "a", r.Spec.Host)
	}
}

func TestPickAllUnhealthyReturnsErr(t *testing.T) {
	a := makeReplica("a", 5432, 1)
	a.healthy.Store(false)
	m := NewManager("appdb", []*Replica{a}, time.Hour, "SELECT 1",
		slog.New(slog.DiscardHandler))
	_, err := m.Pick()
	require.ErrorIs(t, err, ErrNoHealthyReplica)
}

func TestReplicaHealthyAndLagAtomicAccessors(t *testing.T) {
	r := makeReplica("a", 5432, 1)
	require.True(t, r.Healthy())
	r.healthy.Store(false)
	require.False(t, r.Healthy())
	r.lagBytes.Store(1024)
	require.Equal(t, int64(1024), r.LagBytes())
}
