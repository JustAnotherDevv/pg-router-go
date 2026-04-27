package replica

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPickRespectsMaxLag(t *testing.T) {
	a := makeReplica("a", 5432, 1)
	a.lagBytes.Store(50)
	b := makeReplica("b", 5432, 1)
	b.lagBytes.Store(500)

	m := NewManager("appdb", []*Replica{a, b}, time.Hour, "SELECT 1",
		slog.New(slog.DiscardHandler))
	m.SetMaxLag(100)

	// Only `a` is under the cap.
	for i := 0; i < 10; i++ {
		r, err := m.Pick()
		require.NoError(t, err)
		require.Equal(t, "a", r.Spec.Host)
	}
}

func TestPickAllOverCapReturnsErr(t *testing.T) {
	a := makeReplica("a", 5432, 1)
	a.lagBytes.Store(500)
	m := NewManager("appdb", []*Replica{a}, time.Hour, "SELECT 1",
		slog.New(slog.DiscardHandler))
	m.SetMaxLag(100)
	_, err := m.Pick()
	require.ErrorIs(t, err, ErrNoHealthyReplica)
}

func TestPickMaxLagZeroMeansUnbounded(t *testing.T) {
	a := makeReplica("a", 5432, 1)
	a.lagBytes.Store(9999)
	m := NewManager("appdb", []*Replica{a}, time.Hour, "SELECT 1",
		slog.New(slog.DiscardHandler))
	r, err := m.Pick()
	require.NoError(t, err)
	require.Equal(t, "a", r.Spec.Host)
}
