package replica

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
)

// nopDial is a never-called dialer used when the test drives state
// transitions via recordSuccess/recordFailure directly.
func nopDial(ctx context.Context) (*backend.Conn, error) {
	return nil, errors.New("dial not expected")
}

func TestPrimaryMonitorStartsHealthy(t *testing.T) {
	pm := NewPrimaryMonitor("appdb", nopDial, time.Hour, 3, "SELECT 1",
		slog.New(slog.DiscardHandler))
	require.True(t, pm.Healthy())
}

func TestPrimaryMonitorMarksUnhealthyAfterThreshold(t *testing.T) {
	pm := NewPrimaryMonitor("appdb", nopDial, time.Hour, 3, "SELECT 1",
		slog.New(slog.DiscardHandler))
	pm.recordFailure("test", errors.New("x"))
	require.True(t, pm.Healthy(), "1 failure < threshold 3")
	pm.recordFailure("test", errors.New("x"))
	require.True(t, pm.Healthy(), "2 failures still < threshold")
	pm.recordFailure("test", errors.New("x"))
	require.False(t, pm.Healthy(), "3rd failure trips threshold")
}

func TestPrimaryMonitorRecoversAfterSuccess(t *testing.T) {
	pm := NewPrimaryMonitor("appdb", nopDial, time.Hour, 1, "SELECT 1",
		slog.New(slog.DiscardHandler))
	pm.recordFailure("test", errors.New("x"))
	require.False(t, pm.Healthy())
	pm.recordSuccess()
	require.True(t, pm.Healthy())
}

// TestPrimaryMonitorSuccessClearsFailCountBeforeThreshold ensures a
// success in the middle of a partial failure run resets the counter
// so the next 3 failures (not 2) are needed to trip.
func TestPrimaryMonitorSuccessClearsFailCountBeforeThreshold(t *testing.T) {
	pm := NewPrimaryMonitor("appdb", nopDial, time.Hour, 3, "SELECT 1",
		slog.New(slog.DiscardHandler))
	pm.recordFailure("a", errors.New("x"))
	pm.recordFailure("b", errors.New("x"))
	pm.recordSuccess()
	pm.recordFailure("c", errors.New("x"))
	require.True(t, pm.Healthy(), "counter was reset; one failure shouldn't trip")
}

// TestPrimaryMonitorRaceFreeStateTransitions hammers concurrent
// recordSuccess + recordFailure to verify the mutex serialises the
// healthy/fails pair (the old atomic.Bool + atomic.Int32 pair had a
// TOCTOU window where Concurrent success could end with healthy=false
// after fails=0 was set).
func TestPrimaryMonitorRaceFreeStateTransitions(t *testing.T) {
	pm := NewPrimaryMonitor("appdb", nopDial, time.Hour, 3, "SELECT 1",
		slog.New(slog.DiscardHandler))

	const N = 2000
	done := make(chan struct{}, 2)
	go func() {
		for i := 0; i < N; i++ {
			pm.recordFailure("x", errors.New("x"))
		}
		done <- struct{}{}
	}()
	go func() {
		for i := 0; i < N; i++ {
			pm.recordSuccess()
		}
		done <- struct{}{}
	}()
	<-done
	<-done

	// After the storm, recordSuccess (which always sets healthy=true)
	// guarantees we end healthy iff the LAST op was a success. We
	// can't predict order, but we CAN assert internal consistency:
	// if healthy==true, fails must be 0.
	pm.state.mu.Lock()
	if pm.state.healthy {
		require.Equal(t, 0, pm.state.fails,
			"healthy=true with non-zero fails indicates a race")
	}
	pm.state.mu.Unlock()
}
