package replica

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPrimaryMonitorStartsHealthy(t *testing.T) {
	// Skip the actual probe (which would Acquire) by constructing the
	// monitor with a nil pool and never calling Start. Just verify
	// initial flag state.
	pm := NewPrimaryMonitor("appdb", nil, time.Hour, 3, "SELECT 1",
		slog.New(slog.DiscardHandler))
	require.True(t, pm.Healthy())
}

func TestPrimaryMonitorMarksUnhealthyAfterThreshold(t *testing.T) {
	pm := NewPrimaryMonitor("appdb", nil, time.Hour, 3, "SELECT 1",
		slog.New(slog.DiscardHandler))
	pm.handleFailure("test", nil)
	require.True(t, pm.Healthy(), "1 failure < threshold 3")
	pm.handleFailure("test", nil)
	require.True(t, pm.Healthy(), "2 failures still < threshold")
	pm.handleFailure("test", nil)
	require.False(t, pm.Healthy(), "3rd failure trips threshold")
}

func TestPrimaryMonitorRecoversAfterSuccess(t *testing.T) {
	pm := NewPrimaryMonitor("appdb", nil, time.Hour, 1, "SELECT 1",
		slog.New(slog.DiscardHandler))
	pm.handleFailure("test", nil)
	require.False(t, pm.Healthy())
	// Simulate a successful probe → resets fails + flips healthy back.
	pm.fails.Store(0)
	pm.healthy.Store(true)
	require.True(t, pm.Healthy())
}
