package testutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/JustAnotherDevv/pgrouter/internal/stats"
)

// ResetStats swaps stats.Reg for a fresh registry and re-runs stats.New
// so the calling test sees zero-valued counters. t.Cleanup restores the
// previous registry (so parallel-test isolation is preserved at end of
// test; the registry is process-global during the test body).
//
// Previously hand-rolled in pooled_phase_a_test.go as
// resetStatsForPhaseATest — hoisted here.
func ResetStats(t *testing.T) {
	t.Helper()
	orig := stats.Reg
	stats.Reg = prometheus.NewRegistry()
	_ = stats.New()
	t.Cleanup(func() { stats.Reg = orig })
}
