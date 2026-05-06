// Package statreset provides ResetStats for unit tests that touch the
// process-global prometheus registry. It lives in a sub-package because
// `internal/testutil` (the leaf helpers) cannot import `internal/stats`
// without forming a cycle: stats' own tests use testutil.Discard.
package statreset

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/JustAnotherDevv/pgrouter/internal/stats"
)

// ResetStats swaps stats.Reg for a fresh registry and re-runs stats.New
// so the calling test sees zero-valued counters. t.Cleanup restores the
// previous registry.
//
// Hoisted from pooled_phase_a_test.go where it was resetStatsForPhaseATest.
func ResetStats(t *testing.T) {
	t.Helper()
	orig := stats.Reg
	stats.Reg = prometheus.NewRegistry()
	_ = stats.New()
	t.Cleanup(func() { stats.Reg = orig })
}
