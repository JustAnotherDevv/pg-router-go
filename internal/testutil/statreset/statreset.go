// Package statreset provides ResetStats for unit tests that touch the
// process-global prometheus registry. It lives in a sub-package because
// `internal/testutil` (the leaf helpers) cannot import `internal/stats`
// without forming a cycle: stats' own tests use testutil.Discard.
package statreset

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pg-router-go/internal/stats"
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

// GetCounter gathers stats.Reg and returns the float value of the
// counter named `name` whose label set is a superset of `labels`
// (subset-match). Returns 0 if no series matches.
//
// Hoisted from two near-identical helpers (gatherCounter in
// pooled_phase_a_test.go and getCounter+labelsMatch in
// cmd/pgrouter/sighup_test.go).
func GetCounter(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := stats.Reg.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), labels) {
				if c := m.GetCounter(); c != nil {
					return c.GetValue()
				}
			}
		}
	}
	return 0
}

func labelsMatch(have []*dto.LabelPair, want map[string]string) bool {
	got := make(map[string]string, len(have))
	for _, lp := range have {
		got[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}
