// Read-only snapshot of the live Prometheus registry.
//
// StatsSnapshot + SnapshotFromRegistry are the cheap-to-serve rollup
// served by GET /api/v1/stats. The HTTP handler (admin.go) calls
// SnapshotFromRegistry, encodes it as JSON, sends it. No write side.
//
// Split out of admin.go in the AL8 refactor: admin.go now strictly
// holds the AdminAPI surface + HTTP handlers; this file holds the
// data shape + the scraper.

package stats

import "time"

// StatsSnapshot is a compact rollup for GET /api/v1/stats. JSON tags
// are a stable downstream contract.
type StatsSnapshot struct {
	UptimeSeconds  float64 `json:"uptime_seconds"`
	ClientsActive  int     `json:"clients_active"`
	BackendsActive int     `json:"backends_active"`
	BackendsIdle   int     `json:"backends_idle"`
	QueriesTotal   float64 `json:"queries_total"`
	TxStartsTotal  float64 `json:"tx_starts_total"`
	PreparedHits   float64 `json:"prepared_hits_total"`
	PreparedMisses float64 `json:"prepared_misses_total"`
}

// SnapshotFromRegistry scrapes the package Reg for a few rollup counts.
// Sums across all label values for vec metrics so the snapshot is
// genuinely "across the whole pgrouter process."
//
// Cheap relative to /metrics: only 7 metric families are inspected
// and only counter/gauge sums are computed.
func SnapshotFromRegistry(uptime time.Duration) StatsSnapshot {
	out := StatsSnapshot{UptimeSeconds: uptime.Seconds()}
	families, err := Reg.Gather()
	if err != nil {
		return out
	}
	for _, mf := range families {
		switch mf.GetName() {
		case "pgrouter_client_active":
			for _, m := range mf.GetMetric() {
				if g := m.GetGauge(); g != nil {
					out.ClientsActive = int(g.GetValue())
				}
			}
		case "pgrouter_backend_active":
			for _, m := range mf.GetMetric() {
				if g := m.GetGauge(); g != nil {
					out.BackendsActive = int(g.GetValue())
				}
			}
		case "pgrouter_backend_idle":
			for _, m := range mf.GetMetric() {
				if g := m.GetGauge(); g != nil {
					out.BackendsIdle = int(g.GetValue())
				}
			}
		case "pgrouter_queries_total":
			for _, m := range mf.GetMetric() {
				if c := m.GetCounter(); c != nil {
					out.QueriesTotal += c.GetValue()
				}
			}
		case "pgrouter_tx_starts_total":
			for _, m := range mf.GetMetric() {
				if c := m.GetCounter(); c != nil {
					out.TxStartsTotal += c.GetValue()
				}
			}
		case "pgrouter_prepared_cache_hits_total":
			for _, m := range mf.GetMetric() {
				if c := m.GetCounter(); c != nil {
					out.PreparedHits += c.GetValue()
				}
			}
		case "pgrouter_prepared_cache_misses_total":
			for _, m := range mf.GetMetric() {
				if c := m.GetCounter(); c != nil {
					out.PreparedMisses += c.GetValue()
				}
			}
		}
	}
	return out
}
