// Package stats exposes pgrouter's metrics surface: Prometheus exporter,
// connection / pool / timing histograms, and (post-MVP) OpenTelemetry
// traces.
//
// MVP scope (M.13):
//   - Prometheus exporter on a configurable :port
//   - connection metrics (total, active, idle, waiting)
//   - timing histograms (acquire_wait, query, backend_dial)
//   - pool metrics (size, in_use, evictions)
package stats
