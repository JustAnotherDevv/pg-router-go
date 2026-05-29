// Package pool implements per-(db, user) connection pools.
//
// MVP scope:
//   - M.8.1: Pool struct + registry
//   - M.8.2: FIFO wait queue with timeout
//   - M.8.3: per-pool sizing (default, min, reserve)
//   - M.8.4: backend spawn/reap
//   - M.8.5: graceful drain on shutdown
//   - M.8.6: multiple pools per database (per-user isolation)
//
// Post-MVP: read-replica routing, sharding, multi-tenant quotas.
package pool
