// Per-backend state machine. Tracks lifetime + idle clock so
// internal/pool can decide when to retire a backend.

package backend

import (
	"sync/atomic"
	"time"
)

// State is the lifecycle phase of a backend.
type State uint32

const (
	// StateNew has been created but not yet handed to a client.
	StateNew State = iota
	// StateActive is currently servicing a client.
	StateActive
	// StateIdle is waiting in the pool for the next client.
	StateIdle
	// StateClosed is unusable; reaper has dropped it.
	StateClosed
)

// String returns the human-readable name.
func (s State) String() string {
	switch s {
	case StateNew:
		return "new"
	case StateActive:
		return "active"
	case StateIdle:
		return "idle"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Lifecycle is embedded in `Conn` (added separately to avoid changing
// the existing struct shape across milestones). Tracks creation time,
// last-active time, current state.
type Lifecycle struct {
	createdAt   time.Time
	lastActive  atomic.Int64 // unix nanos
	state       atomic.Uint32
	useCount    atomic.Uint64
}

// Init initializes a zero-value Lifecycle in place (avoids heap alloc).
func (l *Lifecycle) Init(now time.Time) {
	l.createdAt = now
	l.lastActive.Store(now.UnixNano())
	l.state.Store(uint32(StateNew))
}

// State returns the current state.
func (l *Lifecycle) State() State { return State(l.state.Load()) }

// CreatedAt returns the creation timestamp (immutable).
func (l *Lifecycle) CreatedAt() time.Time { return l.createdAt }

// LastActive returns the last-active timestamp.
func (l *Lifecycle) LastActive() time.Time {
	return time.Unix(0, l.lastActive.Load())
}

// UseCount returns how many client checkouts this backend has serviced.
func (l *Lifecycle) UseCount() uint64 { return l.useCount.Load() }

// MarkActive transitions to StateActive + bumps last-active.
func (l *Lifecycle) MarkActive(now time.Time) {
	l.state.Store(uint32(StateActive))
	l.lastActive.Store(now.UnixNano())
	l.useCount.Add(1)
}

// MarkIdle transitions to StateIdle + bumps last-active.
func (l *Lifecycle) MarkIdle(now time.Time) {
	l.state.Store(uint32(StateIdle))
	l.lastActive.Store(now.UnixNano())
}

// MarkClosed sets StateClosed (terminal).
func (l *Lifecycle) MarkClosed() {
	l.state.Store(uint32(StateClosed))
}

// ShouldRecycle returns true if the backend has exceeded the
// configured lifetime (`maxLifetime`) — caller should retire it.
func (l *Lifecycle) ShouldRecycle(now time.Time, maxLifetime time.Duration) bool {
	if maxLifetime <= 0 {
		return false
	}
	return now.Sub(l.createdAt) >= maxLifetime
}

// ShouldEvict returns true if the backend has been idle longer than
// `maxIdle` — caller should close + replace.
func (l *Lifecycle) ShouldEvict(now time.Time, maxIdle time.Duration) bool {
	if maxIdle <= 0 || l.State() != StateIdle {
		return false
	}
	return now.Sub(l.LastActive()) >= maxIdle
}
