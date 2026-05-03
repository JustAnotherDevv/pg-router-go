// Package cancel implements PostgreSQL CancelRequest routing.
//
// Postgres CancelRequest flow:
//
//  1. On successful auth, the server sends BackendKeyData{ProcessID,
//     SecretKey} to the client.
//  2. Later, to cancel a running query, the client opens a NEW
//     connection and sends only a CancelRequest packet containing the
//     same ProcessID + SecretKey. The connection is closed without an
//     auth handshake.
//  3. The server matches (ProcessID, SecretKey) to a running session
//     and signals its query handler.
//
// pgrouter sits in the middle: the BackendKeyData we send to the client
// must use OUR (synthetic) ProcessID + SecretKey — never the upstream's,
// because the cancel will come back to us, not to the upstream. We then
// look up the corresponding backend connection in our Tracker, dial a
// fresh upstream connection, and forward a CancelRequest carrying the
// upstream's real ProcessID + SecretKey.

package cancel

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

// ErrNotFound is returned by Tracker.Lookup when the key is unknown.
var ErrNotFound = errors.New("cancel: key not found")

// Key identifies one tracked client → backend mapping.
type Key struct {
	ProcessID uint32
	SecretKey [4]byte
}

// Target is the upstream coordinates a CancelRequest must be forwarded
// to.
type Target struct {
	// BackendAddr is the upstream "host:port" the cancel side-channel
	// must dial. The cancel does NOT reuse the existing socket — it's
	// always a fresh connection.
	BackendAddr string
	// BackendProcessID + BackendSecretKey are what the upstream told
	// us its real PID/secret are (from its BackendKeyData during auth).
	BackendProcessID uint32
	BackendSecretKey []byte
}

// Tracker maintains the (ourPID, ourSecret) → upstream Target map.
//
// Goroutine-safe. Typical lifecycle:
//   - On client auth complete: Tracker.Allocate() -> (Key, Target slot
//     filled later when backend attaches).
//   - On backend acquire / first attach: Tracker.Bind(Key, Target).
//   - On CancelRequest received: Tracker.Lookup(Key) -> Target, dial,
//     forward.
//   - On client disconnect: Tracker.Release(Key).
type Tracker struct {
	mu sync.RWMutex
	m  map[Key]Target
	// createdAt tracks when each Key was Allocate()'d. The normal path
	// (Release at client disconnect) deletes both entries together.
	// Sweep() drops entries that were Allocated but never Bound and
	// are older than ttl — covers the panic/crash leak path where a
	// servePooled goroutine dies between Allocate and Bind without
	// the deferred Release firing.
	createdAt map[Key]time.Time

	// nowFn allows tests to override time. nil → time.Now.
	nowFn func() time.Time
}

// NewTracker returns an empty tracker.
func NewTracker() *Tracker {
	return &Tracker{
		m:         map[Key]Target{},
		createdAt: map[Key]time.Time{},
	}
}

func (t *Tracker) now() time.Time {
	if t.nowFn != nil {
		return t.nowFn()
	}
	return time.Now()
}

// Allocate generates a fresh (ProcessID, SecretKey) and returns it as a
// Key. The Target is not yet bound — caller calls Bind once it knows
// which backend address + upstream PID/secret to forward to.
//
// We collision-check against the existing map; the 64-bit search space
// makes collisions astronomically unlikely but it's cheap to verify.
func (t *Tracker) Allocate() (Key, error) {
	const maxAttempts = 5
	for i := 0; i < maxAttempts; i++ {
		var buf [8]byte
		if _, err := rand.Read(buf[:]); err != nil {
			return Key{}, err
		}
		pid := binary.BigEndian.Uint32(buf[0:4])
		if pid == 0 {
			pid = 1
		}
		var sec [4]byte
		copy(sec[:], buf[4:8])
		k := Key{ProcessID: pid, SecretKey: sec}
		t.mu.Lock()
		if _, taken := t.m[k]; !taken {
			t.m[k] = Target{}
			t.createdAt[k] = t.now()
			t.mu.Unlock()
			return k, nil
		}
		t.mu.Unlock()
	}
	return Key{}, errors.New("cancel: allocation collision after retries")
}

// Bind associates a Key with the upstream target. Overwrites any
// previous binding (the backend may have changed between txns).
func (t *Tracker) Bind(k Key, target Target) {
	t.mu.Lock()
	t.m[k] = target
	t.mu.Unlock()
}

// Lookup returns the bound target or ErrNotFound.
func (t *Tracker) Lookup(k Key) (Target, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	target, ok := t.m[k]
	if !ok {
		return Target{}, ErrNotFound
	}
	if target.BackendAddr == "" {
		// Allocated but not yet Bound — the cancel arrived before any
		// backend was attached. Treat as not found; the cancel is moot
		// (no query is running).
		return Target{}, ErrNotFound
	}
	return target, nil
}

// Release removes a Key. Safe to call on already-released keys.
func (t *Tracker) Release(k Key) {
	t.mu.Lock()
	delete(t.m, k)
	delete(t.createdAt, k)
	t.mu.Unlock()
}

// Len returns the number of tracked clients (for metrics).
func (t *Tracker) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.m)
}

// SweepUnbound drops entries that were Allocate()'d but never Bind()'d
// and are older than ttl. Returns the number of entries dropped.
//
// Intended to be called periodically by StartSweeper. Manual callers
// (tests) can run it directly. Bound entries are NEVER swept — the
// normal Release path handles those.
func (t *Tracker) SweepUnbound(ttl time.Duration) int {
	if ttl <= 0 {
		return 0
	}
	cutoff := t.now().Add(-ttl)
	t.mu.Lock()
	defer t.mu.Unlock()
	dropped := 0
	for k, created := range t.createdAt {
		if !created.Before(cutoff) {
			continue
		}
		tg, ok := t.m[k]
		if !ok {
			// Inconsistent state — clean up the stale createdAt entry.
			delete(t.createdAt, k)
			continue
		}
		if tg.BackendAddr != "" {
			// Bound; keep. Lifetime managed by Release().
			continue
		}
		delete(t.m, k)
		delete(t.createdAt, k)
		dropped++
	}
	return dropped
}

// StartSweeper spawns a goroutine that calls SweepUnbound every
// `period` until ctx is done. Returns immediately. Pass ctx tied to
// process lifetime; ttl=1h is a sensible default.
func (t *Tracker) StartSweeper(ctx context.Context, period, ttl time.Duration) {
	go func() {
		tk := time.NewTicker(period)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				_ = t.SweepUnbound(ttl)
			}
		}
	}()
}
