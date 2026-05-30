// Token bucket rate limiter.
//
// Used by pgrouter for per-(database, user) QPS caps. One bucket per
// tenant, refilled lazily on Take(). Thread-safe.

package util

import (
	"sync"
	"time"
)

// TokenBucket is a classic token bucket: `capacity` tokens accrue at
// `rate` tokens/sec, capped at `capacity`. Take() consumes one token
// and returns true; if the bucket is empty it returns false (no wait).
//
// Lazy refill: tokens advance based on wall-clock elapsed since the
// last Take, so we don't need a goroutine ticker.
type TokenBucket struct {
	mu       sync.Mutex
	capacity float64
	rate     float64
	tokens   float64
	last     time.Time
}

// NewTokenBucket creates a bucket with the given capacity (burst size)
// and refill rate (tokens / second).
func NewTokenBucket(capacity, ratePerSec float64) *TokenBucket {
	return &TokenBucket{
		capacity: capacity,
		rate:     ratePerSec,
		tokens:   capacity,
		last:     time.Now(),
	}
}

// Take consumes one token. Returns false if the bucket is empty.
func (b *TokenBucket) Take() bool {
	return b.TakeN(1)
}

// TakeN consumes n tokens. Returns false if fewer than n are available.
func (b *TokenBucket) TakeN(n float64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	b.last = now
	b.tokens += elapsed * b.rate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	if b.tokens < n {
		return false
	}
	b.tokens -= n
	return true
}

// Available reports the current token balance (for metrics / debug).
func (b *TokenBucket) Available() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	avail := b.tokens + elapsed*b.rate
	if avail > b.capacity {
		avail = b.capacity
	}
	return avail
}
