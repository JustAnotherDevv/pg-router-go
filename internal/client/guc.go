// Per-client GUC (parameter / SESSION variable) tracking.
//
// In transaction-mode pooling a client may set a session-level GUC like
//   SET search_path = my_schema;
//   SET timezone = 'UTC';
// and expect it to persist across subsequent transactions, even when
// the underlying backend changes between transactions.
//
// PgBouncer's varcache.c handles this by parsing SET statements out of
// the client's traffic, recording (name, value) pairs in a per-client
// map, then replaying them on every backend acquire.
//
// MVP M.10 scope:
//   - Detect SET via a regex on plain Query strings (the common case).
//   - Honour RESET / RESET ALL / DISCARD ALL.
//   - Replay via a single Query string that the caller fires on the
//     backend before passing it to the client.
//
// Out of scope:
//   - Detecting SET via the extended protocol (Parse + Bind). Rare in
//     practice — pgx uses simple Query for these.
//   - Transactional SET (SET LOCAL) — we ignore these; they live and
//     die with their txn, which is correct.

package client

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// setRe matches:
//   SET key = value
//   SET key TO value
//   SET SESSION key = value
//   SET LOCAL key = value      (we skip; transaction-scoped)
// Quoted values are accepted but we capture the raw RHS — backend will
// re-parse it on replay.
//
// Case-insensitive. Anchored to start (after optional whitespace).
var setRe = regexp.MustCompile(`(?i)^\s*SET\s+(LOCAL\s+|SESSION\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=|\bTO\b)\s*(.+?)\s*;?\s*$`)

// resetRe matches RESET <name> | RESET ALL.
var resetRe = regexp.MustCompile(`(?i)^\s*RESET\s+([a-zA-Z_][a-zA-Z0-9_]*|ALL)\s*;?\s*$`)

// discardRe matches DISCARD ALL (clears everything we track).
var discardRe = regexp.MustCompile(`(?i)^\s*DISCARD\s+ALL\s*;?\s*$`)

// GUCCache holds the (name, value) pairs a client has SET. Goroutine-
// safe: a separate janitor / metrics goroutine may read snapshots.
type GUCCache struct {
	mu   sync.RWMutex
	vars map[string]string
}

// NewGUCCache returns an empty cache.
func NewGUCCache() *GUCCache {
	return &GUCCache{vars: map[string]string{}}
}

// ObserveQuery inspects one client Query.String and updates the cache.
// Returns true if the cache was modified (caller may want to invalidate
// any pending replay).
func (c *GUCCache) ObserveQuery(sql string) bool {
	if discardRe.MatchString(sql) {
		c.mu.Lock()
		modified := len(c.vars) > 0
		c.vars = map[string]string{}
		c.mu.Unlock()
		return modified
	}
	if m := resetRe.FindStringSubmatch(sql); m != nil {
		name := strings.ToLower(m[1])
		c.mu.Lock()
		if name == "all" {
			modified := len(c.vars) > 0
			c.vars = map[string]string{}
			c.mu.Unlock()
			return modified
		}
		_, existed := c.vars[name]
		delete(c.vars, name)
		c.mu.Unlock()
		return existed
	}
	if m := setRe.FindStringSubmatch(sql); m != nil {
		scope := strings.ToUpper(strings.TrimSpace(m[1]))
		if scope == "LOCAL" {
			return false // txn-scoped; not our concern
		}
		name := strings.ToLower(m[2])
		val := strings.TrimSpace(m[3])
		c.mu.Lock()
		old, existed := c.vars[name]
		c.vars[name] = val
		modified := !existed || old != val
		c.mu.Unlock()
		return modified
	}
	return false
}

// Snapshot returns a copy of the current (name -> value) map.
func (c *GUCCache) Snapshot() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]string, len(c.vars))
	for k, v := range c.vars {
		out[k] = v
	}
	return out
}

// Len returns the number of tracked variables.
func (c *GUCCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.vars)
}

// ReplayQuery returns a single SQL string that re-applies every tracked
// SET. Empty string if nothing tracked. Caller fires this on the
// backend before handing it to the client.
//
// Output: "SET var1=val1; SET var2=val2; ..."  (semicolons separate.)
func (c *GUCCache) ReplayQuery() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.vars) == 0 {
		return ""
	}
	var b strings.Builder
	for name, val := range c.vars {
		fmt.Fprintf(&b, "SET %s=%s; ", name, val)
	}
	return strings.TrimSuffix(b.String(), " ")
}
