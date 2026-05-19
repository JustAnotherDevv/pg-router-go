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

// defaultReplayable is the default whitelist of GUC names we are willing
// to replay on a fresh backend. Matches the safe-to-replay set the
// PostgreSQL community + PgBouncer's varcache.c agree on: session-scoped,
// idempotent, no side effects beyond setting state.
//
// Names are lowercase; comparison is case-insensitive on input. Keep this
// list small and conservative: an unrecognized SET forces session-pin
// (the client gets correct semantics at the cost of a held backend).
//
// MVP M.10 scope. Operators can extend via config (post-MVP knob;
// PgBouncer's `track_extra_parameters`).
var defaultReplayable = map[string]struct{}{
	"search_path":                         {},
	"application_name":                    {},
	"timezone":                            {},
	"datestyle":                           {},
	"intervalstyle":                       {},
	"extra_float_digits":                  {},
	"statement_timeout":                   {},
	"lock_timeout":                        {},
	"idle_in_transaction_session_timeout": {},
	"client_encoding":                     {},
	"client_min_messages":                 {},
	"default_transaction_isolation":       {},
	"default_transaction_read_only":       {},
	"default_transaction_deferrable":      {},
	"transaction_isolation":               {},
	"transaction_read_only":               {},
	"transaction_deferrable":              {},
	"row_security":                        {},
}

// GUCCache holds the (name, value) pairs a client has SET. Goroutine-
// safe: a separate janitor / metrics goroutine may read snapshots.
//
// Only names in the `replayable` whitelist are remembered. SET of a name
// outside the whitelist flips `unrecognized` to true — PooledConn watches
// this flag and force-session-pins the client so backend state stays
// consistent for the rest of the session.
type GUCCache struct {
	mu           sync.RWMutex
	vars         map[string]string
	replayable   map[string]struct{}
	unrecognized bool
}

// NewGUCCache returns an empty cache using the default replayable
// whitelist.
func NewGUCCache() *GUCCache {
	return &GUCCache{vars: map[string]string{}, replayable: defaultReplayable}
}

// NewGUCCacheWith returns a cache that uses `replayable` as the whitelist.
// Names should be lowercase. Empty map = nothing replayable, every SET
// triggers session-pin.
func NewGUCCacheWith(replayable map[string]struct{}) *GUCCache {
	if replayable == nil {
		replayable = map[string]struct{}{}
	}
	return &GUCCache{vars: map[string]string{}, replayable: replayable}
}

// HasUnrecognizedSet reports whether the client has issued any SET for a
// name outside the replayable whitelist since the last DISCARD ALL.
//
// PooledConn treats this as a session-pin trigger: PgBouncer-style
// pool_mode=transaction can't safely re-apply unknown GUCs across backend
// swaps, so we pin to the current backend for the rest of the session.
func (c *GUCCache) HasUnrecognizedSet() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.unrecognized
}

// ObserveQuery inspects one client Query.String and updates the cache.
// Returns true if the cache was modified (caller may want to invalidate
// any pending replay).
func (c *GUCCache) ObserveQuery(sql string) bool {
	// Fast-path: GUC patterns are DISCARD, RESET, SET — all start with
	// a distinct keyword. If the first keyword is not one of these,
	// skip all 3 regex evaluations (the vast majority of queries).
	if !maybeNeedsGUC(sql) {
		return false
	}
	if discardRe.MatchString(sql) {
		c.mu.Lock()
		modified := len(c.vars) > 0 || c.unrecognized
		c.vars = map[string]string{}
		c.unrecognized = false
		c.mu.Unlock()
		return modified
	}
	if m := resetRe.FindStringSubmatch(sql); m != nil {
		name := strings.ToLower(m[1])
		c.mu.Lock()
		if name == "all" {
			modified := len(c.vars) > 0 || c.unrecognized
			c.vars = map[string]string{}
			c.unrecognized = false
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
		defer c.mu.Unlock()
		if _, ok := c.replayable[name]; !ok {
			// Unknown GUC. Record the trigger so PooledConn pins the
			// session, but DON'T store it — replaying an unknown name
			// across backends would be incorrect.
			c.unrecognized = true
			return true
		}
		old, existed := c.vars[name]
		c.vars[name] = val
		return !existed || old != val
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

// maybeNeedsGUC returns false if the SQL's first keyword provably
// cannot match any GUC pattern (DISCARD, RESET, SET). This skips
// 3 regex evaluations for the vast majority of queries.
func maybeNeedsGUC(sql string) bool {
	i := 0
	for i < len(sql) {
		c := sql[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		if i+1 < len(sql) && c == '-' && sql[i+1] == '-' {
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if i+1 < len(sql) && c == '/' && sql[i+1] == '*' {
			i += 2
			for i+1 < len(sql) && !(sql[i] == '*' && sql[i+1] == '/') {
				i++
			}
			if i+1 < len(sql) {
				i += 2
			} else {
				i = len(sql)
			}
			continue
		}
		break
	}
	if i >= len(sql) {
		return true
	}
	j := i
	for j < len(sql) && sql[j] != ' ' && sql[j] != '\t' && sql[j] != '\n' && sql[j] != '\r' && sql[j] != '(' && sql[j] != ';' {
		j++
	}
	if j == i {
		return true
	}
	var buf [16]byte
	kwLen := j - i
	if kwLen > len(buf) {
		return true
	}
	for k := 0; k < kwLen; k++ {
		c := sql[i+k]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		buf[k] = c
	}
	kw := string(buf[:kwLen])
	switch kw {
	case "DISCARD", "RESET", "SET":
		return true
	}
	return false
}
