// Force-session detection for features that need a stable backend.
//
// In transaction-mode pooling pgrouter releases the backend at every
// idle ReadyForQuery, then re-acquires (possibly a DIFFERENT backend)
// for the next query. Three Postgres features break under that model:
//
//   - LISTEN ch — registers interest in NOTIFY events on a specific
//     backend. After release, subsequent NOTIFY events on that backend
//     will never reach the client.
//   - pg_advisory_lock(...) (session-level) — the lock is held until
//     the session ends. After release another client may dequeue the
//     same backend, see the lock as held by "their" session, then drop
//     it when THEY disconnect.
//   - CREATE TEMP TABLE — backed by a backend-local schema. The next
//     SELECT against the table fails on a different backend.
//
// When PooledConn observes any of these patterns, it flips into
// session-pinned mode: the current backend stays attached for the
// remainder of the client's session, mimicking session-mode pooling
// for that one client. PgBouncer's `server_reset_query_always` +
// `pool_mode=session` flag combo does the same.

package client

import (
	"regexp"
	"time"
)

const pinnedBackendPollInterval = 100 * time.Millisecond

// pinPatterns are the SQL fragments that trigger force-session.
//
// We match plain Query strings. Statements arriving via Parse → Bind →
// Execute would also need to pin; deferred to a future iteration since
// the common case (psql / pgx / drivers calling LISTEN) uses simple
// Query.
//
// Patterns are case-insensitive and ignore SQL comments / leading whitespace
// in the simple way: regex `(?i)`.
var pinPatterns = []*regexp.Regexp{
	// LISTEN <channel>
	regexp.MustCompile(`(?i)\bLISTEN\s+[a-zA-Z_]`),
	// session-level pg_advisory_lock / pg_advisory_lock_shared
	// (the *_xact_lock variants live with the transaction, so they're
	// safe in txn-mode.)
	regexp.MustCompile(`(?i)\bpg_advisory_lock\s*\(`),
	regexp.MustCompile(`(?i)\bpg_advisory_lock_shared\s*\(`),
	regexp.MustCompile(`(?i)\bpg_try_advisory_lock\s*\(`),
	regexp.MustCompile(`(?i)\bpg_try_advisory_lock_shared\s*\(`),
	// CREATE TEMP / TEMPORARY TABLE
	regexp.MustCompile(`(?i)\bCREATE\s+(?:GLOBAL\s+|LOCAL\s+)?(?:TEMP|TEMPORARY)\s+TABLE\b`),
	// DECLARE <name> [BINARY] [INSENSITIVE] [SCROLL] CURSOR WITHOUT HOLD
	// (cursors WITH HOLD are txn-safe; the default "WITHOUT HOLD" is not.)
	// Pattern is permissive: any DECLARE … CURSOR.
	regexp.MustCompile(`(?i)\bDECLARE\s+\w+\s+(?:BINARY\s+|INSENSITIVE\s+|SCROLL\s+|NO\s+SCROLL\s+)*CURSOR\b`),
}

// needsSessionPin returns true if the SQL contains a fragment that
// forces session mode.
//
// False negatives are acceptable here (we miss-detect → client sees
// confusing behaviour). False positives are wasteful but not unsafe
// (extra backend pinning).
func needsSessionPin(sql string) bool {
	// Fast-path: check the first keyword. Pin triggers are LISTEN,
	// pg_advisory_lock*, CREATE TEMP TABLE, DECLARE CURSOR — none of
	// which start with SELECT, INSERT, UPDATE, DELETE, VALUES, TABLE,
	// SHOW, EXPLAIN, WITH, COMMIT, ROLLBACK, BEGIN, or ANALYZE.
	// This skips the 5 regex evaluations for the vast majority of
	// queries without any false negatives.
	if !maybeNeedsPin(sql) {
		return false
	}
	cleaned := stripSQLComments(sql)
	for _, re := range pinPatterns {
		if re.MatchString(cleaned) {
			return true
		}
	}
	return false
}

// maybeNeedsPin returns false if the SQL's first keyword provably
// cannot match any pin pattern. Conservative: returns true for any
// keyword not in the safe list (including function calls like
// pg_advisory_lock which aren't SQL keywords).
func maybeNeedsPin(sql string) bool {
	// Session-level advisory lock helpers can appear anywhere inside a
	// statement (e.g. SELECT pg_try_advisory_lock(42)).
	if containsSessionAdvisoryCall(sql) {
		return true
	}
	kw := firstKeyword(stripLeadingNoise(sql))
	switch kw {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "VALUES", "TABLE",
		"SHOW", "EXPLAIN", "WITH", "COMMIT", "ROLLBACK", "BEGIN",
		"ANALYZE", "VACUUM", "COPY", "LOCK", "FETCH", "MOVE",
		"CLOSE", "DEALLOCATE", "PREPARE", "DO", "GRANT", "REVOKE",
		"COMMENT", "REFRESH", "CLUSTER", "REINDEX", "CHECKPOINT",
		"NOTIFY", "UNLISTEN", "SAVEPOINT", "RELEASE":
		return false
	}
	return true // unknown keyword — could be pin trigger
}

func containsSessionAdvisoryCall(sql string) bool {
	for i := 0; i < len(sql); i++ {
		if !matchFoldAt(sql, i, "pg_advisory_") && !matchFoldAt(sql, i, "pg_try_advisory_") {
			continue
		}
		return true
	}
	return false
}

func matchFoldAt(s string, start int, needle string) bool {
	if start+len(needle) > len(s) {
		return false
	}
	for i := 0; i < len(needle); i++ {
		sb := s[start+i]
		nb := needle[i]
		if sb >= 'A' && sb <= 'Z' {
			sb += 'a' - 'A'
		}
		if sb != nb {
			return false
		}
	}
	return true
}

// stripSQLComments removes `-- ... EOL` line comments and `/* ... */`
// block comments. Block comments are NOT nested per pgwire grammar's
// extended dialect (PostgreSQL allows nesting, but it's vanishingly
// rare in real client queries; we treat all `/*` … `*/` as flat).
func stripSQLComments(sql string) string {
	var b []byte
	for i := 0; i < len(sql); {
		// Line comment.
		if i+1 < len(sql) && sql[i] == '-' && sql[i+1] == '-' {
			// skip to next \n or EOF
			j := i + 2
			for j < len(sql) && sql[j] != '\n' {
				j++
			}
			i = j
			continue
		}
		// Block comment.
		if i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*' {
			j := i + 2
			for j+1 < len(sql) && !(sql[j] == '*' && sql[j+1] == '/') {
				j++
			}
			if j+1 < len(sql) {
				j += 2 // skip closing */
			} else {
				j = len(sql) // unterminated; skip remainder
			}
			i = j
			continue
		}
		b = append(b, sql[i])
		i++
	}
	return string(b)
}
