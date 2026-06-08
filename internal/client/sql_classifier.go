// SQL read/write classifier.
//
// Scans the first SQL keyword (after stripping leading whitespace +
// comments + CTE prefixes) and returns Read / Write. Pure lexical â€”
// no real parser; false positives (treat-as-write) are preferred to
// false negatives (let a write hit a replica).
//
// Read keywords:   SELECT, VALUES, TABLE, SHOW, EXPLAIN
// Write keywords:  INSERT, UPDATE, DELETE, MERGE, COPY, CALL, EXEC,
//                  TRUNCATE, CREATE, DROP, ALTER, GRANT, REVOKE,
//                  COMMENT, VACUUM, ANALYZE, REINDEX, LOCK, REFRESH,
//                  CLUSTER, NOTIFY, LISTEN, UNLISTEN, BEGIN, COMMIT,
//                  ROLLBACK, SAVEPOINT, RELEASE, SET, RESET, DISCARD,
//                  PREPARE, DEALLOCATE, FETCH, MOVE, DECLARE, CLOSE,
//                  CHECKPOINT, DO, IMPORT, SECURITY
//
// WITH .. is parsed enough to peek the inner statement:
//   WITH foo AS (...) SELECT ...   â†’ Read
//   WITH foo AS (...) UPDATE ...   â†’ Write
//
// EXPLAIN ANALYZE <stmt> is Write because ANALYZE writes â€” we look
// at the inner statement for non-ANALYZE EXPLAIN.

package client

import (
	"strings"

	"github.com/JustAnotherDevv/pg-router-go/internal/util"
)

// SQLOp is the classification verdict.
type SQLOp int

const (
	SQLOpUnknown SQLOp = iota
	SQLOpRead
	SQLOpWrite
)

// SQLInfo holds all per-message SQL analysis results computed in a
// single pass. Avoids redundant keyword extraction, comment stripping,
// and regex evaluation across GUC cache, session pin, classifier, and
// statement-mode checks.
type SQLInfo struct {
	Keyword   string // first keyword uppercased (e.g. "SELECT")
	Op        SQLOp  // Read / Write / Unknown
	IsROBegin bool   // read-only BEGIN variant
	NeedsPin  bool   // session-pin trigger detected
	NeedsGUC  bool   // DISCARD / RESET / SET detected
	HasPgAdv  bool   // pg_advisory* function call present
	Stripped  string // SQL with leading whitespace+comments stripped
	HasSQL    bool   // true if sql was non-empty
}

// AnalyzeSQL computes all per-message SQL results in one pass:
//   - strips leading whitespace + comments once
//   - extracts first keyword once (uppercased)
//   - classifies Read/Write
//   - checks readOnlyBegin regex (only for BEGIN variants)
//   - checks GUC patterns (DISCARD/RESET/SET keyword)
//   - checks session-pin patterns (keyword + pg_advisory substring)
//
// This replaces 3 independent keyword extractions + 6 regex evals
// with a single scan.
func AnalyzeSQL(sql string) SQLInfo {
	if len(sql) == 0 {
		return SQLInfo{Op: SQLOpUnknown}
	}
	s := stripLeadingNoise(sql)
	kw := firstKeyword(s)
	if len(kw) == 0 {
		return SQLInfo{Stripped: s, HasSQL: true, Op: SQLOpUnknown}
	}

	// Classification (1 pass, matches classifyStripped logic).
	var op SQLOp
	switch kw {
	case "SELECT", "VALUES", "TABLE", "SHOW":
		op = SQLOpRead
	case "EXPLAIN":
		op = classifyExplain(s)
	case "WITH":
		op = classifyCTE(s)
	default:
		op = SQLOpWrite
	}

	// readOnly BEGIN check â€” only run for BEGIN-like keywords.
	isROBegin := false
	if kw == "BEGIN" || kw == "START" {
		isROBegin = readOnlyBeginRE.MatchString(s)
	}

	// GUC check â€” keyword is DISCARD, RESET, or SET.
	needsGUC := kw == "DISCARD" || kw == "RESET" || kw == "SET"

	// Session-pin check â€” keyword-based fast-path + pg_advisory scan.
	needsPin := false
	hasPgAdv := false
	for i := 0; i+10 <= len(sql); i++ {
		if sql[i] == 'p' && sql[i+1] == 'g' && sql[i+2] == '_' &&
			(sql[i+3] == 'a' || sql[i+3] == 'A') &&
			(sql[i+4] == 'd' || sql[i+4] == 'D') {
			hasPgAdv = true
			needsPin = true
			break
		}
	}
	if !needsPin {
		switch kw {
		case "LISTEN", "CREATE", "DECLARE":
			needsPin = true
		}
	}

	return SQLInfo{
		Keyword:   kw,
		Op:        op,
		IsROBegin: isROBegin,
		NeedsPin:  needsPin,
		NeedsGUC:  needsGUC,
		HasPgAdv:  hasPgAdv,
		Stripped:  s,
		HasSQL:    true,
	}
}

// ClassifySQL returns Read / Write / Unknown for sql.
// Empty / whitespace-only input â†’ Unknown.
func ClassifySQL(sql string) SQLOp {
	s := stripLeadingNoise(sql) // reused from statement_mode.go
	return classifyStripped(s)
}

// classifyStripped is ClassifySQL on already-stripped input. Lets
// ClassifyDetail share the strip pass with the BEGIN-READ-ONLY check.
func classifyStripped(s string) SQLOp {
	if s == "" {
		return SQLOpUnknown
	}
	kw := firstKeyword(s)
	switch kw {
	case "SELECT", "VALUES", "TABLE", "SHOW":
		return SQLOpRead
	case "EXPLAIN":
		return classifyExplain(s)
	case "WITH":
		return classifyCTE(s)
	case "":
		return SQLOpUnknown
	}
	// Conservative default: everything else is a write.
	return SQLOpWrite
}

// ClassifyDetail returns ClassifySQL + IsExplicitReadOnlyBeginSQL in
// one stripLeadingNoise pass. PooledConn.Serve calls both per Query/
// Parse; before this, the SQL prefix was scanned twice for whitespace
// + comments per message.
func ClassifyDetail(sql string) (op SQLOp, isROBegin bool) {
	s := stripLeadingNoise(sql)
	op = classifyStripped(s)
	isROBegin = readOnlyBeginRE.MatchString(s)
	return
}

// eqFold returns true if a equals b case-insensitively.
// a and b must be ASCII identifier bytes.
func eqFold(a, b byte) bool { return util.EqFold(a, b) }

// firstKeyword returns the first SQL token uppercased.
// Uses byte-level comparison to avoid strings.ToUpper allocation.
// Returns canonical uppercase string for known keywords, or does a
// single lowercase-byte scan for the fallback path (still cheaper than
// strings.ToUpper which allocates + scans twice).
func firstKeyword(s string) string {
	i := 0
	for i < len(s) && isIdentByte(s[i]) {
		i++
	}
	if i == 0 {
		return ""
	}
	// Direct byte comparisons â€” no allocation, no ToUpper.
	// Only check keywords used by AnalyzeSQL + classifier switches.
	switch i {
	case 2:
		if eqFold(s[0], 'd') && eqFold(s[1], 'o') {
			return "DO"
		}
	case 3:
		if eqFold(s[0], 's') && eqFold(s[1], 'e') && eqFold(s[2], 't') {
			return "SET"
		}
	case 4:
		if eqFold(s[0], 's') && eqFold(s[1], 'h') && eqFold(s[2], 'o') && eqFold(s[3], 'w') {
			return "SHOW"
		}
		if eqFold(s[0], 'c') && eqFold(s[1], 'a') && eqFold(s[2], 'l') && eqFold(s[3], 'l') {
			return "CALL"
		}
		if eqFold(s[0], 'c') && eqFold(s[1], 'o') && eqFold(s[2], 'p') && eqFold(s[3], 'y') {
			return "COPY"
		}
		if eqFold(s[0], 'l') && eqFold(s[1], 'o') && eqFold(s[2], 'c') && eqFold(s[3], 'k') {
			return "LOCK"
		}
	case 5:
		if eqFold(s[0], 'b') && eqFold(s[1], 'e') && eqFold(s[2], 'g') && eqFold(s[3], 'i') && eqFold(s[4], 'n') {
			return "BEGIN"
		}
		if eqFold(s[0], 'f') && eqFold(s[1], 'e') && eqFold(s[2], 't') && eqFold(s[3], 'c') && eqFold(s[4], 'h') {
			return "FETCH"
		}
		if eqFold(s[0], 'm') && eqFold(s[1], 'e') && eqFold(s[2], 'r') && eqFold(s[3], 'g') && eqFold(s[4], 'e') {
			return "MERGE"
		}
		if eqFold(s[0], 'c') && eqFold(s[1], 'l') && eqFold(s[2], 'o') && eqFold(s[3], 's') && eqFold(s[4], 'e') {
			return "CLOSE"
		}
		if eqFold(s[0], 's') && eqFold(s[1], 't') && eqFold(s[2], 'a') && eqFold(s[3], 'r') && eqFold(s[4], 't') {
			return "START"
		}
		if eqFold(s[0], 't') && eqFold(s[1], 'a') && eqFold(s[2], 'b') && eqFold(s[3], 'l') && eqFold(s[4], 'e') {
			return "TABLE"
		}
		if eqFold(s[0], 'r') && eqFold(s[1], 'e') && eqFold(s[2], 's') && eqFold(s[3], 'e') && eqFold(s[4], 't') {
			return "RESET"
		}
	case 6:
		if eqFold(s[0], 's') && eqFold(s[1], 'e') && eqFold(s[2], 'l') && eqFold(s[3], 'e') && eqFold(s[4], 'c') && eqFold(s[5], 't') {
			return "SELECT"
		}
		if eqFold(s[0], 'u') && eqFold(s[1], 'p') && eqFold(s[2], 'd') && eqFold(s[3], 'a') && eqFold(s[4], 't') && eqFold(s[5], 'e') {
			return "UPDATE"
		}
		if eqFold(s[0], 'd') && eqFold(s[1], 'e') && eqFold(s[2], 'l') && eqFold(s[3], 'e') && eqFold(s[4], 't') && eqFold(s[5], 'e') {
			return "DELETE"
		}
		if eqFold(s[0], 'v') && eqFold(s[1], 'a') && eqFold(s[2], 'l') && eqFold(s[3], 'u') && eqFold(s[4], 'e') && eqFold(s[5], 's') {
			return "VALUES"
		}
		if eqFold(s[0], 'c') && eqFold(s[1], 'r') && eqFold(s[2], 'e') && eqFold(s[3], 'a') && eqFold(s[4], 't') && eqFold(s[5], 'e') {
			return "CREATE"
		}
		if eqFold(s[0], 'r') && eqFold(s[1], 'e') && eqFold(s[2], 'v') && eqFold(s[3], 'o') && eqFold(s[4], 'k') && eqFold(s[5], 'e') {
			return "REVOKE"
		}
		if eqFold(s[0], 'c') && eqFold(s[1], 'o') && eqFold(s[2], 'm') && eqFold(s[3], 'm') && eqFold(s[4], 'i') && eqFold(s[5], 't') {
			return "COMMIT"
		}
		if eqFold(s[0], 'l') && eqFold(s[1], 'i') && eqFold(s[2], 's') && eqFold(s[3], 't') && eqFold(s[4], 'e') && eqFold(s[5], 'n') {
			return "LISTEN"
		}
	case 7:
		if eqFold(s[0], 'i') && eqFold(s[1], 'n') && eqFold(s[2], 's') && eqFold(s[3], 'e') && eqFold(s[4], 'r') && eqFold(s[5], 't') {
			return "INSERT"
		}
		if eqFold(s[0], 'e') && eqFold(s[1], 'x') && eqFold(s[2], 'p') && eqFold(s[3], 'l') && eqFold(s[4], 'a') && eqFold(s[5], 'i') && eqFold(s[6], 'n') {
			return "EXPLAIN"
		}
		if eqFold(s[0], 'd') && eqFold(s[1], 'e') && eqFold(s[2], 'c') && eqFold(s[3], 'l') && eqFold(s[4], 'a') && eqFold(s[5], 'r') && eqFold(s[6], 'e') {
			return "DECLARE"
		}
		if eqFold(s[0], 'c') && eqFold(s[1], 'l') && eqFold(s[2], 'u') && eqFold(s[3], 's') && eqFold(s[4], 't') && eqFold(s[5], 'e') && eqFold(s[6], 'r') {
			return "CLUSTER"
		}
		if eqFold(s[0], 'p') && eqFold(s[1], 'r') && eqFold(s[2], 'e') && eqFold(s[3], 'p') && eqFold(s[4], 'a') && eqFold(s[5], 'r') && eqFold(s[6], 'e') {
			return "PREPARE"
		}
		if eqFold(s[0], 'r') && eqFold(s[1], 'e') && eqFold(s[2], 'i') && eqFold(s[3], 'n') && eqFold(s[4], 'd') && eqFold(s[5], 'e') && eqFold(s[6], 'x') {
			return "REINDEX"
		}
		if eqFold(s[0], 'c') && eqFold(s[1], 'o') && eqFold(s[2], 'm') && eqFold(s[3], 'm') && eqFold(s[4], 'e') && eqFold(s[5], 'n') && eqFold(s[6], 't') {
			return "COMMENT"
		}
		if eqFold(s[0], 'r') && eqFold(s[1], 'e') && eqFold(s[2], 'f') && eqFold(s[3], 'r') && eqFold(s[4], 'e') && eqFold(s[5], 's') && eqFold(s[6], 'h') {
			return "REFRESH"
		}
		if eqFold(s[0], 'd') && eqFold(s[1], 'i') && eqFold(s[2], 's') && eqFold(s[3], 'c') && eqFold(s[4], 'a') && eqFold(s[5], 'r') && eqFold(s[6], 'd') {
			return "DISCARD"
		}
		if eqFold(s[0], 'a') && eqFold(s[1], 'n') && eqFold(s[2], 'a') && eqFold(s[3], 'l') && eqFold(s[4], 'y') && eqFold(s[5], 'z') && eqFold(s[6], 'e') {
			return "ANALYZE"
		}
		if eqFold(s[0], 'r') && eqFold(s[1], 'e') && eqFold(s[2], 'l') && eqFold(s[3], 'e') && eqFold(s[4], 'a') && eqFold(s[5], 's') && eqFold(s[6], 'e') {
			return "RELEASE"
		}
	case 8:
		if eqFold(s[0], 'r') && eqFold(s[1], 'o') && eqFold(s[2], 'l') && eqFold(s[3], 'l') && eqFold(s[4], 'b') && eqFold(s[5], 'a') && eqFold(s[6], 'c') && eqFold(s[7], 'k') {
			return "ROLLBACK"
		}
		if eqFold(s[0], 't') && eqFold(s[1], 'r') && eqFold(s[2], 'u') && eqFold(s[3], 'n') && eqFold(s[4], 'c') && eqFold(s[5], 'a') && eqFold(s[6], 't') && eqFold(s[7], 'e') {
			return "TRUNCATE"
		}
		if eqFold(s[0], 'u') && eqFold(s[1], 'n') && eqFold(s[2], 'l') && eqFold(s[3], 'i') && eqFold(s[4], 's') && eqFold(s[5], 't') && eqFold(s[6], 'e') && eqFold(s[7], 'n') {
			return "UNLISTEN"
		}
		if eqFold(s[0], 's') && eqFold(s[1], 'e') && eqFold(s[2], 'c') && eqFold(s[3], 'u') && eqFold(s[4], 'r') && eqFold(s[5], 'i') && eqFold(s[6], 't') && eqFold(s[7], 'y') {
			return "SECURITY"
		}
	case 9:
		if eqFold(s[0], 's') && eqFold(s[1], 'a') && eqFold(s[2], 'v') && eqFold(s[3], 'e') && eqFold(s[4], 'p') && eqFold(s[5], 'o') && eqFold(s[6], 'i') && eqFold(s[7], 'n') && eqFold(s[8], 't') {
			return "SAVEPOINT"
		}
		if eqFold(s[0], 'd') && eqFold(s[1], 'e') && eqFold(s[2], 'a') && eqFold(s[3], 'l') && eqFold(s[4], 'l') && eqFold(s[5], 'o') && eqFold(s[6], 'c') && eqFold(s[7], 'a') && eqFold(s[8], 't') {
			return "DEALLOCATE"
		}
	case 10:
		if eqFold(s[0], 'c') && eqFold(s[1], 'h') && eqFold(s[2], 'e') && eqFold(s[3], 'c') && eqFold(s[4], 'k') && eqFold(s[5], 'p') && eqFold(s[6], 'o') && eqFold(s[7], 'i') && eqFold(s[8], 'n') && eqFold(s[9], 't') {
			return "CHECKPOINT"
		}
	}
	// Fallback: single-pass uppercase into a stack-sized buffer.
	var buf [32]byte
	n := 0
	for _, c := range s[:i] {
		if c >= 'a' && c <= 'z' {
			if n < len(buf) {
				buf[n] = byte(c - 32)
			}
		} else {
			if n < len(buf) {
				buf[n] = byte(c)
			}
		}
		n++
	}
	return string(buf[:n])
}

// classifyExplain inspects the part after EXPLAIN [ANALYZE [VERBOSE...]].
// ANALYZE makes it a write; otherwise classify the inner statement.
func classifyExplain(s string) SQLOp {
	// Skip "EXPLAIN " and any (option, option) block.
	s = strings.TrimSpace(s[len("EXPLAIN"):])
	for {
		s = stripLeadingNoise(s)
		if s == "" {
			return SQLOpUnknown
		}
		if s[0] == '(' {
			// Skip to matching ).
			depth := 1
			i := 1
			for i < len(s) && depth > 0 {
				switch s[i] {
				case '(':
					depth++
				case ')':
					depth--
				}
				i++
			}
			// If options contained ANALYZE, EXPLAIN writes.
			if containsKeyword(strings.ToUpper(s[:i]), "ANALYZE") {
				return SQLOpWrite
			}
			s = s[i:]
			continue
		}
		kw := firstKeyword(s)
		if kw == "ANALYZE" {
			return SQLOpWrite
		}
		// Recurse into inner statement.
		return ClassifySQL(s)
	}
}

// classifyCTE walks past WITH name AS (...) [, name AS (...)]* to find
// the underlying statement, then classifies it.
func classifyCTE(s string) SQLOp {
	// Drop "WITH" + optional "RECURSIVE".
	s = strings.TrimSpace(s[len("WITH"):])
	s = stripLeadingNoise(s)
	if strings.HasPrefix(strings.ToUpper(s), "RECURSIVE") {
		s = stripLeadingNoise(s[len("RECURSIVE"):])
	}
	// Skip the CTE list: parens balanced.
	for {
		// name
		i := 0
		for i < len(s) && (isIdentByte(s[i]) || s[i] == '"') {
			i++
		}
		s = stripLeadingNoise(s[i:])
		if !strings.HasPrefix(strings.ToUpper(s), "AS") {
			return SQLOpUnknown
		}
		s = stripLeadingNoise(s[2:])
		// Optional MATERIALIZED / NOT MATERIALIZED.
		for _, kw := range []string{"NOT MATERIALIZED", "MATERIALIZED"} {
			if strings.HasPrefix(strings.ToUpper(s), kw) {
				s = stripLeadingNoise(s[len(kw):])
				break
			}
		}
		if s == "" || s[0] != '(' {
			return SQLOpUnknown
		}
		depth := 1
		j := 1
		for j < len(s) && depth > 0 {
			switch s[j] {
			case '(':
				depth++
			case ')':
				depth--
			}
			j++
		}
		s = stripLeadingNoise(s[j:])
		if strings.HasPrefix(s, ",") {
			s = stripLeadingNoise(s[1:])
			continue
		}
		// Inner statement.
		return ClassifySQL(s)
	}
}

// containsKeyword does a whole-word search inside an uppercased
// expression body.
func containsKeyword(haystack, needle string) bool {
	i := 0
	for i < len(haystack) {
		j := strings.Index(haystack[i:], needle)
		if j < 0 {
			return false
		}
		j += i
		left := j == 0 || !isIdentByte(haystack[j-1])
		right := j+len(needle) >= len(haystack) || !isIdentByte(haystack[j+len(needle)])
		if left && right {
			return true
		}
		i = j + len(needle)
	}
	return false
}
