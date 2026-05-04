// SQL read/write classifier.
//
// Scans the first SQL keyword (after stripping leading whitespace +
// comments + CTE prefixes) and returns Read / Write. Pure lexical —
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
//   WITH foo AS (...) SELECT ...   → Read
//   WITH foo AS (...) UPDATE ...   → Write
//
// EXPLAIN ANALYZE <stmt> is Write because ANALYZE writes — we look
// at the inner statement for non-ANALYZE EXPLAIN.

package client

import "strings"

// SQLOp is the classification verdict.
type SQLOp int

const (
	SQLOpUnknown SQLOp = iota
	SQLOpRead
	SQLOpWrite
)

// ClassifySQL returns Read / Write / Unknown for sql.
// Empty / whitespace-only input → Unknown.
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

// firstKeyword returns the first SQL token uppercased.
func firstKeyword(s string) string {
	i := 0
	for i < len(s) && isIdentByte(s[i]) {
		i++
	}
	return strings.ToUpper(s[:i])
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
