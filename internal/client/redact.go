// SQL literal redaction for safe logging.
//
// Pgrouter never persists SQL, but we do emit a debug-friendly
// `sql=...` field on every Query / Parse log line. Real workloads
// embed PII inside literals (`WHERE email='alice@example.com'`,
// `INSERT ... VALUES (..., '4111111111111111')`), so the default
// LogSQL mode is "redacted": we replace literals with `?` before
// the SQL ever reaches the log writer.
//
// This is a scan-style redactor; it does not try to parse the SQL.
// The goal is "no literal bytes leak into the log", not a perfectly
// reformatted statement. False positives (over-redaction) are OK,
// false negatives (a literal that escapes redaction) are not.
//
// Recognised literal forms:
//   - single-quoted string             '...'        with '' escape
//   - dollar-quoted string             $tag$...$tag$
//   - C-style E'...' / e'...' strings  with backslash escapes
//   - decimal / hex / float numerics   123, 0xFF, 3.14e-5
//   - bind-parameter placeholders      $1, $42 (kept verbatim)
//
// SQL identifiers ("col_name", schema.table) and double-quoted
// identifiers ("My Col") are kept verbatim because they're rarely PII.
// Comments (-- ... and /* ... */) are also kept; operators sometimes
// embed query hints there and they shouldn't carry secrets.

package client

import (
	"strings"

	"github.com/JustAnotherDevv/pg-router-go/internal/util"
)

// RedactSQL returns sql with every recognised literal replaced by `?`.
// Bind parameters ($1, $42) are kept verbatim because they don't carry
// data, only the placeholder slot index.
func RedactSQL(sql string) string {
	if sql == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(sql))
	i := 0
	n := len(sql)
	for i < n {
		c := sql[i]
		switch {
		case c == '\'':
			// single-quoted string with '' escape
			j := i + 1
			for j < n {
				if sql[j] == '\'' {
					if j+1 < n && sql[j+1] == '\'' {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			b.WriteByte('?')
			i = j
		case (c == 'E' || c == 'e') && i+1 < n && sql[i+1] == '\'':
			// E'...' or e'...' with backslash escapes
			j := i + 2
			for j < n {
				if sql[j] == '\\' && j+1 < n {
					j += 2
					continue
				}
				if sql[j] == '\'' {
					if j+1 < n && sql[j+1] == '\'' {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			b.WriteByte('?')
			i = j
		case c == '$':
			// Distinguish bind-param ($1, $42) from dollar-quoted
			// ($tag$body$tag$, $$body$$). A bind-param is followed by a
			// digit; a dollar-quote tag is empty or [A-Za-z_].
			if i+1 < n && isDigit(sql[i+1]) {
				j := i + 1
				for j < n && isDigit(sql[j]) {
					j++
				}
				b.WriteString(sql[i:j]) // keep $1, $42 verbatim
				i = j
				continue
			}
			tagEnd := i + 1
			for tagEnd < n && sql[tagEnd] != '$' {
				if !isIdentByte(sql[tagEnd]) {
					// Not a dollar-quote opener; emit the `$` raw.
					b.WriteByte('$')
					i++
					continue
				}
				tagEnd++
			}
			if tagEnd >= n {
				// Unclosed tag: preserve to make the bug visible.
				b.WriteString(sql[i:])
				i = n
				continue
			}
			tag := sql[i : tagEnd+1] // includes both $'s
			closeIdx := strings.Index(sql[tagEnd+1:], tag)
			if closeIdx < 0 {
				// No matching close: preserve to make the bug visible.
				b.WriteString(sql[i:])
				i = n
				continue
			}
			b.WriteByte('?')
			i = tagEnd + 1 + closeIdx + len(tag)
		case c == '-' && i+1 < n && sql[i+1] == '-':
			// -- line comment
			j := i + 2
			for j < n && sql[j] != '\n' {
				j++
			}
			b.WriteString(sql[i:j]) // keep comments
			i = j
		case c == '/' && i+1 < n && sql[i+1] == '*':
			// /* block comment */ (nested per PG)
			j := i + 2
			depth := 1
			for j < n && depth > 0 {
				if j+1 < n && sql[j] == '/' && sql[j+1] == '*' {
					depth++
					j += 2
					continue
				}
				if j+1 < n && sql[j] == '*' && sql[j+1] == '/' {
					depth--
					j += 2
					continue
				}
				j++
			}
			b.WriteString(sql[i:j])
			i = j
		case isDigit(c) || (c == '.' && i+1 < n && isDigit(sql[i+1])):
			// Numeric literal: int, float, hex, e-notation.
			// If preceding byte was an identifier byte (so this `1` is the
			// `1` in `user1`), this is identifier-tail not a literal;
			// keep verbatim.
			if i > 0 && isIdentByte(sql[i-1]) {
				j := i
				for j < n && (isDigit(sql[j]) || isIdentByte(sql[j])) {
					j++
				}
				b.WriteString(sql[i:j])
				i = j
				continue
			}
			j := i
			if c == '0' && i+1 < n && (sql[i+1] == 'x' || sql[i+1] == 'X') {
				j += 2
				for j < n && isHex(sql[j]) {
					j++
				}
			} else {
				for j < n && (isDigit(sql[j]) || sql[j] == '.') {
					j++
				}
				if j < n && (sql[j] == 'e' || sql[j] == 'E') {
					j++
					if j < n && (sql[j] == '+' || sql[j] == '-') {
						j++
					}
					for j < n && isDigit(sql[j]) {
						j++
					}
				}
			}
			b.WriteByte('?')
			i = j
		case c == '"':
			// Double-quoted identifier kept verbatim.
			j := i + 1
			for j < n {
				if sql[j] == '"' {
					if j+1 < n && sql[j+1] == '"' {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			b.WriteString(sql[i:j])
			i = j
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }
func isHex(c byte) bool {
	return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
func isIdentByte(c byte) bool { return util.IsIdentByte(c) }

// SQLForLog returns the SQL string to log under the given mode.
// Truncated to maxLen with a trailing "..." so a 10 MB INSERT can't blow
// up a structured-log line.
func SQLForLog(mode string, sql string, maxLen int) string {
	if maxLen <= 0 {
		maxLen = 256
	}
	switch mode {
	case "off", "none", "false", "":
		return ""
	case "full", "raw":
		return truncate(sql, maxLen)
	default: // "redacted" or anything else
		return truncate(RedactSQL(sql), maxLen)
	}
}
