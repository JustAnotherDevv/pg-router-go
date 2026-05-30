// Statement-mode pool dispatch helpers.
//
// In statement mode (the most aggressive pooling level) the backend is
// released after every ReadyForQuery — including ones with TxStatus 'T'
// (in-transaction). That is incompatible with explicit BEGIN /
// SAVEPOINT / SET TRANSACTION blocks, because the next query may land
// on a DIFFERENT backend that has no knowledge of the partly-rolled
// state. The PgBouncer convention is to REJECT explicit transaction
// statements outright; the client gets a 25001 ("active SQL transaction"
// SQLSTATE-adjacent) error and the connection survives.
//
// Implicit single-statement transactions (`SELECT 1`, `INSERT ... RETURNING`)
// are fine: PG opens an implicit tx, the query runs, RFQ='I' arrives,
// and we release. Backends remain interchangeable.
//
// What we reject:
//   - BEGIN [ WORK | TRANSACTION ] [ ... ]
//   - START TRANSACTION [ ... ]
//
// What we permit but never release on (already handled by the txn-mode
// path): the implicit transaction PG wraps around any non-BEGIN simple
// Query.
//
// SAVEPOINT and SET LOCAL appear inside an explicit tx, so they're
// naturally fenced off by the BEGIN rejection — if the client never
// got a BEGIN through, they can't reach SAVEPOINT.

package client

import "regexp"

// explicitBeginRE matches the first SQL token of an explicit
// transaction-open statement.
//
//	BEGIN
//	BEGIN WORK
//	BEGIN TRANSACTION
//	BEGIN ISOLATION LEVEL ...
//	START TRANSACTION ...
//
// Case-insensitive. Leading whitespace + SQL comments are stripped by
// stripLeadingNoise first.
var explicitBeginRE = regexp.MustCompile(`(?i)^\s*(?:BEGIN|START\s+TRANSACTION)\b`)

// stripLeadingNoise drops leading whitespace, `-- ...` line comments,
// and `/* ... */` block comments so we can match the first SQL keyword.
func stripLeadingNoise(sql string) string {
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
			sql = sql[1:]
			continue
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			j := i + 2
			for j < len(sql) && sql[j] != '\n' {
				j++
			}
			sql = sql[j:]
			i = 0
			continue
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			j := i + 2
			depth := 1
			for j < len(sql) && depth > 0 {
				if j+1 < len(sql) && sql[j] == '/' && sql[j+1] == '*' {
					depth++
					j += 2
					continue
				}
				if j+1 < len(sql) && sql[j] == '*' && sql[j+1] == '/' {
					depth--
					j += 2
					continue
				}
				j++
			}
			sql = sql[j:]
			i = 0
			continue
		default:
			return sql
		}
	}
	return sql
}

// IsExplicitBeginSQL returns true when sql starts an explicit
// transaction. Comments + leading whitespace are tolerated.
func IsExplicitBeginSQL(sql string) bool {
	return explicitBeginRE.MatchString(stripLeadingNoise(sql))
}
