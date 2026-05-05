// Synthesised pgwire response helpers.
//
// pgrouter often needs to fabricate a server-side response without an
// upstream backend involved:
//
//   - admin console (SHOW STATS / SHOW POOLS / ...) on the virtual
//     "pgbouncer" database
//   - per-tenant rate-limit rejection (53300)
//   - statement-mode BEGIN rejection (25001)
//   - prepared-statement Close('S') ack
//
// These functions wrap the raw pgproto3 backend Send calls so callers
// don't have to remember the {RowDescription, DataRow, CommandComplete,
// ReadyForQuery} order, or the exact ErrorResponse field set.

package proto

import (
	"github.com/jackc/pgx/v5/pgproto3"
)

// TextCol returns a FieldDescription for a text-format column.
// All admin-console outputs use text format (DataTypeOID=25 / TEXT).
func TextCol(name string) pgproto3.FieldDescription {
	return pgproto3.FieldDescription{
		Name:         []byte(name),
		DataTypeOID:  25, // text
		DataTypeSize: -1,
		Format:       0, // text
		TypeModifier: -1,
	}
}

// SendRowDesc emits a RowDescription with the supplied column shape.
func SendRowDesc(be *pgproto3.Backend, cols []pgproto3.FieldDescription) {
	be.Send(&pgproto3.RowDescription{Fields: cols})
}

// SendDataRow emits one DataRow whose values are stringified column
// cells. nil-safe; an empty `vals` produces an empty-row DataRow.
//
// EFF3 note: a sync.Pool over the outer [][]byte holder was
// considered but skipped — pgproto3.Backend.Send may retain the
// Values slice until Flush, so recycling the header before flush
// risks the next SendDataRow overwriting an in-flight buffer.
// The per-call allocation (small slice header + per-cell []byte)
// is dominated by network bandwidth in practice.
func SendDataRow(be *pgproto3.Backend, vals ...string) {
	row := make([][]byte, len(vals))
	for i, v := range vals {
		row[i] = []byte(v)
	}
	be.Send(&pgproto3.DataRow{Values: row})
}

// CompleteAndRFQ sends CommandComplete with `tag` and ReadyForQuery
// with TxStatus='I', then flushes. Use after the DataRow stream of
// a SHOW-style command.
func CompleteAndRFQ(be *pgproto3.Backend, tag string) {
	be.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	_ = be.Flush()
}

// SendNoticeCompleteRFQ emits an optional NoticeResponse, then
// CommandComplete + RFQ. Useful when an admin command succeeds but
// the operator should see an informational note (e.g. PAUSE accepted
// but a no-op in v1).
func SendNoticeCompleteRFQ(be *pgproto3.Backend, tag, notice string) {
	if notice != "" {
		be.Send(&pgproto3.NoticeResponse{
			Severity: "NOTICE", Code: "00000", Message: notice,
		})
	}
	be.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	_ = be.Flush()
}

// SendErrorRFQ emits a non-fatal ErrorResponse followed by RFQ so the
// client can issue another statement. The conn stays usable; use this
// for protocol-level rejections (statement-mode BEGIN guard, QPS cap
// hit, unknown admin command).
func SendErrorRFQ(be *pgproto3.Backend, code, msg string) {
	be.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR", Code: code, Message: msg,
	})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	_ = be.Flush()
}

// SendFatalError emits a FATAL ErrorResponse and flushes. Caller is
// expected to close the conn afterward — there is no RFQ since the
// session is being terminated.
func SendFatalError(be *pgproto3.Backend, code, msg string) {
	be.Send(&pgproto3.ErrorResponse{
		Severity: "FATAL", Code: code, Message: msg,
	})
	_ = be.Flush()
}
