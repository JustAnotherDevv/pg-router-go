// Shared simple-query Send+Flush+drain-to-RFQ helper.
//
// Three slightly-different implementations existed before extraction:
//   - replica/ping.go pingConn  — drain-only health probe
//   - replica/lag.go scalarInt  — drain + capture first int column
//   - client/pooled.go fireReplay — drain-only GUC replay
//
// Same loop, three places to keep in sync when a bug was fixed in one.
// Now the loop lives here; callers pass a per-DataRow callback (or nil).

package proto

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

// DrainSimpleQuery sends a simple Query, drains responses until
// ReadyForQuery, and returns:
//   - nil on a clean RFQ
//   - the first ErrorResponse, wrapped
//   - the first network/parse error, wrapped
//
// If onRow is non-nil, it's called for every DataRow received. Returning
// a non-nil error from onRow aborts the drain with that error.
//
// The Frontend stays usable on nil return; on error the caller should
// treat the conn as poisoned and close it.
func DrainSimpleQuery(
	fe *pgproto3.Frontend,
	sql string,
	onRow func(*pgproto3.DataRow) error,
) error {
	fe.Send(&pgproto3.Query{String: sql})
	if err := fe.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	for {
		msg, err := fe.Receive()
		if err != nil {
			return fmt.Errorf("recv: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("server error %s: %s", m.Severity, m.Message)
		case *pgproto3.DataRow:
			if onRow != nil {
				if err := onRow(m); err != nil {
					return err
				}
			}
		case *pgproto3.ReadyForQuery:
			return nil
		}
	}
}
