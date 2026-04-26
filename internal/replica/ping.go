// Single-query ping helper for replica health checks.
//
// Sends one Query, drains responses until ReadyForQuery. Returns
// the first ErrorResponse if PG rejects it (we treat that as
// unhealthy), or nil on clean RFQ.

package replica

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
)

func pingConn(c *backend.Conn, sql string) error {
	c.Frontend.Send(&pgproto3.Query{String: sql})
	if err := c.Frontend.Flush(); err != nil {
		return fmt.Errorf("ping flush: %w", err)
	}
	for {
		msg, err := c.Frontend.Receive()
		if err != nil {
			return fmt.Errorf("ping recv: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("ping error: %s", m.Message)
		case *pgproto3.ReadyForQuery:
			return nil
		}
	}
}
