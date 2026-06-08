// Single-query ping helper for replica health checks.
//
// Sends one Query, drains responses until ReadyForQuery. Returns
// the first ErrorResponse if PG rejects it (we treat that as
// unhealthy), or nil on clean RFQ.

package replica

import (
	"github.com/JustAnotherDevv/pg-router-go/internal/backend"
	"github.com/JustAnotherDevv/pg-router-go/internal/proto"
)

func pingConn(c *backend.Conn, sql string) error {
	return proto.DrainSimpleQuery(c.Frontend, sql, nil)
}
