package testutil

import (
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// SendStartup encodes a pgproto3.StartupMessage with the given user +
// database parameters and writes it to conn. Replaces the 6-line
// motif that appears 7 times across conn_test, conn_tls_test,
// client_side_test, and proto tests:
//
//	startup := &pgproto3.StartupMessage{
//	    ProtocolVersion: pgproto3.ProtocolVersionNumber,
//	    Parameters: map[string]string{"user": "u", "database": "d"},
//	}
//	enc, err := startup.Encode(nil)
//	require.NoError(t, err)
//	_, err = conn.Write(enc)
//	require.NoError(t, err)
func SendStartup(t *testing.T, conn net.Conn, user, database string) {
	t.Helper()
	sm := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": user, "database": database},
	}
	enc, err := sm.Encode(nil)
	require.NoError(t, err)
	_, err = conn.Write(enc)
	require.NoError(t, err)
}
