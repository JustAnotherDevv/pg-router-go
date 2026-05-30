// mTLS-based client authentication.
//
// When the listener accepted the connection via a TLS config that
// requires + verifies a client certificate, the peer cert chain is
// available via conn.(*tls.Conn).ConnectionState().PeerCertificates.
// The leaf cert's Subject CN (or first DNSName SAN) is taken as the
// authenticated identity.
//
// `auth.type: cert` accepts the client iff:
//   1. There IS a verified peer cert (tls.Conn).
//   2. The cert's CN (or SAN) equals the StartupMessage user.
//
// Otherwise sendAuthFailed → FATAL 28P01.

package auth

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
)

// ErrNoClientCert is returned by CertIdentity when conn isn't a TLS
// conn, or the TLS state has no peer cert (e.g. client_mode=require
// without verify-*).
var ErrNoClientCert = errors.New("no client certificate presented")

// CertIdentity returns the authenticated identity carried by the
// client certificate on a TLS conn. Looks first at Subject CN; falls
// back to the first DNSName SAN.
//
// Returns ErrNoClientCert if conn isn't TLS or no cert was presented.
func CertIdentity(conn net.Conn) (string, error) {
	tc, ok := conn.(*tls.Conn)
	if !ok {
		return "", ErrNoClientCert
	}
	state := tc.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return "", ErrNoClientCert
	}
	leaf := state.PeerCertificates[0]
	if cn := leaf.Subject.CommonName; cn != "" {
		return cn, nil
	}
	if len(leaf.DNSNames) > 0 {
		return leaf.DNSNames[0], nil
	}
	if len(leaf.EmailAddresses) > 0 {
		return leaf.EmailAddresses[0], nil
	}
	return "", fmt.Errorf("client cert has no usable identity (CN/SAN/email empty)")
}

// doServerCert verifies the mTLS cert identity matches the
// StartupMessage user.
func doServerCert(be *pgproto3.Backend, conn net.Conn, log *slog.Logger, username string) error {
	id, err := CertIdentity(conn)
	if err != nil {
		return sendAuthFailed(be, log,
			fmt.Sprintf("cert auth: %v", err))
	}
	if id != username {
		return sendAuthFailed(be, log,
			fmt.Sprintf("cert auth: cert identity %q does not match user %q", id, username))
	}
	log.Debug("cert auth ok", "user", username)
	return nil
}
