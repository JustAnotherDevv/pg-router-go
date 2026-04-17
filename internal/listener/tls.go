// TLS upgrade helpers for the listener / dialer.
//
// pgwire's TLS negotiation is unusual: the client sends an SSLRequest
// magic value (4-byte length=8, 4-byte magic=80877103), the server
// responds with a single byte 'S' (proceed with TLS) or 'N' (decline,
// continue plaintext or fail per client sslmode). After 'S', a normal
// TLS handshake happens on the same socket. After 'N', the client
// follows up with a regular StartupMessage in plaintext (or closes).
//
// MVP scope:
//   - UpgradeClientToTLS: server-side wrap after SSLRequest('S')
//   - UpgradeBackendToTLS: client-side wrap before sending StartupMessage
//
// See https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SSL.

package listener

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
)

// SSLRequestMagic is the int32 magic value following the int32 length
// inside a pgwire SSLRequest packet.
const SSLRequestMagic uint32 = 80877103

// GSSEncRequestMagic is the int32 magic for GSSAPI encryption requests.
// We always decline these in MVP.
const GSSEncRequestMagic uint32 = 80877104

// WriteSSLAccept writes a single 'S' byte to indicate the server is
// willing to proceed with TLS.
func WriteSSLAccept(w io.Writer) error {
	_, err := w.Write([]byte{'S'})
	return err
}

// WriteSSLDecline writes a single 'N' byte to indicate the server will
// not proceed with TLS on this connection (client may continue with
// plaintext StartupMessage, or close).
func WriteSSLDecline(w io.Writer) error {
	_, err := w.Write([]byte{'N'})
	return err
}

// UpgradeServerToTLS wraps the given client connection in a server-side
// TLS handshake. The caller must have already written 'S' in response
// to the client's SSLRequest.
//
// Returns a tls.Conn ready for the post-handshake StartupMessage.
func UpgradeServerToTLS(c net.Conn, cfg *tls.Config) (*tls.Conn, error) {
	if cfg == nil {
		return nil, fmt.Errorf("server tls config is nil")
	}
	tlsConn := tls.Server(c, cfg)
	if err := tlsConn.Handshake(); err != nil {
		return nil, fmt.Errorf("tls server handshake: %w", err)
	}
	return tlsConn, nil
}

// UpgradeClientToTLS wraps a backend connection in client-side TLS.
// Caller has already sent an SSLRequest and read 'S' in response.
func UpgradeClientToTLS(c net.Conn, cfg *tls.Config) (*tls.Conn, error) {
	if cfg == nil {
		return nil, fmt.Errorf("client tls config is nil")
	}
	tlsConn := tls.Client(c, cfg)
	if err := tlsConn.Handshake(); err != nil {
		return nil, fmt.Errorf("tls client handshake: %w", err)
	}
	return tlsConn, nil
}
