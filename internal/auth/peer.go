// SO_PEERCRED-based peer authentication.
//
// When a client connects over a Unix domain socket the kernel knows
// which OS uid is on the far side. Peer auth uses that directly:
//   1. Look up the uid -> username via the OS user database
//      (`os/user.LookupId`).
//   2. Compare it to the user that the client supplied in the
//      StartupMessage.
//   3. Match → accept; mismatch → return an error (the dispatcher
//      replies with FATAL 28P01).
//
// This mirrors PgBouncer `auth_type = peer` and Postgres `peer` in
// pg_hba.conf. It's the safest cred path for in-host services
// because there's literally nothing to leak — no password, no token.
//
// Cross-platform support: SO_PEERCRED exists on Linux. macOS has
// getpeereid (also fine). Windows AF_UNIX exists but has no peer-cred
// concept — peer auth on Windows will always return ErrPeerCredUnsup.

package auth

import (
	"errors"
	"net"
	"os/user"
)

// ErrPeerCredUnsup is returned by PeerUsername when the platform does
// not expose peer credentials (Windows, exotic Unixes).
var ErrPeerCredUnsup = errors.New("peer-cred lookup unsupported on this platform")

// PeerUsername returns the OS username on the far side of conn.
// conn must be a *net.UnixConn that the caller obtained from a Unix
// socket listener. Anything else returns an error.
//
// Implementation is per-platform: see peer_linux.go / peer_other.go.
func PeerUsername(conn net.Conn) (string, error) {
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return "", errors.New("peer auth requires Unix socket conn")
	}
	uid, err := peerUID(uc)
	if err != nil {
		return "", err
	}
	u, err := user.LookupId(uid)
	if err != nil {
		return "", err
	}
	return u.Username, nil
}
