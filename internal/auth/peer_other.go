//go:build !linux

package auth

import "net"

// peerUID is unsupported on non-Linux platforms in MVP scope.
// macOS / BSD getpeereid path can be added post-MVP; Windows AF_UNIX
// carries no peer credentials so peer auth is fundamentally unavailable
// there.
func peerUID(_ *net.UnixConn) (string, error) {
	return "", ErrPeerCredUnsup
}
