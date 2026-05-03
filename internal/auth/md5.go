// MD5 password authentication for Postgres.
//
// Postgres MD5 auth is computed as:
//
//	"md5" + md5_hex(md5_hex(password + username) + salt)
//
// Where the inner md5 produces a 32-char lowercase hex string and the
// outer one wraps it again with the 4-byte salt.
//
// Deprecated since PostgreSQL 14 in favour of SCRAM-SHA-256, but still
// in the wild and required for full PgBouncer compatibility.

package auth

import (
	"crypto/md5" //nolint:gosec // protocol requires MD5
	"crypto/subtle"
	"encoding/hex"
)

// MD5PasswordResponse computes the response string a client sends to
// AuthenticationMD5Password.
//
// `salt` must be exactly 4 bytes (the value from
// AuthenticationMD5Password.Salt).
func MD5PasswordResponse(username, password string, salt [4]byte) string {
	inner := md5.Sum([]byte(password + username)) //nolint:gosec
	innerHex := hex.EncodeToString(inner[:])
	outer := md5.Sum(append([]byte(innerHex), salt[:]...)) //nolint:gosec
	return "md5" + hex.EncodeToString(outer[:])
}

// VerifyMD5Password checks a client's MD5PasswordResponse against the
// expected password. Caller's `stored` is either:
//   - the raw cleartext password, or
//   - the legacy `md5<hex>` form stored in pg_authid.rolpassword for
//     md5-authed users (in which case we skip the inner hash).
//
// Returns true on match.
func VerifyMD5Password(username string, stored string, salt [4]byte, clientResponse string) bool {
	// Compute the inner hash from `stored`. If stored is already in
	// "md5..." form, strip the prefix and reuse it directly.
	var innerHex string
	if len(stored) == 35 && stored[:3] == "md5" {
		innerHex = stored[3:]
	} else {
		inner := md5.Sum([]byte(stored + username)) //nolint:gosec
		innerHex = hex.EncodeToString(inner[:])
	}
	outer := md5.Sum(append([]byte(innerHex), salt[:]...)) //nolint:gosec
	expected := "md5" + hex.EncodeToString(outer[:])
	// subtle.ConstantTimeCompare returns 1 iff slices are equal length
	// AND byte-for-byte equal, in fixed time. Stdlib guarantees no
	// length-derived timing channel; the previous hand-rolled compare
	// did an unconditional length-check that was timing-observable
	// before the XOR loop.
	return subtle.ConstantTimeCompare([]byte(expected), []byte(clientResponse)) == 1
}
