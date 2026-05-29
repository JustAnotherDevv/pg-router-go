package auth

import (
	"crypto/md5" //nolint:gosec // protocol requires MD5
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMD5RoundTrip(t *testing.T) {
	salt := [4]byte{0x01, 0x02, 0x03, 0x04}
	resp := MD5PasswordResponse("alice", "wonderland", salt)
	require.Len(t, resp, 35) // "md5" + 32 hex chars
	require.Equal(t, "md5", resp[:3])
}

func TestVerifyMD5WithClearStoredPassword(t *testing.T) {
	salt := [4]byte{0x10, 0x20, 0x30, 0x40}
	resp := MD5PasswordResponse("bob", "p@ssw0rd", salt)
	require.True(t, VerifyMD5Password("bob", "p@ssw0rd", salt, resp))
	require.False(t, VerifyMD5Password("bob", "wrong", salt, resp))
}

func TestVerifyMD5WithMD5PrefixedStored(t *testing.T) {
	salt := [4]byte{0xa1, 0xb2, 0xc3, 0xd4}
	resp := MD5PasswordResponse("eve", "topsecret", salt)

	// Build the "md5<inner_hex>" form Postgres stores in pg_authid.rolpassword
	// for md5-typed users: md5(password + username).
	inner := md5.Sum([]byte("topsecret" + "eve")) //nolint:gosec
	storedPrefixed := "md5" + hex.EncodeToString(inner[:])
	require.True(t, VerifyMD5Password("eve", storedPrefixed, salt, resp))
}

func TestVerifyMD5RejectsWrongLength(t *testing.T) {
	salt := [4]byte{0, 0, 0, 0}
	require.False(t, VerifyMD5Password("u", "pwd", salt, "shorter"))
	require.False(t, VerifyMD5Password("u", "pwd", salt, "md5"+"x"))
}

func TestConstantTimeEqString(t *testing.T) {
	require.True(t, constantTimeEqString("", ""))
	require.True(t, constantTimeEqString("abc", "abc"))
	require.False(t, constantTimeEqString("abc", "abd"))
	require.False(t, constantTimeEqString("abc", "ab"))
}
