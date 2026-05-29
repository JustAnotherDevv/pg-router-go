package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xdg-go/pbkdf2"
)

// TestVerifierRoundTrip checks that MakeSCRAMVerifier produces a verifier
// whose String() round-trips through ParseSCRAMVerifier.
func TestVerifierRoundTrip(t *testing.T) {
	v, err := MakeSCRAMVerifier("hunter2")
	require.NoError(t, err)
	s := v.String()
	require.True(t, strings.HasPrefix(s, "SCRAM-SHA-256$"))

	parsed, err := ParseSCRAMVerifier(s)
	require.NoError(t, err)
	require.Equal(t, v.Iterations, parsed.Iterations)
	require.Equal(t, v.Salt, parsed.Salt)
	require.Equal(t, v.StoredKey, parsed.StoredKey)
	require.Equal(t, v.ServerKey, parsed.ServerKey)
}

// TestVerifierFixedSalt makes the derivation deterministic so we can
// pin a known hex value as a regression check.
func TestVerifierFixedSalt(t *testing.T) {
	salt := []byte("0123456789ABCDEF")
	v, err := MakeSCRAMVerifierWithSalt("password", salt, 4096)
	require.NoError(t, err)
	require.Equal(t, 4096, v.Iterations)
	require.Equal(t, salt, v.Salt)
	require.Len(t, v.StoredKey, 32)
	require.Len(t, v.ServerKey, 32)

	// Recompute manually and check.
	saltedPwd := pbkdf2.Key([]byte("password"), salt, 4096, sha256.Size, sha256.New)
	clientKey := hmacSHA256(saltedPwd, []byte("Client Key"))
	serverKey := hmacSHA256(saltedPwd, []byte("Server Key"))
	expectedStored := sha256Sum(clientKey)
	require.True(t, bytes.Equal(expectedStored, v.StoredKey))
	require.True(t, bytes.Equal(serverKey, v.ServerKey))
}

func TestParseVerifierRejects(t *testing.T) {
	cases := []string{
		"",
		"MD5something",
		"SCRAM-SHA-256$",
		"SCRAM-SHA-256$4096:invalid$xxx:yyy",
		"SCRAM-SHA-256$4096:" + base64.StdEncoding.EncodeToString([]byte("salt")) + "$notbase64!!:" + base64.StdEncoding.EncodeToString([]byte("server")),
	}
	for _, s := range cases {
		t.Run(s, func(t *testing.T) {
			_, err := ParseSCRAMVerifier(s)
			require.Error(t, err)
		})
	}
}

// TestSCRAMServerFullConversation simulates a client + server SCRAM
// conversation end-to-end and asserts both sides agree.
//
// Closely mirrors what pgproto3 + pg drivers do during a real login.
func TestSCRAMServerFullConversation(t *testing.T) {
	password := "topsecret"
	verifier, err := MakeSCRAMVerifier(password)
	require.NoError(t, err)
	username := "alice"

	server := NewSCRAMServer(username, verifier)

	// --- client step 1: client-first-message ---
	clientNonceRaw := make([]byte, 18)
	_, _ = rand.Read(clientNonceRaw)
	clientNonce := base64.StdEncoding.EncodeToString(clientNonceRaw)
	clientFirst := []byte(fmt.Sprintf("n,,n=%s,r=%s", username, clientNonce))

	serverFirst, err := server.Step1(clientFirst)
	require.NoError(t, err)

	// Parse server-first.
	parts, err := splitSCRAMAttrs(serverFirst)
	require.NoError(t, err)
	combinedNonce := parts["r"]
	require.True(t, strings.HasPrefix(combinedNonce, clientNonce))
	saltB64 := parts["s"]
	iter := parts["i"]
	require.Equal(t, fmt.Sprintf("%d", verifier.Iterations), iter)

	// --- client step 2: client-final-message ---
	salt, err := base64.StdEncoding.DecodeString(saltB64)
	require.NoError(t, err)

	saltedPwd := pbkdf2.Key([]byte(password), salt, verifier.Iterations, sha256.Size, sha256.New)
	clientKey := hmacSHA256(saltedPwd, []byte("Client Key"))
	storedKey := sha256Sum(clientKey)

	channelBindB64 := base64.StdEncoding.EncodeToString([]byte("n,,"))
	clientFinalWithoutProof := []byte(fmt.Sprintf("c=%s,r=%s", channelBindB64, combinedNonce))

	clientFirstBare := []byte(fmt.Sprintf("n=%s,r=%s", username, clientNonce))
	authMessage := bytes.Join([][]byte{clientFirstBare, serverFirst, clientFinalWithoutProof}, []byte(","))
	clientSignature := hmacSHA256(storedKey, authMessage)
	clientProof := make([]byte, len(clientKey))
	for i := range clientProof {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}
	clientFinal := append(append([]byte{}, clientFinalWithoutProof...),
		[]byte(",p="+base64.StdEncoding.EncodeToString(clientProof))...)

	serverFinal, err := server.Step2(clientFinal)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(serverFinal, []byte("v=")))

	// Verify server signature matches what client would compute.
	serverKeyExpected := hmacSHA256(saltedPwd, []byte("Server Key"))
	serverSignature := hmacSHA256(serverKeyExpected, authMessage)
	expected := []byte("v=" + base64.StdEncoding.EncodeToString(serverSignature))
	require.True(t, hmac.Equal(serverFinal, expected))
}

// TestSCRAMServerWrongPassword: a client computing proof from the
// wrong password must fail at Step2.
func TestSCRAMServerWrongPassword(t *testing.T) {
	verifier, err := MakeSCRAMVerifier("correct-password")
	require.NoError(t, err)
	server := NewSCRAMServer("alice", verifier)

	clientNonce := base64.StdEncoding.EncodeToString([]byte("clientnonce-fixed"))
	clientFirst := []byte(fmt.Sprintf("n,,n=alice,r=%s", clientNonce))
	serverFirst, err := server.Step1(clientFirst)
	require.NoError(t, err)

	parts, _ := splitSCRAMAttrs(serverFirst)
	salt, _ := base64.StdEncoding.DecodeString(parts["s"])

	// Compute proof using a DIFFERENT password.
	wrong := pbkdf2.Key([]byte("wrong-password"), salt, verifier.Iterations, sha256.Size, sha256.New)
	wrongKey := hmacSHA256(wrong, []byte("Client Key"))
	wrongStored := sha256Sum(wrongKey)
	channelBindB64 := base64.StdEncoding.EncodeToString([]byte("n,,"))
	clientFinalWithoutProof := []byte(fmt.Sprintf("c=%s,r=%s", channelBindB64, parts["r"]))
	clientFirstBare := []byte(fmt.Sprintf("n=alice,r=%s", clientNonce))
	authMessage := bytes.Join([][]byte{clientFirstBare, serverFirst, clientFinalWithoutProof}, []byte(","))
	sig := hmacSHA256(wrongStored, authMessage)
	proof := make([]byte, len(wrongKey))
	for i := range proof {
		proof[i] = wrongKey[i] ^ sig[i]
	}
	clientFinal := append(clientFinalWithoutProof, []byte(",p="+base64.StdEncoding.EncodeToString(proof))...)

	_, err = server.Step2(clientFinal)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid client proof")
}

func TestSCRAMServerMalformedInputs(t *testing.T) {
	verifier, _ := MakeSCRAMVerifier("password")
	server := NewSCRAMServer("u", verifier)

	// Bad gs2 header.
	_, err := server.Step1([]byte("zzz"))
	require.Error(t, err)

	// Missing 'r' attribute.
	_, err = server.Step1([]byte("n,,n=u"))
	require.Error(t, err)
}

func TestStripGS2Header(t *testing.T) {
	bare, err := stripGS2Header([]byte("n,,n=alice,r=abc"))
	require.NoError(t, err)
	require.Equal(t, "n=alice,r=abc", string(bare))

	bare, err = stripGS2Header([]byte("y,,n=bob,r=def"))
	require.NoError(t, err)
	require.Equal(t, "n=bob,r=def", string(bare))

	_, err = stripGS2Header([]byte(""))
	require.Error(t, err)

	_, err = stripGS2Header([]byte("XX"))
	require.Error(t, err)
}
