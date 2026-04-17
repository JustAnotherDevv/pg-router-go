package auth

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSCRAMClientServerInterop runs both sides of the conversation against
// each other and asserts the server accepts the client's proof and the
// client verifies the server's signature.
func TestSCRAMClientServerInterop(t *testing.T) {
	password := "correct horse battery staple"
	verifier, err := MakeSCRAMVerifier(password)
	require.NoError(t, err)

	srv := NewSCRAMServer("alice", verifier)
	cli := NewSCRAMClient("alice", password)

	clientFirst, err := cli.Initial()
	require.NoError(t, err)

	serverFirst, err := srv.Step1(clientFirst)
	require.NoError(t, err)

	clientFinal, err := cli.Step1(serverFirst)
	require.NoError(t, err)

	serverFinal, err := srv.Step2(clientFinal)
	require.NoError(t, err)

	require.NoError(t, cli.Step2(serverFinal))
}

// TestSCRAMClientWrongPasswordFailsAtStep2 confirms that a client with
// the wrong password is rejected by the server (proof check fails).
func TestSCRAMClientWrongPasswordFailsAtStep2(t *testing.T) {
	verifier, err := MakeSCRAMVerifier("correct")
	require.NoError(t, err)

	srv := NewSCRAMServer("alice", verifier)
	cli := NewSCRAMClient("alice", "wrong")

	first, err := cli.Initial()
	require.NoError(t, err)
	sFirst, err := srv.Step1(first)
	require.NoError(t, err)
	cFinal, err := cli.Step1(sFirst)
	require.NoError(t, err)
	_, err = srv.Step2(cFinal)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid client proof")
}

// TestSCRAMServerSignatureForgedFailsClientStep2: a tampered v= byte
// must fail the client-side verification.
func TestSCRAMServerSignatureForgedFailsClientStep2(t *testing.T) {
	verifier, err := MakeSCRAMVerifier("p")
	require.NoError(t, err)
	srv := NewSCRAMServer("u", verifier)
	cli := NewSCRAMClient("u", "p")

	first, _ := cli.Initial()
	sFirst, _ := srv.Step1(first)
	cFinal, _ := cli.Step1(sFirst)
	sFinal, _ := srv.Step2(cFinal)

	// Flip the last byte of the v= signature.
	tampered := append([]byte{}, sFinal...)
	tampered[len(tampered)-1] ^= 0x01

	require.Error(t, cli.Step2(tampered))
}
