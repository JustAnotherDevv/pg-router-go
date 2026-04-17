package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"

	"github.com/xdg-go/pbkdf2"
)

// SCRAMClientConv is a client-side SCRAM-SHA-256 conversation. Used
// when pgrouter authenticates AS A CLIENT to an upstream Postgres.
//
// Flow:
//  1. caller advertises username (NewSCRAMClient)
//  2. Initial() returns client-first-message; caller wraps in SASLInitialResponse
//  3. Step1(serverFirst) returns client-final-message; caller wraps in SASLResponse
//  4. Step2(serverFinal) verifies server signature, returns error on mismatch
type SCRAMClientConv struct {
	username string
	password string

	clientNonce     []byte
	clientFirstBare []byte
	serverFirstMsg  []byte

	// cached for Step2 server-signature verification.
	saltedPwd   []byte
	authMessage []byte
}

// NewSCRAMClient builds a fresh conversation. password is the cleartext
// secret — we never store it persistently, but the lib needs it to derive keys.
func NewSCRAMClient(username, password string) *SCRAMClientConv {
	return &SCRAMClientConv{username: username, password: password}
}

// Initial returns the client-first-message ("n,,n=<user>,r=<nonce>")
// that the caller wraps as `SASLInitialResponse.Data`.
func (c *SCRAMClientConv) Initial() ([]byte, error) {
	nonceRaw := make([]byte, 18)
	if _, err := rand.Read(nonceRaw); err != nil {
		return nil, fmt.Errorf("nonce: %w", err)
	}
	c.clientNonce = []byte(base64.StdEncoding.EncodeToString(nonceRaw))
	c.clientFirstBare = []byte(fmt.Sprintf("n=%s,r=%s", c.username, c.clientNonce))
	out := append([]byte("n,,"), c.clientFirstBare...)
	return out, nil
}

// Step1 consumes the server-first-message (from AuthenticationSASLContinue)
// and produces the client-final-message (for SASLResponse).
func (c *SCRAMClientConv) Step1(serverFirst []byte) ([]byte, error) {
	c.serverFirstMsg = append([]byte{}, serverFirst...)

	parts, err := splitSCRAMAttrs(serverFirst)
	if err != nil {
		return nil, fmt.Errorf("scram client step1: %w", err)
	}
	combinedNonce, ok := parts["r"]
	if !ok {
		return nil, errors.New("scram client step1: missing 'r'")
	}
	saltB64, ok := parts["s"]
	if !ok {
		return nil, errors.New("scram client step1: missing 's'")
	}
	iterStr, ok := parts["i"]
	if !ok {
		return nil, errors.New("scram client step1: missing 'i'")
	}
	iter, err := strconv.Atoi(iterStr)
	if err != nil || iter < 1 {
		return nil, fmt.Errorf("scram client step1: bad iteration count %q", iterStr)
	}

	if !bytes.HasPrefix([]byte(combinedNonce), c.clientNonce) {
		return nil, errors.New("scram client step1: nonce prefix mismatch")
	}

	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		return nil, fmt.Errorf("scram client step1: salt b64: %w", err)
	}

	saltedPwd := pbkdf2.Key([]byte(c.password), salt, iter, sha256.Size, sha256.New)
	clientKey := hmacSHA256(saltedPwd, []byte("Client Key"))
	storedKey := sha256Sum(clientKey)

	channelBindB64 := base64.StdEncoding.EncodeToString([]byte("n,,"))
	clientFinalWithoutProof := []byte(fmt.Sprintf("c=%s,r=%s", channelBindB64, combinedNonce))

	authMessage := bytes.Join([][]byte{
		c.clientFirstBare,
		c.serverFirstMsg,
		clientFinalWithoutProof,
	}, []byte(","))

	clientSignature := hmacSHA256(storedKey, authMessage)
	clientProof := make([]byte, len(clientKey))
	for i := range clientProof {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}

	// Stash for Step2.
	c.saltedPwd = saltedPwd
	c.authMessage = authMessage

	clientFinal := append(append([]byte{}, clientFinalWithoutProof...),
		[]byte(",p="+base64.StdEncoding.EncodeToString(clientProof))...)
	return clientFinal, nil
}

// Step2 verifies the server's signature in server-final-message
// (`v=<sig>`). Returns nil if the server is authentic.
func (c *SCRAMClientConv) Step2(serverFinal []byte) error {
	parts, err := splitSCRAMAttrs(serverFinal)
	if err != nil {
		return fmt.Errorf("scram client step2: %w", err)
	}
	if eMsg, ok := parts["e"]; ok {
		return fmt.Errorf("scram server reported error: %s", eMsg)
	}
	sigB64, ok := parts["v"]
	if !ok {
		return errors.New("scram client step2: missing 'v'")
	}
	gotSig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return fmt.Errorf("scram client step2: sig b64: %w", err)
	}
	serverKey := hmacSHA256(c.saltedPwd, []byte("Server Key"))
	wantSig := hmacSHA256(serverKey, c.authMessage)
	if !hmac.Equal(gotSig, wantSig) {
		return errors.New("scram client step2: server signature mismatch")
	}
	return nil
}
