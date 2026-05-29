package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// SCRAMServerConv is a server-side SCRAM-SHA-256 conversation.
//
// Flow:
//  1. caller advertises SCRAM-SHA-256 via AuthenticationSASL
//  2. client sends SASLInitialResponse (mechanism + client-first-message)
//     → Step1(initialResp) returns server-first-message
//  3. caller sends server-first as AuthenticationSASLContinue
//  4. client sends SASLResponse (client-final-message)
//     → Step2(finalResp) returns server-final-message (with verifier)
//  5. caller sends server-final as AuthenticationSASLFinal
//  6. caller sends AuthenticationOk if Step2 returned nil error
//
// One SCRAMServerConv is used for exactly one client login.
type SCRAMServerConv struct {
	verifier *SCRAMVerifier
	username string

	// state captured between Step1 and Step2.
	clientFirstBare []byte
	serverFirstMsg  []byte
	serverNonce     []byte
	clientNonce     []byte
}

// NewSCRAMServer prepares a conversation for the given user's verifier.
// `verifier` is what we'd have stored in our equivalent of
// pg_authid.rolpassword. Caller is responsible for not leaking it.
func NewSCRAMServer(username string, verifier *SCRAMVerifier) *SCRAMServerConv {
	return &SCRAMServerConv{username: username, verifier: verifier}
}

// Step1 consumes the client-first-message (the SASLInitialResponse
// body) and returns the server-first-message.
func (c *SCRAMServerConv) Step1(clientFirst []byte) ([]byte, error) {
	if c.verifier == nil {
		// We deliberately don't disclose this — but the client will fail
		// on the proof check at Step2 anyway, so we just synthesize the
		// fake conversation to keep timing similar.
		return nil, errors.New("auth: no SCRAM verifier for user")
	}

	// client-first-message-bare strips the GS2 header (cbind flag + authzid).
	bare, err := stripGS2Header(clientFirst)
	if err != nil {
		return nil, fmt.Errorf("scram step1: %w", err)
	}
	// IMPORTANT: copy. pgproto3's Receive buffer gets reused on the
	// next Receive call, so keeping a slice into `clientFirst` would
	// have the bytes silently mutate beneath us before Step2 reads them.
	c.clientFirstBare = append([]byte{}, bare...)

	// Parse client nonce.
	parts, err := splitSCRAMAttrs(bare)
	if err != nil {
		return nil, fmt.Errorf("scram step1: %w", err)
	}
	clientNonceB64, ok := parts["r"]
	if !ok {
		return nil, errors.New("scram step1: missing 'r' attribute")
	}
	c.clientNonce = []byte(clientNonceB64)

	// Generate server nonce (random + base64-encoded).
	srvNonceRaw := make([]byte, 18)
	if _, err := rand.Read(srvNonceRaw); err != nil {
		return nil, fmt.Errorf("scram step1: nonce rand: %w", err)
	}
	c.serverNonce = []byte(base64.StdEncoding.EncodeToString(srvNonceRaw))

	combined := append([]byte{}, c.clientNonce...)
	combined = append(combined, c.serverNonce...)

	saltB64 := base64.StdEncoding.EncodeToString(c.verifier.Salt)
	c.serverFirstMsg = []byte(fmt.Sprintf("r=%s,s=%s,i=%d",
		combined, saltB64, c.verifier.Iterations))
	return c.serverFirstMsg, nil
}

// Step2 consumes the client-final-message and returns the
// server-final-message on successful auth, error on failure.
func (c *SCRAMServerConv) Step2(clientFinal []byte) ([]byte, error) {
	if c.verifier == nil {
		return nil, errors.New("auth: no SCRAM verifier")
	}
	parts, err := splitSCRAMAttrs(clientFinal)
	if err != nil {
		return nil, fmt.Errorf("scram step2: %w", err)
	}
	chanBindB64, ok := parts["c"]
	if !ok {
		return nil, errors.New("scram step2: missing 'c'")
	}
	combinedNonce, ok := parts["r"]
	if !ok {
		return nil, errors.New("scram step2: missing 'r'")
	}
	clientProofB64, ok := parts["p"]
	if !ok {
		return nil, errors.New("scram step2: missing 'p'")
	}

	// Channel-binding flag for non-TLS pgwire must be biws (base64 of "n,,").
	if chanBindB64 != "biws" {
		return nil, fmt.Errorf("scram step2: unsupported channel binding %q", chanBindB64)
	}

	expected := append([]byte{}, c.clientNonce...)
	expected = append(expected, c.serverNonce...)
	if combinedNonce != string(expected) {
		return nil, errors.New("scram step2: nonce mismatch")
	}

	clientFinalWithoutProof := clientFinal[:bytes.LastIndex(clientFinal, []byte(",p="))]
	authMessage := bytes.Join([][]byte{
		c.clientFirstBare,
		c.serverFirstMsg,
		clientFinalWithoutProof,
	}, []byte(","))

	clientSignature := hmacSHA256(c.verifier.StoredKey, authMessage)
	clientProof, err := base64.StdEncoding.DecodeString(clientProofB64)
	if err != nil {
		return nil, fmt.Errorf("scram step2: decode proof: %w", err)
	}
	if len(clientProof) != len(clientSignature) {
		return nil, errors.New("scram step2: proof length mismatch")
	}
	clientKey := make([]byte, len(clientProof))
	for i := range clientProof {
		clientKey[i] = clientProof[i] ^ clientSignature[i]
	}
	if !hmac.Equal(sha256Sum(clientKey), c.verifier.StoredKey) {
		return nil, errors.New("scram step2: invalid client proof")
	}

	serverSignature := hmacSHA256(c.verifier.ServerKey, authMessage)
	return []byte("v=" + base64.StdEncoding.EncodeToString(serverSignature)), nil
}

// stripGS2Header removes the GS2 header from a client-first message and
// returns the bare part (everything after the second ',').
//
// pgwire flow (RFC 7677): "n,,n=u,r=clientnonce" — the gs2-cbind-flag
// is 'n' (no channel binding) and authzid is empty.
func stripGS2Header(clientFirst []byte) ([]byte, error) {
	// Tolerate the "y" flag (client supports cbind but believes the
	// server doesn't) — pg drivers use both.
	if len(clientFirst) < 3 {
		return nil, errors.New("client-first too short")
	}
	first := clientFirst[0]
	if first != 'n' && first != 'y' && first != 'p' {
		return nil, fmt.Errorf("unexpected gs2-cbind-flag %q", first)
	}
	if clientFirst[1] != ',' {
		return nil, errors.New("malformed gs2 header")
	}
	// Find the second comma.
	if idx := bytes.IndexByte(clientFirst[2:], ','); idx >= 0 {
		return clientFirst[2+idx+1:], nil
	}
	return nil, errors.New("malformed gs2 header: missing second ','")
}

// splitSCRAMAttrs parses a comma-separated attr=value sequence into a
// map. SCRAM strings never contain quoted commas so this is safe.
func splitSCRAMAttrs(b []byte) (map[string]string, error) {
	parts := strings.Split(string(b), ",")
	out := make(map[string]string, len(parts))
	for _, p := range parts {
		k, v, ok := strings.Cut(p, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("bad attr %q", p)
		}
		out[k] = v
	}
	return out, nil
}
