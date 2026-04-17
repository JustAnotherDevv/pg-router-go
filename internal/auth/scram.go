package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/xdg-go/pbkdf2"
)

// SCRAMMechanism is the mechanism string we advertise + accept.
const SCRAMMechanism = "SCRAM-SHA-256"

// SCRAMIterations is the iteration count we use when stretching new
// passwords. PostgreSQL 16's default is also 4096.
const SCRAMIterations = 4096

// SCRAMSaltLen is the salt length we generate when stretching a new
// password from cleartext.
const SCRAMSaltLen = 16

// SCRAMVerifier represents a stored SCRAM-SHA-256 credential. This is
// the same shape that PostgreSQL stores in pg_authid.rolpassword
// (`SCRAM-SHA-256$<iter>:<salt-b64>$<stored-key-b64>:<server-key-b64>`).
//
// We never see the plaintext password once the verifier is built.
type SCRAMVerifier struct {
	Iterations int
	Salt       []byte
	StoredKey  []byte
	ServerKey  []byte
}

// ParseSCRAMVerifier parses a Postgres-format SCRAM secret string.
func ParseSCRAMVerifier(s string) (*SCRAMVerifier, error) {
	const prefix = "SCRAM-SHA-256$"
	if !strings.HasPrefix(s, prefix) {
		return nil, errors.New("not a SCRAM-SHA-256 verifier")
	}
	body := s[len(prefix):]
	// "<iter>:<salt-b64>$<stored-b64>:<server-b64>"
	idx := strings.Index(body, "$")
	if idx < 0 {
		return nil, errors.New("malformed SCRAM verifier: missing '$' separator")
	}
	left, right := body[:idx], body[idx+1:]
	iterStr, saltB64, ok := strings.Cut(left, ":")
	if !ok {
		return nil, errors.New("malformed SCRAM verifier: iter:salt section")
	}
	iter, err := strconv.Atoi(iterStr)
	if err != nil || iter < 1 {
		return nil, fmt.Errorf("malformed SCRAM verifier: bad iteration count %q", iterStr)
	}
	storedB64, serverB64, ok := strings.Cut(right, ":")
	if !ok {
		return nil, errors.New("malformed SCRAM verifier: stored:server section")
	}
	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		return nil, fmt.Errorf("malformed SCRAM verifier: salt: %w", err)
	}
	stored, err := base64.StdEncoding.DecodeString(storedB64)
	if err != nil {
		return nil, fmt.Errorf("malformed SCRAM verifier: stored key: %w", err)
	}
	server, err := base64.StdEncoding.DecodeString(serverB64)
	if err != nil {
		return nil, fmt.Errorf("malformed SCRAM verifier: server key: %w", err)
	}
	return &SCRAMVerifier{
		Iterations: iter,
		Salt:       salt,
		StoredKey:  stored,
		ServerKey:  server,
	}, nil
}

// String renders the verifier in Postgres on-disk format.
func (v *SCRAMVerifier) String() string {
	return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s",
		v.Iterations,
		base64.StdEncoding.EncodeToString(v.Salt),
		base64.StdEncoding.EncodeToString(v.StoredKey),
		base64.StdEncoding.EncodeToString(v.ServerKey),
	)
}

// MakeSCRAMVerifier derives a verifier from a cleartext password using
// a fresh random salt + SCRAMIterations rounds.
//
// Used by tools that turn a user-supplied password into the on-disk
// SCRAM credential — e.g. an admin command that pre-populates a
// userlist.txt entry.
func MakeSCRAMVerifier(password string) (*SCRAMVerifier, error) {
	salt := make([]byte, SCRAMSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("salt rand: %w", err)
	}
	return MakeSCRAMVerifierWithSalt(password, salt, SCRAMIterations)
}

// MakeSCRAMVerifierWithSalt is MakeSCRAMVerifier but with caller-supplied
// salt + iterations. Use only for tests / RFC vector reproduction.
func MakeSCRAMVerifierWithSalt(password string, salt []byte, iterations int) (*SCRAMVerifier, error) {
	if iterations < 1 {
		return nil, fmt.Errorf("iterations must be >= 1, got %d", iterations)
	}
	saltedPwd := pbkdf2.Key([]byte(password), salt, iterations, sha256.Size, sha256.New)
	clientKey := hmacSHA256(saltedPwd, []byte("Client Key"))
	serverKey := hmacSHA256(saltedPwd, []byte("Server Key"))
	storedKey := sha256Sum(clientKey)
	return &SCRAMVerifier{
		Iterations: iterations,
		Salt:       salt,
		StoredKey:  storedKey,
		ServerKey:  serverKey,
	}, nil
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func sha256Sum(data []byte) []byte {
	s := sha256.Sum256(data)
	return s[:]
}
