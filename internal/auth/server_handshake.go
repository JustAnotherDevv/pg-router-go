// Server-side auth handshakes: pgrouter authenticates clients via
// userlist.txt-backed trust / MD5 / SCRAM-SHA-256.
//
// Called from internal/client during the startup phase.

package auth

import (
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/JustAnotherDevv/pgrouter/internal/config"
)

// ServerAuthOptions carries the decisions made by client.Conn for the
// auth phase: which AuthType to enforce, which Userlist to query.
type ServerAuthOptions struct {
	Type     config.AuthType
	Userlist *Userlist // nil for trust mode
	Log      *slog.Logger
}

// PerformServerAuth runs the server-side authentication phase against
// a newly-connected client. After StartupMessage has been parsed,
// client.Conn calls this with the username + database (for logging) and
// the pgproto3 Backend driving the client conn.
//
// Returns nil on success; the caller should then send the
// AuthenticationOk + ParameterStatus* + BackendKeyData + ReadyForQuery
// sequence (or hand off to the proxy forwarder).
func PerformServerAuth(be *pgproto3.Backend, opts ServerAuthOptions, username string) error {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	switch opts.Type {
	case "", config.AuthTrust:
		// Nothing to do — caller sends AuthenticationOk itself.
		return nil

	case config.AuthMD5:
		if opts.Userlist == nil {
			return errors.New("md5 auth requires a userlist")
		}
		entry, ok := opts.Userlist.Lookup(username)
		if !ok {
			return sendAuthFailed(be, log, fmt.Sprintf("user %q not found", username))
		}
		return doServerMD5(be, log, username, entry)

	case config.AuthSCRAM:
		if opts.Userlist == nil {
			return errors.New("scram auth requires a userlist")
		}
		entry, ok := opts.Userlist.Lookup(username)
		if !ok {
			return sendAuthFailed(be, log, fmt.Sprintf("user %q not found", username))
		}
		return doServerSCRAM(be, log, username, entry)

	default:
		return fmt.Errorf("auth type %q not supported in MVP", opts.Type)
	}
}

// doServerMD5 sends AuthenticationMD5Password, reads the client's
// PasswordMessage, and verifies the hash.
func doServerMD5(be *pgproto3.Backend, log *slog.Logger, username string, entry *UserEntry) error {
	var salt [4]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return fmt.Errorf("md5 salt: %w", err)
	}
	be.Send(&pgproto3.AuthenticationMD5Password{Salt: salt})
	if err := be.Flush(); err != nil {
		return fmt.Errorf("md5 send: %w", err)
	}
	// Tell pgproto3 the next 'p'-tagged message is a PasswordMessage.
	if err := be.SetAuthType(pgproto3.AuthTypeMD5Password); err != nil {
		return fmt.Errorf("set auth type md5: %w", err)
	}

	msg, err := be.Receive()
	if err != nil {
		return fmt.Errorf("md5 recv: %w", err)
	}
	pwd, ok := msg.(*pgproto3.PasswordMessage)
	if !ok {
		return fmt.Errorf("expected PasswordMessage, got %T", msg)
	}

	stored := entry.MD5Hash
	if stored == "" {
		// Plaintext stored — VerifyMD5Password handles that path.
		stored = entry.PlainPassword
	}
	if !VerifyMD5Password(username, stored, salt, pwd.Password) {
		return sendAuthFailed(be, log, fmt.Sprintf("md5 password mismatch for %q", username))
	}
	return nil
}

// doServerSCRAM runs the SCRAM-SHA-256 conversation against the client.
func doServerSCRAM(be *pgproto3.Backend, log *slog.Logger, username string, entry *UserEntry) error {
	if entry.SCRAMVerifier == nil {
		return sendAuthFailed(be, log, fmt.Sprintf("user %q has no SCRAM verifier", username))
	}

	be.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{SCRAMMechanism}})
	if err := be.Flush(); err != nil {
		return fmt.Errorf("sasl initial send: %w", err)
	}
	// Tell pgproto3 next 'p'-tagged message is a SASLInitialResponse.
	if err := be.SetAuthType(pgproto3.AuthTypeSASL); err != nil {
		return fmt.Errorf("set auth type sasl: %w", err)
	}

	initial, err := be.Receive()
	if err != nil {
		return fmt.Errorf("sasl initial recv: %w", err)
	}
	sir, ok := initial.(*pgproto3.SASLInitialResponse)
	if !ok {
		return fmt.Errorf("expected SASLInitialResponse, got %T", initial)
	}
	if sir.AuthMechanism != SCRAMMechanism {
		return fmt.Errorf("client picked unsupported mechanism %q", sir.AuthMechanism)
	}

	conv := NewSCRAMServer(username, entry.SCRAMVerifier)
	serverFirst, err := conv.Step1(sir.Data)
	if err != nil {
		return sendAuthFailed(be, log, fmt.Sprintf("scram step1: %v", err))
	}
	be.Send(&pgproto3.AuthenticationSASLContinue{Data: serverFirst})
	if err := be.Flush(); err != nil {
		return fmt.Errorf("sasl continue send: %w", err)
	}
	// Next 'p'-tagged message is a SASLResponse.
	if err := be.SetAuthType(pgproto3.AuthTypeSASLContinue); err != nil {
		return fmt.Errorf("set auth type sasl continue: %w", err)
	}

	resp, err := be.Receive()
	if err != nil {
		return fmt.Errorf("sasl response recv: %w", err)
	}
	sr, ok := resp.(*pgproto3.SASLResponse)
	if !ok {
		return fmt.Errorf("expected SASLResponse, got %T", resp)
	}
	serverFinal, err := conv.Step2(sr.Data)
	if err != nil {
		return sendAuthFailed(be, log, fmt.Sprintf("scram step2: %v", err))
	}
	be.Send(&pgproto3.AuthenticationSASLFinal{Data: serverFinal})
	if err := be.Flush(); err != nil {
		return fmt.Errorf("sasl final send: %w", err)
	}
	return nil
}

func sendAuthFailed(be *pgproto3.Backend, log *slog.Logger, reason string) error {
	log.Info("auth failed", "reason", reason)
	be.Send(&pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     "28P01",
		Message:  "password authentication failed",
	})
	_ = be.Flush()
	return errors.New(reason)
}
