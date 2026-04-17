// Client-side auth handshakes: pgrouter authenticates AS A CLIENT to
// the upstream Postgres. Called from internal/backend during Dial.
//
// MVP scope: trust (no extra messages), MD5, SCRAM-SHA-256.

package auth

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

// PerformClientAuth runs the client-side authentication phase against
// an upstream backend, dispatching on whichever Authentication* variant
// the server sent.
//
// fe is the pgproto3 Frontend driving the upstream. The first
// Authentication message has already been received (it's passed in as
// `msg`). PerformClientAuth keeps the conversation going until the
// server sends AuthenticationOk or an ErrorResponse.
//
// The caller is responsible for ReadyForQuery + post-auth message
// reception.
func PerformClientAuth(fe *pgproto3.Frontend, username, password string, msg pgproto3.BackendMessage) error {
	for {
		switch m := msg.(type) {
		case *pgproto3.AuthenticationOk:
			return nil

		case *pgproto3.AuthenticationCleartextPassword:
			fe.Send(&pgproto3.PasswordMessage{Password: password})
			if err := fe.Flush(); err != nil {
				return fmt.Errorf("cleartext flush: %w", err)
			}

		case *pgproto3.AuthenticationMD5Password:
			resp := MD5PasswordResponse(username, password, m.Salt)
			fe.Send(&pgproto3.PasswordMessage{Password: resp})
			if err := fe.Flush(); err != nil {
				return fmt.Errorf("md5 flush: %w", err)
			}

		case *pgproto3.AuthenticationSASL:
			if !containsString(m.AuthMechanisms, SCRAMMechanism) {
				return fmt.Errorf("server offered SASL mechanisms %v; %s not supported by pgrouter",
					m.AuthMechanisms, SCRAMMechanism)
			}
			if err := runClientSCRAM(fe, username, password); err != nil {
				return fmt.Errorf("scram: %w", err)
			}
			// Server should now send AuthenticationOk; loop continues.

		case *pgproto3.ErrorResponse:
			return fmt.Errorf("backend auth error %s: %s", m.Severity, m.Message)

		case *pgproto3.AuthenticationGSS,
			*pgproto3.AuthenticationGSSContinue:
			return errors.New("backend requested GSS auth; not supported")

		default:
			return fmt.Errorf("unexpected auth message %T", msg)
		}

		var err error
		msg, err = fe.Receive()
		if err != nil {
			return fmt.Errorf("receive next auth msg: %w", err)
		}
	}
}

// runClientSCRAM drives a full SCRAM-SHA-256 conversation as the client.
//
// On entry the server has just sent AuthenticationSASL. On exit we
// have sent the SASLResponse with the client proof; the next
// fe.Receive() should produce AuthenticationSASLFinal then AuthenticationOk.
func runClientSCRAM(fe *pgproto3.Frontend, username, password string) error {
	conv := NewSCRAMClient(username, password)
	clientFirst, err := conv.Initial()
	if err != nil {
		return fmt.Errorf("initial: %w", err)
	}
	fe.Send(&pgproto3.SASLInitialResponse{
		AuthMechanism: SCRAMMechanism,
		Data:          clientFirst,
	})
	if err := fe.Flush(); err != nil {
		return fmt.Errorf("sasl initial flush: %w", err)
	}

	// Server should respond with AuthenticationSASLContinue.
	resp, err := fe.Receive()
	if err != nil {
		return fmt.Errorf("recv after sasl initial: %w", err)
	}
	cont, ok := resp.(*pgproto3.AuthenticationSASLContinue)
	if !ok {
		return fmt.Errorf("expected SASLContinue, got %T", resp)
	}

	clientFinal, err := conv.Step1(cont.Data)
	if err != nil {
		return fmt.Errorf("step1: %w", err)
	}
	fe.Send(&pgproto3.SASLResponse{Data: clientFinal})
	if err := fe.Flush(); err != nil {
		return fmt.Errorf("sasl response flush: %w", err)
	}

	// Server responds with AuthenticationSASLFinal containing v= signature.
	resp, err = fe.Receive()
	if err != nil {
		return fmt.Errorf("recv after sasl response: %w", err)
	}
	final, ok := resp.(*pgproto3.AuthenticationSASLFinal)
	if !ok {
		return fmt.Errorf("expected SASLFinal, got %T", resp)
	}
	if err := conv.Step2(final.Data); err != nil {
		return fmt.Errorf("step2 verify: %w", err)
	}
	return nil
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
