package client

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/auth"
	"github.com/JustAnotherDevv/pgrouter/internal/config"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
)

// writeUserlist creates a userlist file with one (user, secret) pair.
func writeUserlist(t *testing.T, user, secret string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "userlist.txt")
	body := `"` + user + `" "` + secret + `"`
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// TestServerSCRAMSuccess: client uses correct password → AuthOk received.
func TestServerSCRAMSuccess(t *testing.T) {
	password := "wonderland"
	verifier, err := auth.MakeSCRAMVerifier(password)
	require.NoError(t, err)
	ulPath := writeUserlist(t, "alice", verifier.String())

	ul, err := auth.NewUserlist(ulPath)
	require.NoError(t, err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		h := &Conn{
			Log: testutil.Discard,
			Auth: &auth.ServerAuthOptions{
				Type:     config.AuthSCRAM,
				Userlist: ul,
			},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.Handle(ctx, c)
	}()

	// Client side: drive a SCRAM conversation against the handler.
	cli, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer cli.Close()
	_ = cli.SetDeadline(time.Now().Add(5 * time.Second))

	// Send StartupMessage.
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "alice", "database": "appdb"},
	}
	enc, _ := startup.Encode(nil)
	_, err = cli.Write(enc)
	require.NoError(t, err)

	fe := pgproto3.NewFrontend(cli, cli)

	// Receive first auth message from server, then hand off to the
	// known-good auth.PerformClientAuth (same code path used by
	// backend.Dial against an upstream). It consumes AuthenticationOk
	// itself on its way out, so the next message we see should be the
	// first ParameterStatus from sendStartupResponse.
	msg, err := fe.Receive()
	require.NoError(t, err)
	require.NoError(t, auth.PerformClientAuth(fe, "alice", password, msg))

	msg, err = fe.Receive()
	require.NoError(t, err)
	_, ok := msg.(*pgproto3.ParameterStatus)
	require.True(t, ok, "expected ParameterStatus after SCRAM+AuthOk, got %T", msg)

	_ = cli.Close()
	<-done
}

// TestServerSCRAMWrongPassword: wrong password gets a FATAL ErrorResponse.
func TestServerSCRAMWrongPassword(t *testing.T) {
	verifier, err := auth.MakeSCRAMVerifier("correct")
	require.NoError(t, err)
	ulPath := writeUserlist(t, "alice", verifier.String())
	ul, err := auth.NewUserlist(ulPath)
	require.NoError(t, err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		h := &Conn{
			Log: testutil.Discard,
			Auth: &auth.ServerAuthOptions{
				Type:     config.AuthSCRAM,
				Userlist: ul,
			},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.Handle(ctx, c)
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer cli.Close()
	_ = cli.SetDeadline(time.Now().Add(5 * time.Second))

	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "alice", "database": "appdb"},
	}
	enc, _ := startup.Encode(nil)
	_, _ = cli.Write(enc)

	fe := pgproto3.NewFrontend(cli, cli)
	_, _ = fe.Receive() // AuthenticationSASL

	conv := auth.NewSCRAMClient("alice", "WRONG")
	clientFirst, _ := conv.Initial()
	fe.Send(&pgproto3.SASLInitialResponse{
		AuthMechanism: auth.SCRAMMechanism,
		Data:          clientFirst,
	})
	_ = fe.Flush()

	msg, _ := fe.Receive()
	cont, ok := msg.(*pgproto3.AuthenticationSASLContinue)
	require.True(t, ok)
	clientFinal, _ := conv.Step1(cont.Data)
	fe.Send(&pgproto3.SASLResponse{Data: clientFinal})
	_ = fe.Flush()

	// Server should send ErrorResponse (FATAL).
	msg, err = fe.Receive()
	require.NoError(t, err)
	er, ok := msg.(*pgproto3.ErrorResponse)
	require.True(t, ok, "expected ErrorResponse, got %T", msg)
	require.Equal(t, "FATAL", er.Severity)
	require.Equal(t, "28P01", er.Code)

	_ = cli.Close()
	<-done
}

// TestServerMD5Success: client uses MD5 hash, gets AuthOk.
func TestServerMD5Success(t *testing.T) {
	ulPath := writeUserlist(t, "bob", "secret")
	ul, err := auth.NewUserlist(ulPath)
	require.NoError(t, err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		h := &Conn{
			Log: testutil.Discard,
			Auth: &auth.ServerAuthOptions{
				Type:     config.AuthMD5,
				Userlist: ul,
			},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.Handle(ctx, c)
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer cli.Close()
	_ = cli.SetDeadline(time.Now().Add(5 * time.Second))

	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "bob", "database": "appdb"},
	}
	enc, _ := startup.Encode(nil)
	_, _ = cli.Write(enc)

	fe := pgproto3.NewFrontend(cli, cli)
	msg, _ := fe.Receive()
	mp, ok := msg.(*pgproto3.AuthenticationMD5Password)
	require.True(t, ok)

	resp := auth.MD5PasswordResponse("bob", "secret", mp.Salt)
	fe.Send(&pgproto3.PasswordMessage{Password: resp})
	require.NoError(t, fe.Flush())

	msg, err = fe.Receive()
	require.NoError(t, err)
	_, ok = msg.(*pgproto3.AuthenticationOk)
	require.True(t, ok, "expected AuthOk, got %T", msg)

	_ = cli.Close()
	<-done
}
