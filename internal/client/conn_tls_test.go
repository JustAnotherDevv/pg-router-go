package client

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil/testcerts"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// TestSSLRequestAcceptedThenStartup is the end-to-end TLS path: client
// sends SSLRequest, server responds 'S', handshakes TLS, then client
// sends a normal StartupMessage over the encrypted stream and expects
// AuthenticationOk back.
func TestSSLRequestAcceptedThenStartup(t *testing.T) {
	certs := testcerts.Write(t)
	pair, err := tls.LoadX509KeyPair(certs.ServerCert, certs.ServerKey)
	require.NoError(t, err)
	srvCfg := &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}

	ln, _ := testutil.TCPListener(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		h := &Conn{Log: testutil.Discard, TLSConfig: srvCfg}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.Handle(ctx, c)
	}()

	// Client side.
	rawCli, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer rawCli.Close()

	_ = rawCli.SetDeadline(time.Now().Add(3 * time.Second))

	// 1. SSLRequest.
	sslReq := make([]byte, 8)
	binary.BigEndian.PutUint32(sslReq[0:4], 8)
	binary.BigEndian.PutUint32(sslReq[4:8], 80877103)
	_, err = rawCli.Write(sslReq)
	require.NoError(t, err)

	// 2. Read single byte response.
	one := make([]byte, 1)
	_, err = rawCli.Read(one)
	require.NoError(t, err)
	require.Equal(t, byte('S'), one[0])

	// 3. TLS handshake (verify against the same self-signed cert).
	tlsCli := tls.Client(rawCli, &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, //nolint:gosec // test
		ServerName:         "127.0.0.1",
	})
	require.NoError(t, tlsCli.Handshake())

	// 4. Send StartupMessage on the TLS stream.
	testutil.SendStartup(t, tlsCli, "u", "d")

	// 5. Read AuthenticationOk over TLS.
	fe := pgproto3.NewFrontend(tlsCli, tlsCli)
	msg, err := fe.Receive()
	require.NoError(t, err)
	_, ok := msg.(*pgproto3.AuthenticationOk)
	require.True(t, ok, "expected AuthenticationOk, got %T", msg)

	_ = tlsCli.Close()
	<-done
}

// TestSSLRequestDeclinedWhenTLSDisabled mirrors the legacy path: when
// no TLSConfig is set, SSLRequest must be answered with 'N' and a
// follow-up StartupMessage must still succeed.
func TestSSLRequestDeclinedWhenTLSDisabled(t *testing.T) {
	ln, _ := testutil.TCPListener(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		h := &Conn{Log: testutil.Discard} // TLSConfig nil
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.Handle(ctx, c)
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer cli.Close()
	_ = cli.SetDeadline(time.Now().Add(3 * time.Second))

	sslReq := make([]byte, 8)
	binary.BigEndian.PutUint32(sslReq[0:4], 8)
	binary.BigEndian.PutUint32(sslReq[4:8], 80877103)
	_, err = cli.Write(sslReq)
	require.NoError(t, err)

	one := make([]byte, 1)
	_, err = cli.Read(one)
	require.NoError(t, err)
	require.Equal(t, byte('N'), one[0])

	// Plaintext StartupMessage.
	testutil.SendStartup(t, cli, "u", "d")

	fe := pgproto3.NewFrontend(cli, cli)
	msg, err := fe.Receive()
	require.NoError(t, err)
	_, ok := msg.(*pgproto3.AuthenticationOk)
	require.True(t, ok)

	_ = cli.Close()
	<-done
}
