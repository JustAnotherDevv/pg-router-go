package client

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// writeMinimalCert writes a single self-signed cert + key into t.TempDir().
// Mirrors internal/listener/testcerts_test.go but local to client.
func writeMinimalCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	require.NoError(t, err)

	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")

	f, err := os.Create(certFile)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: der}))
	require.NoError(t, f.Close())

	kd, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	f, err = os.Create(keyFile)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(f, &pem.Block{Type: "EC PRIVATE KEY", Bytes: kd}))
	require.NoError(t, f.Close())
	return
}

// TestSSLRequestAcceptedThenStartup is the end-to-end TLS path: client
// sends SSLRequest, server responds 'S', handshakes TLS, then client
// sends a normal StartupMessage over the encrypted stream and expects
// AuthenticationOk back.
func TestSSLRequestAcceptedThenStartup(t *testing.T) {
	certFile, keyFile := writeMinimalCert(t)
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)
	srvCfg := &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}

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
		h := &Conn{Log: slog.New(slog.DiscardHandler), TLSConfig: srvCfg}
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
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "u", "database": "d"},
	}
	enc, _ := startup.Encode(nil)
	_, err = tlsCli.Write(enc)
	require.NoError(t, err)

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
		h := &Conn{Log: slog.New(slog.DiscardHandler)} // TLSConfig nil
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
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "u", "database": "d"},
	}
	enc, _ := startup.Encode(nil)
	_, _ = cli.Write(enc)

	fe := pgproto3.NewFrontend(cli, cli)
	msg, err := fe.Receive()
	require.NoError(t, err)
	_, ok := msg.(*pgproto3.AuthenticationOk)
	require.True(t, ok)

	_ = cli.Close()
	<-done
}
