package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pg-router-go/internal/config"
	"github.com/JustAnotherDevv/pg-router-go/internal/testutil"
)

// makeMTLSPair returns a CA + a server cert + a client cert (CN=`cn`).
// All certs share the same root CA so verify-full works in tests.
func makeMTLSPair(t *testing.T, cn string) (caPEM []byte, serverCert tls.Certificate, clientCert tls.Certificate) {
	t.Helper()
	// CA.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	mkLeaf := func(cn string, isServer bool) tls.Certificate {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(2),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
		}
		if isServer {
			tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			tmpl.DNSNames = []string{"localhost"}
		} else {
			tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		require.NoError(t, err)
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyDER, err := x509.MarshalECPrivateKey(key)
		require.NoError(t, err)
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
		c, err := tls.X509KeyPair(certPEM, keyPEM)
		require.NoError(t, err)
		return c
	}

	serverCert = mkLeaf("test-server", true)
	clientCert = mkLeaf(cn, false)
	return
}

// mTLSPair sets up an in-memory TLS conn pair using the test certs.
// Returns the (server-side, client-side) net.Conn after the handshake.
func mTLSPair(t *testing.T, cn string) (net.Conn, net.Conn) {
	t.Helper()
	caPEM, serverCert, clientCert := makeMTLSPair(t, cn)
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caPEM)

	ln, _ := testutil.TCPListener(t)

	serverTLSCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
	}
	clientTLSCfg := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caPool,
		ServerName:   "localhost",
	}

	var srvConn net.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		raw, err := ln.Accept()
		if err != nil {
			return
		}
		tc := tls.Server(raw, serverTLSCfg)
		if err := tc.Handshake(); err != nil {
			return
		}
		srvConn = tc
	}()

	rawCli, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	cliConn := tls.Client(rawCli, clientTLSCfg)
	require.NoError(t, cliConn.Handshake())
	wg.Wait()
	require.NotNil(t, srvConn)
	return srvConn, cliConn
}

func TestCertIdentityExtractsCN(t *testing.T) {
	srv, cli := mTLSPair(t, "alice@example.com")
	defer srv.Close()
	defer cli.Close()
	id, err := CertIdentity(srv)
	require.NoError(t, err)
	require.Equal(t, "alice@example.com", id)
}

func TestCertIdentityRejectsNonTLSConn(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	_, err := CertIdentity(c1)
	require.ErrorIs(t, err, ErrNoClientCert)
}

func TestPerformServerAuthConnCertOK(t *testing.T) {
	srv, cli := mTLSPair(t, "alice")
	defer srv.Close()
	defer cli.Close()

	opts := ServerAuthOptions{
		Type: config.AuthCert,
		Log:  testutil.Discard,
	}
	be := pgproto3.NewBackend(srv, srv)
	err := PerformServerAuthConn(be, srv, opts, "alice")
	require.NoError(t, err)
}

func TestPerformServerAuthConnCertMismatch(t *testing.T) {
	srv, cli := mTLSPair(t, "alice")
	defer srv.Close()
	defer cli.Close()

	opts := ServerAuthOptions{
		Type: config.AuthCert,
		Log:  testutil.Discard,
	}
	be := pgproto3.NewBackend(srv, srv)
	// Drain the FATAL response on the client side.
	go io.Copy(io.Discard, cli)
	err := PerformServerAuthConn(be, srv, opts, "bob")
	require.Error(t, err)
}

// Silence the "errors imported and not used" guard when refactoring.
var _ = errors.New
