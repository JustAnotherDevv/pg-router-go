package listener

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testCertPaths is the output of writeTestCerts: dir holding ca.pem,
// server.crt, server.key, client.crt, client.key.
type testCertPaths struct {
	CAFile     string
	ServerCert string
	ServerKey  string
	ClientCert string
	ClientKey  string
}

// writeTestCerts creates a CA + server cert (CN=localhost, SAN=127.0.0.1)
// + client cert and writes PEM files into t.TempDir().
//
// Used by TLS unit + integration tests. The CA is self-signed and only
// trusted by the tests that load it.
func writeTestCerts(t *testing.T) testCertPaths {
	t.Helper()
	dir := t.TempDir()

	// CA.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "pgrouter test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caPath := filepath.Join(dir, "ca.pem")
	require.NoError(t, writePEM(caPath, "CERTIFICATE", caDER))
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	// Server cert.
	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	srvTpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTpl, caCert, &srvKey.PublicKey, caKey)
	require.NoError(t, err)
	srvPath := filepath.Join(dir, "server.crt")
	srvKeyPath := filepath.Join(dir, "server.key")
	require.NoError(t, writePEM(srvPath, "CERTIFICATE", srvDER))
	require.NoError(t, writeECKey(srvKeyPath, srvKey))

	// Client cert (for mTLS tests).
	cliKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cliTpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "pgrouter test client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	cliDER, err := x509.CreateCertificate(rand.Reader, cliTpl, caCert, &cliKey.PublicKey, caKey)
	require.NoError(t, err)
	cliPath := filepath.Join(dir, "client.crt")
	cliKeyPath := filepath.Join(dir, "client.key")
	require.NoError(t, writePEM(cliPath, "CERTIFICATE", cliDER))
	require.NoError(t, writeECKey(cliKeyPath, cliKey))

	return testCertPaths{
		CAFile:     caPath,
		ServerCert: srvPath,
		ServerKey:  srvKeyPath,
		ClientCert: cliPath,
		ClientKey:  cliKeyPath,
	}
}

func writePEM(path, typ string, der []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: typ, Bytes: der})
}

func writeECKey(path string, key *ecdsa.PrivateKey) error {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	return writePEM(path, "EC PRIVATE KEY", der)
}
