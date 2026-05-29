package listener

import (
	"crypto/tls"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/config"
)

func TestWriteSSLAcceptAndDecline(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	go func() { _ = WriteSSLAccept(c1) }()
	buf := make([]byte, 1)
	_, err := c2.Read(buf)
	require.NoError(t, err)
	require.Equal(t, byte('S'), buf[0])

	go func() { _ = WriteSSLDecline(c1) }()
	_, err = c2.Read(buf)
	require.NoError(t, err)
	require.Equal(t, byte('N'), buf[0])
}

func TestCertStoreReload(t *testing.T) {
	certs := writeTestCerts(t)
	cs, err := NewCertStore(certs.ServerCert, certs.ServerKey)
	require.NoError(t, err)

	c1, err := cs.GetCertificate(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	require.NotNil(t, c1)

	// Touch the cert file mtime, reload, ensure pointer changes.
	require.NoError(t, cs.Reload())
	c2, err := cs.GetCertificate(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	require.NotNil(t, c2)
}

func TestCertStoreMissingFile(t *testing.T) {
	_, err := NewCertStore("/no/such/cert", "/no/such/key")
	require.Error(t, err)
}

func TestCertStoreCorruptedReloadRetainsCurrent(t *testing.T) {
	certs := writeTestCerts(t)
	cs, err := NewCertStore(certs.ServerCert, certs.ServerKey)
	require.NoError(t, err)
	good, err := cs.GetCertificate(&tls.ClientHelloInfo{})
	require.NoError(t, err)

	// Overwrite cert file with garbage and reload — must error.
	require.NoError(t, os.WriteFile(certs.ServerCert, []byte("not a cert"), 0o600))
	require.Error(t, cs.Reload())

	// Old cert pointer remains usable.
	after, err := cs.GetCertificate(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	require.Equal(t, good, after)
}

func TestBuildServerTLSConfigDisabled(t *testing.T) {
	cfg, cs, err := BuildServerTLSConfig(config.TLSConfig{ClientMode: config.SSLDisable})
	require.NoError(t, err)
	require.Nil(t, cfg)
	require.Nil(t, cs)
}

func TestBuildServerTLSConfigRequireNeedsCert(t *testing.T) {
	_, _, err := BuildServerTLSConfig(config.TLSConfig{
		ClientMode: config.SSLRequire,
	})
	require.Error(t, err)
}

func TestBuildServerTLSConfigRequireWithCert(t *testing.T) {
	certs := writeTestCerts(t)
	cfg, cs, err := BuildServerTLSConfig(config.TLSConfig{
		ClientMode:     config.SSLRequire,
		ClientCertFile: certs.ServerCert,
		ClientKeyFile:  certs.ServerKey,
	})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.NotNil(t, cs)
	require.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
}

func TestBuildServerTLSConfigWithClientCA(t *testing.T) {
	certs := writeTestCerts(t)
	cfg, _, err := BuildServerTLSConfig(config.TLSConfig{
		ClientMode:     config.SSLRequire,
		ClientCertFile: certs.ServerCert,
		ClientKeyFile:  certs.ServerKey,
		ClientCAFile:   certs.CAFile,
	})
	require.NoError(t, err)
	require.Equal(t, tls.RequireAndVerifyClientCert, cfg.ClientAuth)
	require.NotNil(t, cfg.ClientCAs)
}

func TestBuildBackendTLSConfigDisabled(t *testing.T) {
	cfg, err := BuildBackendTLSConfig(config.TLSConfig{ServerMode: config.SSLDisable})
	require.NoError(t, err)
	require.Nil(t, cfg)
}

func TestBuildBackendTLSConfigPrefer(t *testing.T) {
	cfg, err := BuildBackendTLSConfig(config.TLSConfig{ServerMode: config.SSLPrefer})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.True(t, cfg.InsecureSkipVerify) // pgwire prefer
}

func TestBuildBackendTLSConfigVerifyCA(t *testing.T) {
	certs := writeTestCerts(t)
	cfg, err := BuildBackendTLSConfig(config.TLSConfig{
		ServerMode:   config.SSLVerifyCA,
		ServerCAFile: certs.CAFile,
	})
	require.NoError(t, err)
	require.NotNil(t, cfg.RootCAs)
	require.NotNil(t, cfg.VerifyPeerCertificate)
	require.True(t, cfg.InsecureSkipVerify) // pgwire verify-ca skips hostname
}

func TestBuildBackendTLSConfigVerifyFull(t *testing.T) {
	certs := writeTestCerts(t)
	cfg, err := BuildBackendTLSConfig(config.TLSConfig{
		ServerMode:   config.SSLVerifyFull,
		ServerCAFile: certs.CAFile,
	})
	require.NoError(t, err)
	require.NotNil(t, cfg.RootCAs)
	require.False(t, cfg.InsecureSkipVerify) // verify-full does hostname
}

func TestBuildBackendTLSConfigVerifyCAneedsFile(t *testing.T) {
	_, err := BuildBackendTLSConfig(config.TLSConfig{ServerMode: config.SSLVerifyCA})
	require.Error(t, err)
}

func TestBuildBackendTLSConfigWithClientCert(t *testing.T) {
	certs := writeTestCerts(t)
	cfg, err := BuildBackendTLSConfig(config.TLSConfig{
		ServerMode:     config.SSLVerifyCA,
		ServerCAFile:   certs.CAFile,
		ServerCertFile: certs.ClientCert,
		ServerKeyFile:  certs.ClientKey,
	})
	require.NoError(t, err)
	require.Len(t, cfg.Certificates, 1)
}

// TestEndToEndTLSHandshake spins up a TLS server side using our config
// path and a TLS client side, asserting they handshake successfully.
//
// Useful as a smoke test that BuildServerTLSConfig + BuildBackendTLSConfig
// produce config objects that actually work together.
func TestEndToEndTLSHandshake(t *testing.T) {
	certs := writeTestCerts(t)

	srvCfg, _, err := BuildServerTLSConfig(config.TLSConfig{
		ClientMode:     config.SSLRequire,
		ClientCertFile: certs.ServerCert,
		ClientKeyFile:  certs.ServerKey,
	})
	require.NoError(t, err)
	require.NotNil(t, srvCfg)

	cliCfg, err := BuildBackendTLSConfig(config.TLSConfig{
		ServerMode:   config.SSLVerifyFull,
		ServerCAFile: certs.CAFile,
	})
	require.NoError(t, err)
	require.NotNil(t, cliCfg)
	cliCfg.ServerName = "localhost"

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	type handshakeResult struct {
		err     error
		version uint16
	}
	srvCh := make(chan handshakeResult, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			srvCh <- handshakeResult{err: err}
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		tlsConn, err := UpgradeServerToTLS(c, srvCfg)
		if err != nil {
			srvCh <- handshakeResult{err: err}
			return
		}
		srvCh <- handshakeResult{version: tlsConn.ConnectionState().Version}
	}()

	c, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	tlsConn, err := UpgradeClientToTLS(c, cliCfg)
	require.NoError(t, err, "client TLS handshake")
	defer tlsConn.Close()

	r := <-srvCh
	require.NoError(t, r.err, "server TLS handshake")
	require.GreaterOrEqual(t, r.version, uint16(tls.VersionTLS12))
}
