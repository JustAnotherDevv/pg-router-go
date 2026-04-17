package listener

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/JustAnotherDevv/pgrouter/internal/config"
)

// CertStore loads + holds a TLS cert + key from disk, with an atomic
// pointer so SIGHUP-driven reloads can swap the cert without dropping
// in-flight connections.
//
// Reload() re-reads the original file paths and atomically replaces the
// cached cert. Existing tls.Conn objects keep using the cert they
// negotiated with; new connections see the fresh cert.
type CertStore struct {
	certFile string
	keyFile  string

	current atomic.Pointer[tls.Certificate]
}

// NewCertStore loads the initial cert + key.
func NewCertStore(certFile, keyFile string) (*CertStore, error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("cert and key files are both required")
	}
	cs := &CertStore{certFile: certFile, keyFile: keyFile}
	if err := cs.Reload(); err != nil {
		return nil, err
	}
	return cs, nil
}

// Reload re-reads the cert + key from disk and atomically swaps the
// in-memory copy. Returns nil on success, an error if the new pair is
// unreadable or invalid (the previous cert remains active in that case).
func (cs *CertStore) Reload() error {
	pair, err := tls.LoadX509KeyPair(cs.certFile, cs.keyFile)
	if err != nil {
		return fmt.Errorf("load %s + %s: %w", cs.certFile, cs.keyFile, err)
	}
	cs.current.Store(&pair)
	return nil
}

// GetCertificate satisfies tls.Config.GetCertificate. Returning a fresh
// pointer to the current cert on every connection lets reloads kick in
// without restarting the listener.
func (cs *CertStore) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	if c := cs.current.Load(); c != nil {
		return c, nil
	}
	return nil, errors.New("certificate not loaded")
}

// loadCAPool reads a PEM-encoded CA bundle into a *x509.CertPool.
func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read CA bundle %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no valid PEM blocks in %s", path)
	}
	return pool, nil
}

// BuildServerTLSConfig assembles a *tls.Config for the client-facing
// listener from the user's config.TLSConfig.
//
// Returns (nil, nil) when client_mode=disable — i.e. TLS off, signal
// upstream to write 'N' to SSLRequest.
func BuildServerTLSConfig(cfg config.TLSConfig) (*tls.Config, *CertStore, error) {
	if cfg.ClientMode == config.SSLDisable || cfg.ClientMode == "" {
		return nil, nil, nil
	}
	if cfg.ClientCertFile == "" || cfg.ClientKeyFile == "" {
		return nil, nil, fmt.Errorf("client_mode=%s requires client_cert_file + client_key_file", cfg.ClientMode)
	}
	cs, err := NewCertStore(cfg.ClientCertFile, cfg.ClientKeyFile)
	if err != nil {
		return nil, nil, err
	}
	tlsCfg := &tls.Config{
		GetCertificate: cs.GetCertificate,
		MinVersion:     tls.VersionTLS12,
	}
	// Optional CA for mTLS / client-cert verification.
	if cfg.ClientCAFile != "" {
		pool, err := loadCAPool(cfg.ClientCAFile)
		if err != nil {
			return nil, nil, err
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tlsCfg, cs, nil
}

// BuildBackendTLSConfig assembles a *tls.Config for outgoing
// connections to the upstream Postgres. ServerName is filled per-dial.
func BuildBackendTLSConfig(cfg config.TLSConfig) (*tls.Config, error) {
	if cfg.ServerMode == config.SSLDisable || cfg.ServerMode == "" {
		return nil, nil
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	switch cfg.ServerMode {
	case config.SSLAllow, config.SSLPrefer, config.SSLRequire:
		// Verification not required (but encrypted).
		tlsCfg.InsecureSkipVerify = true //nolint:gosec // intentional per sslmode
	case config.SSLVerifyCA:
		if cfg.ServerCAFile == "" {
			return nil, fmt.Errorf("server_mode=%s requires server_ca_file", cfg.ServerMode)
		}
		pool, err := loadCAPool(cfg.ServerCAFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.RootCAs = pool
		// verify-ca skips hostname check but verifies chain.
		tlsCfg.InsecureSkipVerify = true //nolint:gosec // pgwire verify-ca
		tlsCfg.VerifyPeerCertificate = makePoolOnlyVerifier(pool)
	case config.SSLVerifyFull:
		if cfg.ServerCAFile == "" {
			return nil, fmt.Errorf("server_mode=%s requires server_ca_file", cfg.ServerMode)
		}
		pool, err := loadCAPool(cfg.ServerCAFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.RootCAs = pool
		// verify-full: full chain + hostname.
	}
	// Optional client cert for mTLS towards upstream.
	if cfg.ServerCertFile != "" && cfg.ServerKeyFile != "" {
		pair, err := tls.LoadX509KeyPair(cfg.ServerCertFile, cfg.ServerKeyFile)
		if err != nil {
			return nil, fmt.Errorf("backend client cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{pair}
	}
	return tlsCfg, nil
}

// makePoolOnlyVerifier builds a callback that verifies the chain
// against `pool` but ignores hostname mismatch (pgwire verify-ca).
func makePoolOnlyVerifier(pool *x509.CertPool) func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("no peer cert presented")
		}
		certs := make([]*x509.Certificate, 0, len(rawCerts))
		for _, r := range rawCerts {
			c, err := x509.ParseCertificate(r)
			if err != nil {
				return fmt.Errorf("parse peer cert: %w", err)
			}
			certs = append(certs, c)
		}
		opts := x509.VerifyOptions{
			Roots:         pool,
			Intermediates: x509.NewCertPool(),
		}
		for _, c := range certs[1:] {
			opts.Intermediates.AddCert(c)
		}
		_, err := certs[0].Verify(opts)
		return err
	}
}
