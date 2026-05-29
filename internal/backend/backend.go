// Package backend opens a TCP connection to an upstream Postgres and
// completes the v3 startup handshake. PoC scope: trust auth only.
// Real SCRAM auth + lifecycle wired in M.5 / M.7.
package backend

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/JustAnotherDevv/pgrouter/internal/listener"
)

// Conn is a ready upstream Postgres backend connection.
// After Dial returns, the backend has emitted ReadyForQuery and is
// awaiting a Query / Parse / Sync from the caller.
type Conn struct {
	NetConn     net.Conn
	Frontend    *pgproto3.Frontend
	PostgresPID uint32
	SecretKey   []byte
	Params      map[string]string
	Log         *slog.Logger
}

// DialOptions controls how a backend connection is established.
type DialOptions struct {
	Addr     string // host:port
	User     string
	Database string
	AppName  string // optional application_name
	Timeout  time.Duration
	Log      *slog.Logger

	// TLSConfig, if non-nil, makes Dial negotiate TLS before sending
	// StartupMessage. The ServerName must already be set (typically the
	// hostname portion of Addr). Nil = plain TCP.
	TLSConfig *tls.Config

	// TLSRequired controls how a backend's 'N' response to our
	// SSLRequest is handled.
	//   false  → fall back to plaintext (matches pgwire sslmode=prefer)
	//   true   → error (matches sslmode=require / verify-ca / verify-full)
	TLSRequired bool
}

// Dial opens a TCP connection to the upstream and performs the startup
// handshake. PoC supports only trust auth (AuthenticationOk immediately).
// SCRAM / MD5 are rejected with an error in this PoC scope.
func Dial(ctx context.Context, opts DialOptions) (*Conn, error) {
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	log := opts.Log.With("backend", opts.Addr, "user", opts.User, "database", opts.Database)

	d := net.Dialer{Timeout: opts.Timeout}
	c, err := d.DialContext(ctx, "tcp", opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", opts.Addr, err)
	}

	// Deadline only for the handshake; remove after we're connected.
	deadline := time.Now().Add(opts.Timeout)
	_ = c.SetDeadline(deadline)

	// Optional TLS upgrade BEFORE StartupMessage (pgwire flow).
	if opts.TLSConfig != nil {
		var sslReq [8]byte
		binary.BigEndian.PutUint32(sslReq[0:4], 8)
		binary.BigEndian.PutUint32(sslReq[4:8], listener.SSLRequestMagic)
		if _, err := c.Write(sslReq[:]); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("send SSLRequest: %w", err)
		}
		var resp [1]byte
		if _, err := io.ReadFull(c, resp[:]); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("read SSLRequest reply: %w", err)
		}
		switch resp[0] {
		case 'S':
			tlsConn, err := listener.UpgradeClientToTLS(c, opts.TLSConfig)
			if err != nil {
				_ = c.Close()
				return nil, fmt.Errorf("backend tls upgrade: %w", err)
			}
			c = tlsConn
			log.Debug("backend TLS upgrade ok",
				"version", tlsConn.ConnectionState().Version)
		case 'N':
			if opts.TLSRequired {
				_ = c.Close()
				return nil, errors.New("backend refused TLS and server_mode requires it")
			}
			log.Warn("backend declined TLS; falling back to plaintext")
		default:
			_ = c.Close()
			return nil, fmt.Errorf("unexpected SSLRequest reply byte 0x%02x", resp[0])
		}
	}

	fe := pgproto3.NewFrontend(c, c)

	// Send StartupMessage.
	params := map[string]string{
		"user":     opts.User,
		"database": opts.Database,
	}
	if opts.AppName != "" {
		params["application_name"] = opts.AppName
	}
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      params,
	}
	fe.Send(startup)
	if err := fe.Flush(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("send startup: %w", err)
	}

	conn := &Conn{
		NetConn:  c,
		Frontend: fe,
		Params:   map[string]string{},
		Log:      log,
	}

	// Read until ReadyForQuery.
	for {
		msg, err := fe.Receive()
		if err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("receive during handshake: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.AuthenticationOk:
			log.Debug("backend auth ok")
		case *pgproto3.AuthenticationCleartextPassword,
			*pgproto3.AuthenticationMD5Password,
			*pgproto3.AuthenticationSASL,
			*pgproto3.AuthenticationGSS:
			_ = c.Close()
			return nil, errors.New("backend requires auth other than trust; SCRAM/MD5 wired in MVP M.5")
		case *pgproto3.ParameterStatus:
			conn.Params[m.Name] = m.Value
		case *pgproto3.BackendKeyData:
			conn.PostgresPID = m.ProcessID
			conn.SecretKey = append([]byte(nil), m.SecretKey...)
		case *pgproto3.NoticeResponse:
			log.Info("backend notice", "severity", m.Severity, "message", m.Message)
		case *pgproto3.ErrorResponse:
			_ = c.Close()
			return nil, fmt.Errorf("backend startup error: %s: %s", m.Severity, m.Message)
		case *pgproto3.ReadyForQuery:
			_ = c.SetDeadline(time.Time{}) // clear deadline post-handshake
			log.Debug("backend ready",
				"tx_status", string(m.TxStatus),
				"backend_pid", conn.PostgresPID,
				"params_count", len(conn.Params),
			)
			return conn, nil
		default:
			log.Warn("backend unexpected handshake msg", "type", fmt.Sprintf("%T", m))
		}
	}
}

// Close terminates the backend connection.
func (c *Conn) Close() error {
	if c == nil || c.NetConn == nil {
		return nil
	}
	// Best-effort Terminate, then close.
	c.Frontend.Send(&pgproto3.Terminate{})
	_ = c.Frontend.Flush()
	return c.NetConn.Close()
}
