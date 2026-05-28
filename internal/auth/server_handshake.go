// Server-side auth handshakes: pgrouter authenticates clients via
// userlist.txt-backed trust / MD5 / SCRAM-SHA-256, or — on Unix
// socket conns — via SO_PEERCRED ("peer" auth).
//
// Called from internal/client during the startup phase.

package auth

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/JustAnotherDevv/pgrouter/internal/config"
)

// ServerAuthOptions carries the startup-auth decisions: which AuthType
// to enforce, which Userlist to query, and optional HBA/auth_query state.
type ServerAuthOptions struct {
	Type     config.AuthType
	Userlist *Userlist         // nil for trust mode
	HBA      *HBAFile          // non-nil when Type == "hba"
	Fetcher  *AuthQueryFetcher // non-nil when auth_query is configured
	Log      *slog.Logger

	// DBName is the StartupMessage database; needed for HBA matching
	// + auth_query bootstrap conn target.
	DBName string
}

// PerformServerAuth runs the server-side authentication phase against
// a newly-connected client after StartupMessage has been parsed.
//
// Returns nil on success; the caller should then send the
// AuthenticationOk + ParameterStatus* + BackendKeyData + ReadyForQuery
// sequence (or hand off to the proxy forwarder).
//
// Deprecated: prefer PerformServerAuthConn which can authenticate via
// peer credentials on Unix sockets. PerformServerAuth keeps the old
// signature working for trust / MD5 / SCRAM tests that don't need the
// raw net.Conn.
func PerformServerAuth(be *pgproto3.Backend, opts ServerAuthOptions, username string) error {
	return PerformServerAuthConn(be, nil, opts, username)
}

// PerformServerAuthConn is PerformServerAuth + access to the raw client
// net.Conn so peer auth can call SO_PEERCRED. Pass `conn == nil` only
// when peer auth is impossible (TCP-only listener / tests).
func PerformServerAuthConn(be *pgproto3.Backend, conn net.Conn, opts ServerAuthOptions, username string) error {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	switch opts.Type {
	case "", config.AuthTrust:
		// Nothing to do — caller sends AuthenticationOk itself.
		return nil

	case config.AuthMD5:
		entry, err := resolveEntry(opts, username)
		if err != nil {
			return sendAuthFailed(be, log, err.Error())
		}
		return doServerMD5(be, log, username, entry)

	case config.AuthSCRAM:
		entry, err := resolveEntry(opts, username)
		if err != nil {
			return sendAuthFailed(be, log, err.Error())
		}
		return doServerSCRAM(be, log, username, entry)

	case config.AuthPeer:
		if conn == nil {
			return sendAuthFailed(be, log,
				"peer auth requires a Unix socket connection")
		}
		return doServerPeer(be, conn, log, username)

	case config.AuthCert:
		if conn == nil {
			return sendAuthFailed(be, log,
				"cert auth requires a TLS connection")
		}
		return doServerCert(be, conn, log, username)

	case config.AuthHBA:
		if opts.HBA == nil {
			return sendAuthFailed(be, log, "hba auth requires hba_file")
		}
		return doServerHBA(be, conn, opts, log, username)

	default:
		return fmt.Errorf("auth type %q not supported", opts.Type)
	}
}

// hbaMethodHandler runs a single HBA method against an
// already-matched rule. Implementations live below + are registered in
// hbaMethods. Returning an error means auth failed; the wrapper has
// already sent the FATAL ErrorResponse to the client (if needed).
type hbaMethodHandler func(be *pgproto3.Backend, conn net.Conn,
	opts ServerAuthOptions, log *slog.Logger, username string,
	rule HBARule) error

// hbaMethods maps method names to handlers. Replaces the previous
// switch statement so adding a new method (gss, ldap, etc.) is a
// single-line registration.
var hbaMethods = map[string]hbaMethodHandler{
	"trust":         hbaTrust,
	"reject":        hbaReject,
	"md5":           hbaMD5,
	"scram-sha-256": hbaSCRAM,
	"peer":          hbaPeer,
	"cert":          hbaCert,
}

// doServerHBA matches (db, user, peer-ip, tls?) against the HBA
// ruleset and dispatches to the rule's method handler via hbaMethods.
func doServerHBA(be *pgproto3.Backend, conn net.Conn, opts ServerAuthOptions, log *slog.Logger, username string) error {
	var (
		peerIP net.IP
		isTLS  bool
	)
	if conn != nil {
		if a := conn.RemoteAddr(); a != nil {
			if tcp, ok := a.(*net.TCPAddr); ok {
				peerIP = tcp.IP
			}
		}
		if _, ok := conn.(interface{ ConnectionState() any }); ok {
			isTLS = true
		}
	}
	rule, ok := opts.HBA.Match(opts.DBName, username, peerIP, isTLS)
	if !ok {
		return sendAuthFailed(be, log,
			fmt.Sprintf("hba: no rule matches (db=%q user=%q ip=%v tls=%v)",
				opts.DBName, username, peerIP, isTLS))
	}
	log.Debug("hba match", "rule_line", rule.LineNum, "method", rule.Method)

	h, ok := hbaMethods[rule.Method]
	if !ok {
		return sendAuthFailed(be, log,
			fmt.Sprintf("hba method %q not supported", rule.Method))
	}
	return h(be, conn, opts, log, username, rule)
}

func hbaTrust(_ *pgproto3.Backend, _ net.Conn, _ ServerAuthOptions, _ *slog.Logger, _ string, _ HBARule) error {
	return nil
}

func hbaReject(be *pgproto3.Backend, _ net.Conn, _ ServerAuthOptions, log *slog.Logger, _ string, rule HBARule) error {
	return sendAuthFailed(be, log,
		fmt.Sprintf("hba rule line %d rejects this conn", rule.LineNum))
}

func hbaMD5(be *pgproto3.Backend, _ net.Conn, opts ServerAuthOptions, log *slog.Logger, username string, _ HBARule) error {
	entry, err := hbaResolveUserlistEntry(opts, username, "md5")
	if err != nil {
		return sendAuthFailed(be, log, err.Error())
	}
	return doServerMD5(be, log, username, entry)
}

func hbaSCRAM(be *pgproto3.Backend, _ net.Conn, opts ServerAuthOptions, log *slog.Logger, username string, _ HBARule) error {
	entry, err := hbaResolveUserlistEntry(opts, username, "scram")
	if err != nil {
		return sendAuthFailed(be, log, err.Error())
	}
	return doServerSCRAM(be, log, username, entry)
}

func hbaPeer(be *pgproto3.Backend, conn net.Conn, _ ServerAuthOptions, log *slog.Logger, username string, _ HBARule) error {
	return doServerPeer(be, conn, log, username)
}

func hbaCert(be *pgproto3.Backend, conn net.Conn, _ ServerAuthOptions, log *slog.Logger, username string, _ HBARule) error {
	return doServerCert(be, conn, log, username)
}

// hbaResolveUserlistEntry centralises the "need userlist + user must
// exist" precondition shared by HBA md5 + scram.
func hbaResolveUserlistEntry(opts ServerAuthOptions, username, method string) (*UserEntry, error) {
	if opts.Userlist == nil {
		return nil, fmt.Errorf("hba %s needs userlist", method)
	}
	entry, ok := opts.Userlist.Lookup(username)
	if !ok {
		return nil, fmt.Errorf("user %q not found", username)
	}
	return entry, nil
}

// doServerPeer authenticates via SO_PEERCRED — the OS uid on the far
// side of the Unix socket. Match → accept, mismatch → FATAL 28P01.
//
// Caller is responsible for ensuring `conn` is a *net.UnixConn (the
// peer subsystem returns a clean error otherwise).
func doServerPeer(be *pgproto3.Backend, conn net.Conn, log *slog.Logger, username string) error {
	peerUser, err := PeerUsername(conn)
	if err != nil {
		return sendAuthFailed(be, log,
			fmt.Sprintf("peer-cred lookup failed: %v", err))
	}
	if peerUser != username {
		return sendAuthFailed(be, log,
			fmt.Sprintf("peer auth: socket uid maps to %q, client claimed %q",
				peerUser, username))
	}
	log.Debug("peer auth ok", "user", username)
	return nil
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

// resolveEntry returns the UserEntry for username. Tries Userlist
// first; falls back to auth_query Fetcher if userlist misses (or is
// nil). Returns an error suitable for sendAuthFailed.
func resolveEntry(opts ServerAuthOptions, username string) (*UserEntry, error) {
	if opts.Userlist != nil {
		if e, ok := opts.Userlist.Lookup(username); ok {
			return e, nil
		}
	}
	if opts.Fetcher != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e, err := opts.Fetcher.Lookup(ctx, opts.DBName, username)
		if err != nil {
			return nil, fmt.Errorf("auth_query: %w", err)
		}
		return e, nil
	}
	if opts.Userlist == nil {
		return nil, errors.New("auth backend has no userlist or auth_query")
	}
	return nil, fmt.Errorf("user %q not found", username)
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
