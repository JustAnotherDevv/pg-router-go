// auth_query: fetch the per-user credential from a real Postgres
// connection at auth time, PgBouncer-style.
//
// YAML:
//
//	auth:
//	  type: scram-sha-256          # or md5
//	  auth_user: pgrouter
//	  auth_query: SELECT usename, passwd FROM pg_shadow WHERE usename = $1
//
// Flow:
//   1. Client opens conn → StartupMessage(user=alice)
//   2. PerformServerAuthConn looks up "alice" in the Userlist; miss.
//   3. Fetcher dials upstream as `auth_user`, executes auth_query with
//      $1='alice', reads (returned_user, returned_password) row.
//   4. Synthesise a UserEntry from that row, treat as the userlist
//      entry for this auth attempt.
//   5. Result is cached briefly (default 60s TTL) so repeat connects
//      don't hammer the upstream.
//
// Username sanitisation: pg_shadow.usename is a plain identifier with
// restricted chars. We only allow [A-Za-z0-9_$] before substituting
// into the query; everything else is rejected before dial. This makes
// SQL-injection via the username byte-impossible even without using
// extended protocol.

package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// QueryConn is the minimal pgwire frontend the auth_query Fetcher
// needs. Declared locally to keep the auth package free of the backend
// import (which would cycle). main.go provides an adapter over
// *backend.Conn.
type QueryConn interface {
	Send(msg pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)
	Close() error
}

// FrontendAdapter wraps a *pgproto3.Frontend (plus closer) into the
// QueryConn shape, with an explicit Flush after Send so callers don't
// have to remember it.
type FrontendAdapter struct {
	Frontend *pgproto3.Frontend
	Closer   func() error
}

// Send delegates to the underlying Frontend and flushes immediately.
func (a *FrontendAdapter) Send(msg pgproto3.FrontendMessage) error {
	a.Frontend.Send(msg)
	return a.Frontend.Flush()
}

// Receive delegates to the underlying Frontend.
func (a *FrontendAdapter) Receive() (pgproto3.BackendMessage, error) {
	return a.Frontend.Receive()
}

// Close invokes the supplied close func; returns nil if none was set.
func (a *FrontendAdapter) Close() error {
	if a.Closer == nil {
		return nil
	}
	return a.Closer()
}

// AuthQueryFetcher resolves usernames via an upstream Postgres conn.
type AuthQueryFetcher struct {
	// Dial opens an upstream conn as auth_user against the requested db.
	// pgrouter's main wires this using backend.Dial + per-(db) addr/creds.
	Dial func(ctx context.Context, db string) (QueryConn, error)

	// Query is the SQL with `$1` placeholder. Typically:
	//   SELECT usename, passwd FROM pg_shadow WHERE usename = $1
	Query string

	// TTL bounds the result cache (default 60s).
	TTL time.Duration

	mu    sync.Mutex
	cache map[string]cachedEntry
}

type cachedEntry struct {
	entry *UserEntry
	at    time.Time
}

// NewAuthQueryFetcher returns a configured fetcher.
func NewAuthQueryFetcher(dial func(context.Context, string) (QueryConn, error), query string, ttl time.Duration) *AuthQueryFetcher {
	if ttl == 0 {
		ttl = 60 * time.Second
	}
	return &AuthQueryFetcher{
		Dial:  dial,
		Query: query,
		TTL:   ttl,
		cache: map[string]cachedEntry{},
	}
}

// Lookup fetches the credential for username. db is the database the
// client wants (passed to Dial); the auth_user identity Dial uses is
// pgrouter-side config.
func (f *AuthQueryFetcher) Lookup(ctx context.Context, db, username string) (*UserEntry, error) {
	if !validIdent(username) {
		return nil, fmt.Errorf("invalid username for auth_query: %q", username)
	}
	f.mu.Lock()
	if e, ok := f.cache[username]; ok && time.Since(e.at) < f.TTL {
		f.mu.Unlock()
		return e.entry, nil
	}
	f.mu.Unlock()

	conn, err := f.Dial(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("auth_query dial: %w", err)
	}
	defer conn.Close()

	sql := substituteOne(f.Query, username)
	if err := conn.Send(&pgproto3.Query{String: sql}); err != nil {
		return nil, fmt.Errorf("auth_query send: %w", err)
	}

	// We expect EXACTLY one row of (usename, passwd). Multiple rows
	// indicate a misconfigured query (e.g. wildcard LIKE instead of
	// '='); we reject rather than silently using the first because
	// the wrong row could grant the wrong user's credential to the
	// connecting client. Drain to RFQ either way so the conn stays
	// usable (we Close it on return anyway, but RFQ is good hygiene).
	var (
		firstRow  []string
		extraRows int
		errResp   string
	)
	for {
		msg, err := conn.Receive()
		if err != nil {
			return nil, fmt.Errorf("auth_query recv: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.DataRow:
			if firstRow == nil {
				firstRow = make([]string, len(m.Values))
				for i, c := range m.Values {
					firstRow[i] = string(c)
				}
			} else {
				extraRows++
			}
		case *pgproto3.ErrorResponse:
			errResp = m.Message
		case *pgproto3.ReadyForQuery:
			goto done
		}
	}
done:
	if errResp != "" {
		return nil, fmt.Errorf("auth_query error: %s", errResp)
	}
	if firstRow == nil || len(firstRow) < 2 {
		return nil, errors.New("auth_query returned no row")
	}
	if extraRows > 0 {
		return nil, fmt.Errorf("auth_query returned %d rows (expected exactly 1); "+
			"check the query uses '=' not LIKE/ILIKE for the username column",
			extraRows+1)
	}
	entry := classifySecret(firstRow[0], firstRow[1])
	f.mu.Lock()
	f.cache[username] = cachedEntry{entry: entry, at: time.Now()}
	f.mu.Unlock()
	return entry, nil
}

// substituteOne replaces "$1" in q with a SQL-quoted literal of v.
// v is already validIdent-gated by the caller.
func substituteOne(q, v string) string {
	return strings.ReplaceAll(q, "$1", "'"+v+"'")
}

// validIdent gates usernames to [A-Za-z0-9_$] (PG identifier chars).
// Empty / overly-long names are rejected.
func validIdent(s string) bool {
	if len(s) == 0 || len(s) > 63 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '$'
		if !ok {
			return false
		}
	}
	return true
}
