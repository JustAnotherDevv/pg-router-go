// PgBouncer-compatible userlist.txt parser.
//
// Format: one user per line, two double-quoted fields per line,
// separated by whitespace. The first field is the username, the second
// is either a cleartext password, an md5-hashed password ("md5..."), or
// a SCRAM verifier ("SCRAM-SHA-256$...").
//
//	"alice" "wonderland"
//	"bob"   "md5abc1234..."
//	"carol" "SCRAM-SHA-256$4096:..."
//
// Lines starting with `;` or `#` are comments. Blank lines are allowed.

package auth

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

// UserEntry holds the credential bytes for one user.
//
// Exactly one of PlainPassword / MD5Hash / SCRAMVerifier is populated
// (the type is inferred from the stored string prefix).
type UserEntry struct {
	Username       string
	PlainPassword  string         // empty unless raw cleartext
	MD5Hash        string         // "md5<32 hex>" form, empty if not md5
	SCRAMVerifier  *SCRAMVerifier // populated for SCRAM-SHA-256$
}

// Userlist is a thread-safe in-memory user list with atomic reload.
type Userlist struct {
	mu      sync.RWMutex
	entries map[string]*UserEntry
	path    string
}

// NewUserlist loads a userlist.txt from disk.
func NewUserlist(path string) (*Userlist, error) {
	u := &Userlist{path: path}
	if err := u.Reload(); err != nil {
		return nil, err
	}
	return u, nil
}

// Reload re-reads the userlist file. New conns see the new data
// immediately; in-flight conns are unaffected.
func (u *Userlist) Reload() error {
	f, err := os.Open(u.path)
	if err != nil {
		return fmt.Errorf("open userlist %s: %w", u.path, err)
	}
	defer f.Close()
	parsed, err := parseUserlist(f)
	if err != nil {
		return fmt.Errorf("parse userlist %s: %w", u.path, err)
	}
	u.mu.Lock()
	u.entries = parsed
	u.mu.Unlock()
	return nil
}

// Lookup returns the entry for username or (nil, false).
func (u *Userlist) Lookup(username string) (*UserEntry, bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	e, ok := u.entries[username]
	return e, ok
}

// Len returns the number of users currently loaded.
func (u *Userlist) Len() int {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return len(u.entries)
}

// parseUserlist reads one entry per line in PgBouncer userlist.txt format.
func parseUserlist(r io.Reader) (map[string]*UserEntry, error) {
	out := make(map[string]*UserEntry)
	sc := bufio.NewScanner(r)
	lineNum := 0
	for sc.Scan() {
		lineNum++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, ";") || strings.HasPrefix(line, "#") {
			continue
		}
		user, secret, err := splitTwoQuotedFields(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNum, err)
		}
		entry := classifySecret(user, secret)
		out[user] = entry
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// splitTwoQuotedFields parses `"a" "b"` into ("a", "b").
//
// PgBouncer also supports escape sequences inside the quoted strings;
// we treat them as literal (\\ → \\, \" → ") which matches actual usage.
func splitTwoQuotedFields(line string) (string, string, error) {
	a, rest, err := readQuoted(line)
	if err != nil {
		return "", "", err
	}
	rest = strings.TrimLeft(rest, " \t")
	if rest == "" {
		return "", "", fmt.Errorf("expected second quoted field after %q", a)
	}
	b, tail, err := readQuoted(rest)
	if err != nil {
		return "", "", err
	}
	if strings.TrimSpace(tail) != "" {
		return "", "", fmt.Errorf("unexpected trailing data %q", tail)
	}
	return a, b, nil
}

// readQuoted reads one "..." (with \" + \\ escapes) and returns
// (value, remainder, err).
func readQuoted(s string) (string, string, error) {
	s = strings.TrimLeft(s, " \t")
	if !strings.HasPrefix(s, `"`) {
		return "", "", fmt.Errorf("expected quoted string, got %q", s)
	}
	var b strings.Builder
	i := 1
	for i < len(s) {
		c := s[i]
		switch c {
		case '\\':
			if i+1 >= len(s) {
				return "", "", fmt.Errorf("trailing backslash")
			}
			b.WriteByte(s[i+1])
			i += 2
		case '"':
			return b.String(), s[i+1:], nil
		default:
			b.WriteByte(c)
			i++
		}
	}
	return "", "", fmt.Errorf("unterminated quoted string")
}

// classifySecret inspects the stored credential and returns an
// appropriately populated UserEntry.
func classifySecret(user, secret string) *UserEntry {
	e := &UserEntry{Username: user}
	switch {
	case strings.HasPrefix(secret, "SCRAM-SHA-256$"):
		v, err := ParseSCRAMVerifier(secret)
		if err == nil {
			e.SCRAMVerifier = v
			return e
		}
		// Fall through to plain on parse failure (defensive).
		e.PlainPassword = secret
	case len(secret) == 35 && strings.HasPrefix(secret, "md5"):
		e.MD5Hash = secret
	default:
		e.PlainPassword = secret
	}
	return e
}

// EntryHasSCRAMVerifier returns true if the entry has a parsed
// SCRAM verifier (preferred over MD5 + plaintext).
func (e *UserEntry) EntryHasSCRAMVerifier() bool {
	return e != nil && e.SCRAMVerifier != nil
}
