// pg_hba.conf parser + matcher.
//
// PostgreSQL's host-based-auth file is a simple line-oriented format:
//
//	# type   database   user            cidr-or-host    method   [option=value ...]
//	host     all        all             0.0.0.0/0       scram-sha-256
//	host     appdb      alice           10.0.0.0/8      md5
//	hostssl  all        all             ::/0            cert
//	local    all        all                             peer
//
// Rules match top-to-bottom; first matching row wins. Comments (#) and
// blank lines are skipped. "all" is a wildcard. Multiple comma-separated
// values per field expand into OR-matches.
//
// pgrouter uses this as the `auth.type = hba` backend: per-client
// auth.PerformServerAuthConn looks up the method via HBAFile.Match
// and dispatches to the existing scram/md5/peer/cert handlers.

package auth

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
)

// HBARule is one row of pg_hba.conf.
type HBARule struct {
	Type      string   // "local" | "host" | "hostssl" | "hostnossl"
	Databases []string // expanded list; "all" → ["all"]
	Users     []string // expanded list
	CIDR      *net.IPNet // nil for "local" rules
	Method    string   // "trust" | "scram-sha-256" | "md5" | "peer" | "cert" | "reject"
	LineNum   int
	Raw       string
}

// HBAFile is a thread-safe in-memory ruleset with atomic reload.
type HBAFile struct {
	mu    sync.RWMutex
	rules []HBARule
	path  string
}

// NewHBAFile loads pg_hba.conf from disk.
func NewHBAFile(path string) (*HBAFile, error) {
	h := &HBAFile{path: path}
	if err := h.Reload(); err != nil {
		return nil, err
	}
	return h, nil
}

// Reload re-reads the file.
func (h *HBAFile) Reload() error {
	f, err := os.Open(h.path)
	if err != nil {
		return fmt.Errorf("open hba %s: %w", h.path, err)
	}
	defer f.Close()
	rules, err := ParseHBA(f)
	if err != nil {
		return fmt.Errorf("parse hba %s: %w", h.path, err)
	}
	h.mu.Lock()
	h.rules = rules
	h.mu.Unlock()
	return nil
}

// Match returns the first rule whose (type, database, user, ip)
// matches the request. `tls` controls hostssl/hostnossl. `ip` may be
// nil for Unix-socket / local connections (matches "local" rules only).
//
// Returns ("", false) when no rule matches — the caller should reject.
func (h *HBAFile) Match(db, user string, ip net.IP, tls bool) (HBARule, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, r := range h.rules {
		if !matchType(r.Type, ip == nil, tls) {
			continue
		}
		if !inListWildcard(r.Databases, db) {
			continue
		}
		if !inListWildcard(r.Users, user) {
			continue
		}
		if ip != nil {
			if r.CIDR == nil || !r.CIDR.Contains(ip) {
				continue
			}
		}
		return r, true
	}
	return HBARule{}, false
}

// matchType returns true if rule.Type matches the conn type.
//
//	local        — Unix socket only (ip == nil)
//	host         — TCP, TLS or plain
//	hostssl      — TCP + TLS only
//	hostnossl    — TCP + plaintext only
func matchType(t string, local bool, tls bool) bool {
	switch t {
	case "local":
		return local
	case "host":
		return !local
	case "hostssl":
		return !local && tls
	case "hostnossl":
		return !local && !tls
	}
	return false
}

// inListWildcard returns true if v ∈ list or "all" ∈ list. Names
// starting with "+" (role membership) are treated as exact match for
// MVP — full role-membership lookup is post-MVP.
func inListWildcard(list []string, v string) bool {
	for _, x := range list {
		switch {
		case x == "all":
			return true
		case strings.HasPrefix(x, "+"):
			if strings.TrimPrefix(x, "+") == v {
				return true
			}
		case x == v:
			return true
		}
	}
	return false
}

// ParseHBA reads an io.Reader containing pg_hba.conf-style lines and
// returns the parsed rule slice.
func ParseHBA(r io.Reader) ([]HBARule, error) {
	var rules []HBARule
	sc := bufio.NewScanner(r)
	lineNum := 0
	for sc.Scan() {
		lineNum++
		raw := sc.Text()
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := splitHBAFields(line)
		if len(fields) < 4 {
			return nil, fmt.Errorf("line %d: too few fields: %q", lineNum, raw)
		}
		r := HBARule{Type: fields[0], LineNum: lineNum, Raw: raw}
		switch r.Type {
		case "local":
			// local  database  user  method
			if len(fields) < 4 {
				return nil, fmt.Errorf("line %d: local needs 4 fields: %q", lineNum, raw)
			}
			r.Databases = strings.Split(fields[1], ",")
			r.Users = strings.Split(fields[2], ",")
			r.Method = fields[3]
		case "host", "hostssl", "hostnossl":
			// type  database  user  cidr  method
			if len(fields) < 5 {
				return nil, fmt.Errorf("line %d: %s needs 5 fields: %q", lineNum, r.Type, raw)
			}
			r.Databases = strings.Split(fields[1], ",")
			r.Users = strings.Split(fields[2], ",")
			cidr := fields[3]
			// Bare IP without /mask → infer /32 or /128.
			if !strings.Contains(cidr, "/") {
				if strings.Contains(cidr, ":") {
					cidr += "/128"
				} else {
					cidr += "/32"
				}
			}
			_, ipnet, err := net.ParseCIDR(cidr)
			if err != nil {
				return nil, fmt.Errorf("line %d: bad CIDR %q: %w", lineNum, fields[3], err)
			}
			r.CIDR = ipnet
			r.Method = fields[4]
		default:
			return nil, fmt.Errorf("line %d: unknown type %q", lineNum, r.Type)
		}
		rules = append(rules, r)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return rules, nil
}

// splitHBAFields splits on whitespace (no quoting / escapes in MVP).
func splitHBAFields(line string) []string {
	return strings.Fields(line)
}
