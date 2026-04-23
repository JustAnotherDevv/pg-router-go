package auth

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseHBABasic(t *testing.T) {
	body := `# comment
# type     database  user        cidr             method
host       all       all         0.0.0.0/0        scram-sha-256
host       appdb     alice       10.0.0.0/8       md5
hostssl    all       all         ::/0             cert
local      all       all                          peer
host       all       all         192.168.1.1      trust
`
	rules, err := ParseHBA(strings.NewReader(body))
	require.NoError(t, err)
	require.Len(t, rules, 5)

	require.Equal(t, "scram-sha-256", rules[0].Method)
	require.Equal(t, []string{"all"}, rules[0].Databases)
	require.Equal(t, "0.0.0.0/0", rules[0].CIDR.String())

	require.Equal(t, "md5", rules[1].Method)
	require.Equal(t, "appdb", rules[1].Databases[0])
	require.Equal(t, "alice", rules[1].Users[0])

	require.Equal(t, "hostssl", rules[2].Type)
	require.Equal(t, "cert", rules[2].Method)

	require.Equal(t, "local", rules[3].Type)
	require.Nil(t, rules[3].CIDR)

	// Bare IP without /mask got inferred to /32.
	require.Equal(t, "192.168.1.1/32", rules[4].CIDR.String())
}

func TestHBAFileMatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hba.conf")
	require.NoError(t, os.WriteFile(path, []byte(`
host    appdb    alice    10.0.0.0/8    md5
host    all      all      0.0.0.0/0     scram-sha-256
local   all      all                    peer
`), 0o600))
	h, err := NewHBAFile(path)
	require.NoError(t, err)

	r, ok := h.Match("appdb", "alice", net.ParseIP("10.5.5.5"), false)
	require.True(t, ok)
	require.Equal(t, "md5", r.Method)

	// Different user → falls to the wildcard rule.
	r, ok = h.Match("appdb", "bob", net.ParseIP("10.5.5.5"), false)
	require.True(t, ok)
	require.Equal(t, "scram-sha-256", r.Method)

	// Unix socket → local rule.
	r, ok = h.Match("appdb", "alice", nil, false)
	require.True(t, ok)
	require.Equal(t, "peer", r.Method)
}

func TestHBAFileNoMatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hba.conf")
	require.NoError(t, os.WriteFile(path, []byte(
		`host  appdb  alice  10.0.0.0/8  md5`), 0o600))
	h, err := NewHBAFile(path)
	require.NoError(t, err)

	_, ok := h.Match("otherdb", "alice", net.ParseIP("10.0.0.1"), false)
	require.False(t, ok)
}

func TestParseHBARejectsMalformed(t *testing.T) {
	_, err := ParseHBA(strings.NewReader("host all"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "too few fields")
}

func TestParseHBAUnknownType(t *testing.T) {
	_, err := ParseHBA(strings.NewReader("zoo all all 0.0.0.0/0 trust"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown type")
}

func TestHBAReloadRereads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hba.conf")
	require.NoError(t, os.WriteFile(path, []byte(
		`host  appdb  alice  10.0.0.0/8  md5`), 0o600))
	h, err := NewHBAFile(path)
	require.NoError(t, err)
	r, _ := h.Match("appdb", "alice", net.ParseIP("10.0.0.1"), false)
	require.Equal(t, "md5", r.Method)

	require.NoError(t, os.WriteFile(path, []byte(
		`host  appdb  alice  10.0.0.0/8  scram-sha-256`), 0o600))
	require.NoError(t, h.Reload())
	r, _ = h.Match("appdb", "alice", net.ParseIP("10.0.0.1"), false)
	require.Equal(t, "scram-sha-256", r.Method)
}

func TestHBAHostsslMatchOnlyTLS(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hba.conf")
	require.NoError(t, os.WriteFile(path, []byte(`
hostssl   all  all  0.0.0.0/0  cert
host      all  all  0.0.0.0/0  trust
`), 0o600))
	h, err := NewHBAFile(path)
	require.NoError(t, err)

	r, _ := h.Match("appdb", "alice", net.ParseIP("10.0.0.1"), true)
	require.Equal(t, "cert", r.Method)

	r, _ = h.Match("appdb", "alice", net.ParseIP("10.0.0.1"), false)
	require.Equal(t, "trust", r.Method, "plaintext should fall to host rule")
}
