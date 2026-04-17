package auth

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUserlistParsesAllSecretTypes(t *testing.T) {
	verifier, err := MakeSCRAMVerifier("hunter2")
	require.NoError(t, err)
	body := strings.Join([]string{
		`# comment`,
		`; semicolon comment`,
		``,
		`"alice"   "wonderland"`,
		`"bob"     "md5d8578edf8458ce06fbc5bb76a58c5ca4"`,
		`"carol"   "` + verifier.String() + `"`,
	}, "\n")

	dir := t.TempDir()
	path := filepath.Join(dir, "userlist.txt")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

	ul, err := NewUserlist(path)
	require.NoError(t, err)
	require.Equal(t, 3, ul.Len())

	a, ok := ul.Lookup("alice")
	require.True(t, ok)
	require.Equal(t, "wonderland", a.PlainPassword)
	require.Empty(t, a.MD5Hash)
	require.Nil(t, a.SCRAMVerifier)

	b, ok := ul.Lookup("bob")
	require.True(t, ok)
	require.Empty(t, b.PlainPassword)
	require.Equal(t, "md5d8578edf8458ce06fbc5bb76a58c5ca4", b.MD5Hash)
	require.Nil(t, b.SCRAMVerifier)

	c, ok := ul.Lookup("carol")
	require.True(t, ok)
	require.True(t, c.EntryHasSCRAMVerifier())
	require.Equal(t, verifier.Iterations, c.SCRAMVerifier.Iterations)
}

func TestUserlistEscapes(t *testing.T) {
	body := `"name with \"quote\"" "pass \\with \\bs"`
	dir := t.TempDir()
	path := filepath.Join(dir, "userlist.txt")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

	ul, err := NewUserlist(path)
	require.NoError(t, err)
	e, ok := ul.Lookup(`name with "quote"`)
	require.True(t, ok)
	require.Equal(t, `pass \with \bs`, e.PlainPassword)
}

func TestUserlistMissingFile(t *testing.T) {
	_, err := NewUserlist(filepath.Join(t.TempDir(), "nope"))
	require.Error(t, err)
}

func TestUserlistMalformedLine(t *testing.T) {
	body := `"alice"`
	dir := t.TempDir()
	path := filepath.Join(dir, "userlist.txt")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

	_, err := NewUserlist(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected second")
}

func TestUserlistReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "userlist.txt")
	require.NoError(t, os.WriteFile(path, []byte(`"alice" "old"`), 0o600))
	ul, err := NewUserlist(path)
	require.NoError(t, err)
	a, _ := ul.Lookup("alice")
	require.Equal(t, "old", a.PlainPassword)

	require.NoError(t, os.WriteFile(path, []byte(`"alice" "new"`+"\n"+`"bob" "p"`), 0o600))
	require.NoError(t, ul.Reload())
	require.Equal(t, 2, ul.Len())
	a, _ = ul.Lookup("alice")
	require.Equal(t, "new", a.PlainPassword)
}
