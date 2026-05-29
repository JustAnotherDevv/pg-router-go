// Package depcheck exists solely to keep core dependencies as direct,
// not indirect. Each dependency is touched in a passing test so it
// appears in `go.mod`'s require block (not as // indirect). Delete
// this package once each dep is used elsewhere in the codebase.
package depcheck

import (
	"bytes"
	"testing"

	v2 "github.com/jackc/pgproto3/v2"
	v5 "github.com/jackc/pgx/v5/pgproto3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestPgproto3v2 ensures jackc/pgproto3/v2 (the older API) is usable.
// This is the API many PgBouncer-compat examples use.
func TestPgproto3v2(t *testing.T) {
	msg := &v2.ReadyForQuery{TxStatus: 'I'}
	buf, err := msg.Encode(nil)
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	require.Equal(t, byte('Z'), buf[0], "ReadyForQuery message tag is 'Z'")
}

// TestPgxV5Pgproto3 ensures jackc/pgx/v5's bundled pgproto3 works too.
// We may end up using this newer API instead — keep both available.
func TestPgxV5Pgproto3(t *testing.T) {
	msg := &v5.ReadyForQuery{TxStatus: 'I'}
	buf, err := msg.Encode(nil)
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	require.Equal(t, byte('Z'), buf[0])
}

// TestYAMLRoundtrip ensures yaml.v3 marshal/unmarshal works.
func TestYAMLRoundtrip(t *testing.T) {
	type Config struct {
		Listen  string `yaml:"listen"`
		Backend string `yaml:"backend"`
	}
	in := Config{Listen: ":6432", Backend: "localhost:5432"}
	b, err := yaml.Marshal(&in)
	require.NoError(t, err)

	var out Config
	require.NoError(t, yaml.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

// TestTestify makes testify a direct dependency (already used above too).
func TestTestify(t *testing.T) {
	require.True(t, bytes.Equal([]byte("a"), []byte("a")))
}

// TestPrometheus ensures client_golang is usable.
func TestPrometheus(t *testing.T) {
	c := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pgrouter_depcheck_test_counter",
		Help: "dep check counter",
	})
	c.Inc()
}
