package util

import (
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountingConnReportsBytes(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	var in, out int
	cc := NewCountingConn(a,
		func(n int) { in += n },
		func(n int) { out += n },
	)

	go func() {
		_, _ = b.Write([]byte("hello"))
		buf := make([]byte, 32)
		_, _ = io.ReadAtLeast(b, buf, 4)
	}()

	got := make([]byte, 5)
	_, err := io.ReadFull(cc, got)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), got)
	require.Equal(t, 5, in)

	_, err = cc.Write([]byte("ABCD"))
	require.NoError(t, err)
	require.Equal(t, 4, out)
}

func TestCountingConnNilCallbacks(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	cc := NewCountingConn(a, nil, nil)
	go func() { _, _ = b.Write([]byte("x")) }()
	buf := make([]byte, 1)
	_, err := io.ReadFull(cc, buf)
	require.NoError(t, err)
}
