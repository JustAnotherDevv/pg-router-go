package proto

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// FuzzClientSideReceive feeds arbitrary bytes into the client-side
// reader and asserts that it never panics, regardless of how malformed
// the input is. Errors are fine; panics are the bug we're hunting.
//
// Run: `go test -fuzz=FuzzClientSideReceive ./internal/proto -fuzztime=60s`
func FuzzClientSideReceive(f *testing.F) {
	// Seed corpus: real well-formed messages.
	seedReal := func(msg pgproto3.FrontendMessage) []byte {
		buf, _ := msg.Encode(nil)
		return buf
	}
	f.Add(seedReal(&pgproto3.Query{String: "SELECT 1"}))
	f.Add(seedReal(&pgproto3.Parse{Name: "s1", Query: "SELECT $1::int"}))
	f.Add(seedReal(&pgproto3.Bind{PreparedStatement: "s1"}))
	f.Add(seedReal(&pgproto3.Execute{}))
	f.Add(seedReal(&pgproto3.Sync{}))
	f.Add(seedReal(&pgproto3.Terminate{}))
	f.Add(seedReal(&pgproto3.CopyData{Data: []byte{0, 1, 2, 3}}))
	// Also feed pure garbage.
	f.Add([]byte{0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{'Q', 0, 0, 0, 0})       // length 0 — truncated
	f.Add([]byte{'P', 0xff, 0xff, 0xff, 0xff}) // huge length

	f.Fuzz(func(_ *testing.T, data []byte) {
		// Wrap data in an io.Reader and feed via ClientSide.Receive.
		// We don't care about the output, only that it never panics.
		buf := bytes.NewBuffer(data)
		// Pad with a closer so Receive sees EOF quickly.
		nopRW := &readWriter{r: buf, w: &bytes.Buffer{}}
		cs := NewClientSide(nopRW)
		_, _ = cs.Receive()
	})
}

// FuzzServerSideReceive same idea for the server-facing reader.
func FuzzServerSideReceive(f *testing.F) {
	seedReal := func(msg pgproto3.BackendMessage) []byte {
		buf, _ := msg.Encode(nil)
		return buf
	}
	f.Add(seedReal(&pgproto3.AuthenticationOk{}))
	f.Add(seedReal(&pgproto3.ReadyForQuery{TxStatus: 'I'}))
	f.Add(seedReal(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42601"}))
	f.Add(seedReal(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("c")}}}))
	f.Add(seedReal(&pgproto3.DataRow{Values: [][]byte{[]byte("x")}}))
	f.Add(seedReal(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}))
	f.Add([]byte{})
	f.Add([]byte{'Z'})
	f.Add([]byte{'Z', 0, 0, 0, 5, 'X'})

	f.Fuzz(func(_ *testing.T, data []byte) {
		buf := bytes.NewBuffer(data)
		nopRW := &readWriter{r: buf, w: &bytes.Buffer{}}
		ss := NewServerSide(nopRW)
		_, _ = ss.Receive()
	})
}

// FuzzStartupReceive checks the startup decoder path.
func FuzzStartupReceive(f *testing.F) {
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "u", "database": "d"},
	}
	b, _ := startup.Encode(nil)
	f.Add(b)
	// SSLRequest.
	ssl := make([]byte, 8)
	binary.BigEndian.PutUint32(ssl[0:4], 8)
	binary.BigEndian.PutUint32(ssl[4:8], 80877103)
	f.Add(ssl)
	// CancelRequest.
	cr := make([]byte, 16)
	binary.BigEndian.PutUint32(cr[0:4], 16)
	binary.BigEndian.PutUint32(cr[4:8], 80877102)
	binary.BigEndian.PutUint32(cr[8:12], 12345)
	binary.BigEndian.PutUint32(cr[12:16], 0xdeadbeef)
	f.Add(cr)
	// Garbage.
	f.Add([]byte{0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(_ *testing.T, data []byte) {
		buf := bytes.NewBuffer(data)
		nopRW := &readWriter{r: buf, w: &bytes.Buffer{}}
		cs := NewClientSide(nopRW)
		_, _ = cs.ReceiveStartup()
	})
}

// readWriter adapts a Reader + Writer to io.ReadWriter so pgproto3 can
// use one stream end.
type readWriter struct {
	r *bytes.Buffer
	w *bytes.Buffer
}

func (rw *readWriter) Read(p []byte) (int, error)  { return rw.r.Read(p) }
func (rw *readWriter) Write(p []byte) (int, error) { return rw.w.Write(p) }
