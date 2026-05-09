package proto

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// pipePair returns two coupled net.Conn ends; reading from one returns
// what was written to the other.
func pipePair(t *testing.T) (a, b net.Conn) {
	t.Helper()
	a, b = net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return
}

// roundTripFrontend writes a FrontendMessage from a pgproto3.Frontend
// (in a goroutine) and reads it through our ClientSide.
func roundTripFrontend(t *testing.T, msg pgproto3.FrontendMessage) pgproto3.FrontendMessage {
	t.Helper()
	clt, srv := pipePair(t)
	cs := NewClientSide(srv)
	fe := pgproto3.NewFrontend(clt, clt)

	done := make(chan error, 1)
	go func() {
		fe.Send(msg)
		done <- fe.Flush()
	}()

	_ = srv.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := cs.Receive()
	require.NoError(t, err)
	require.NoError(t, <-done)
	return got
}

// roundTripBackend writes a BackendMessage from a pgproto3.Backend
// (in a goroutine) and reads it through our ServerSide.
func roundTripBackend(t *testing.T, msg pgproto3.BackendMessage) pgproto3.BackendMessage {
	t.Helper()
	clt, srv := pipePair(t)
	ss := NewServerSide(clt)
	be := pgproto3.NewBackend(srv, srv)

	done := make(chan error, 1)
	go func() {
		be.Send(msg)
		done <- be.Flush()
	}()

	_ = clt.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := ss.Receive()
	require.NoError(t, err)
	require.NoError(t, <-done)
	return got
}

// Frontend message coverage.

func TestFrontendQuery(t *testing.T) {
	got := roundTripFrontend(t, &pgproto3.Query{String: "SELECT 1"})
	q, ok := got.(*pgproto3.Query)
	require.True(t, ok)
	require.Equal(t, "SELECT 1", q.String)
}

func TestFrontendParse(t *testing.T) {
	got := roundTripFrontend(t, &pgproto3.Parse{
		Name:          "stmt1",
		Query:         "SELECT $1::int",
		ParameterOIDs: []uint32{23},
	})
	p, ok := got.(*pgproto3.Parse)
	require.True(t, ok)
	require.Equal(t, "stmt1", p.Name)
	require.Equal(t, "SELECT $1::int", p.Query)
	require.Equal(t, []uint32{23}, p.ParameterOIDs)
}

func TestFrontendBind(t *testing.T) {
	got := roundTripFrontend(t, &pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "stmt1",
		ParameterFormatCodes: []int16{0},
		Parameters:           [][]byte{[]byte("42")},
		ResultFormatCodes:    []int16{0},
	})
	b, ok := got.(*pgproto3.Bind)
	require.True(t, ok)
	require.Equal(t, "stmt1", b.PreparedStatement)
	require.Equal(t, [][]byte{[]byte("42")}, b.Parameters)
}

// TestFrontendTypeOnlyMessages collapses the per-type tests that only
// verify the message type survives the round trip (no field-level
// assertions needed beyond the type itself).
func TestFrontendTypeOnlyMessages(t *testing.T) {
	cases := []pgproto3.FrontendMessage{
		&pgproto3.Execute{Portal: "", MaxRows: 0},
		&pgproto3.Sync{},
		&pgproto3.Flush{},
		&pgproto3.CopyDone{},
	}
	for _, in := range cases {
		t.Run(fmt.Sprintf("%T", in), func(t *testing.T) {
			got := roundTripFrontend(t, in)
			require.IsType(t, in, got)
		})
	}
}

func TestFrontendDescribe(t *testing.T) {
	got := roundTripFrontend(t, &pgproto3.Describe{ObjectType: 'S', Name: "stmt1"})
	d, ok := got.(*pgproto3.Describe)
	require.True(t, ok)
	require.Equal(t, byte('S'), d.ObjectType)
}

func TestFrontendClose(t *testing.T) {
	got := roundTripFrontend(t, &pgproto3.Close{ObjectType: 'S', Name: "stmt1"})
	c, ok := got.(*pgproto3.Close)
	require.True(t, ok)
	require.Equal(t, byte('S'), c.ObjectType)
}

func TestFrontendTerminate(t *testing.T) {
	got := roundTripFrontend(t, &pgproto3.Terminate{})
	_, ok := got.(*pgproto3.Terminate)
	require.True(t, ok)
	require.True(t, IsTerminate(got))
}

func TestFrontendCopyData(t *testing.T) {
	got := roundTripFrontend(t, &pgproto3.CopyData{Data: []byte("row1\trow2\n")})
	d, ok := got.(*pgproto3.CopyData)
	require.True(t, ok)
	require.True(t, bytes.Equal([]byte("row1\trow2\n"), d.Data))
}

func TestFrontendCopyFail(t *testing.T) {
	got := roundTripFrontend(t, &pgproto3.CopyFail{Message: "oops"})
	cf, ok := got.(*pgproto3.CopyFail)
	require.True(t, ok)
	require.Equal(t, "oops", cf.Message)
}

// Backend message coverage.

// TestBackendTypeOnlyMessages collapses backend round-trips whose only
// check is that the message type survives the round trip.
func TestBackendTypeOnlyMessages(t *testing.T) {
	cases := []pgproto3.BackendMessage{
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParseComplete{},
		&pgproto3.BindComplete{},
		&pgproto3.CloseComplete{},
		&pgproto3.NoData{},
		&pgproto3.EmptyQueryResponse{},
		&pgproto3.PortalSuspended{},
		&pgproto3.CopyBothResponse{OverallFormat: 0, ColumnFormatCodes: []uint16{0}},
	}
	for _, in := range cases {
		t.Run(fmt.Sprintf("%T", in), func(t *testing.T) {
			got := roundTripBackend(t, in)
			require.IsType(t, in, got)
		})
	}
}

func TestBackendAuthenticationMD5(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.AuthenticationMD5Password{Salt: [4]byte{1, 2, 3, 4}})
	m, ok := got.(*pgproto3.AuthenticationMD5Password)
	require.True(t, ok)
	require.Equal(t, [4]byte{1, 2, 3, 4}, m.Salt)
}

func TestBackendAuthenticationSASL(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}})
	s, ok := got.(*pgproto3.AuthenticationSASL)
	require.True(t, ok)
	require.Equal(t, []string{"SCRAM-SHA-256"}, s.AuthMechanisms)
}

func TestBackendParameterStatus(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.ParameterStatus{Name: "server_version", Value: "16.4"})
	p, ok := got.(*pgproto3.ParameterStatus)
	require.True(t, ok)
	require.Equal(t, "server_version", p.Name)
	require.Equal(t, "16.4", p.Value)
}

func TestBackendBackendKeyData(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.BackendKeyData{ProcessID: 12345, SecretKey: []byte{0xde, 0xad, 0xbe, 0xef}})
	k, ok := got.(*pgproto3.BackendKeyData)
	require.True(t, ok)
	require.Equal(t, uint32(12345), k.ProcessID)
	require.Equal(t, []byte{0xde, 0xad, 0xbe, 0xef}, k.SecretKey)
}

func TestBackendReadyForQuery(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.ReadyForQuery{TxStatus: 'T'})
	r, ok := got.(*pgproto3.ReadyForQuery)
	require.True(t, ok)
	require.Equal(t, byte('T'), r.TxStatus)
	s, ok := IsReadyForQuery(got)
	require.True(t, ok)
	require.Equal(t, byte('T'), s)
}

func TestBackendRowDescription(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.RowDescription{
		Fields: []pgproto3.FieldDescription{
			{Name: []byte("id"), DataTypeOID: 23, DataTypeSize: 4, Format: 0},
		},
	})
	rd, ok := got.(*pgproto3.RowDescription)
	require.True(t, ok)
	require.Len(t, rd.Fields, 1)
	require.Equal(t, "id", string(rd.Fields[0].Name))
}

func TestBackendDataRow(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.DataRow{Values: [][]byte{[]byte("42")}})
	dr, ok := got.(*pgproto3.DataRow)
	require.True(t, ok)
	require.Equal(t, [][]byte{[]byte("42")}, dr.Values)
}

func TestBackendCommandComplete(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
	cc, ok := got.(*pgproto3.CommandComplete)
	require.True(t, ok)
	require.Equal(t, "SELECT 1", string(cc.CommandTag))
}

func TestBackendParameterDescription(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.ParameterDescription{ParameterOIDs: []uint32{23, 25}})
	pd, ok := got.(*pgproto3.ParameterDescription)
	require.True(t, ok)
	require.Equal(t, []uint32{23, 25}, pd.ParameterOIDs)
}

func TestBackendErrorResponse(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.ErrorResponse{Severity: "ERROR", Code: "42601", Message: "syntax error"})
	e, ok := got.(*pgproto3.ErrorResponse)
	require.True(t, ok)
	require.Equal(t, "ERROR", e.Severity)
	require.Equal(t, "42601", e.Code)
	require.Equal(t, "syntax error", e.Message)
}

func TestBackendNoticeResponse(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.NoticeResponse{Severity: "NOTICE", Message: "hello"})
	n, ok := got.(*pgproto3.NoticeResponse)
	require.True(t, ok)
	require.Equal(t, "NOTICE", n.Severity)
	require.Equal(t, "hello", n.Message)
}

func TestBackendNotificationResponse(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.NotificationResponse{
		PID: 99, Channel: "ch", Payload: "hi",
	})
	nr, ok := got.(*pgproto3.NotificationResponse)
	require.True(t, ok)
	require.Equal(t, uint32(99), nr.PID)
	require.Equal(t, "ch", nr.Channel)
	require.Equal(t, "hi", nr.Payload)
}

func TestBackendCopyInResponse(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.CopyInResponse{
		OverallFormat:     0,
		ColumnFormatCodes: []uint16{0, 0},
	})
	c, ok := got.(*pgproto3.CopyInResponse)
	require.True(t, ok)
	require.Equal(t, []uint16{0, 0}, c.ColumnFormatCodes)
}

func TestBackendCopyOutResponse(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.CopyOutResponse{
		OverallFormat:     1,
		ColumnFormatCodes: []uint16{1},
	})
	c, ok := got.(*pgproto3.CopyOutResponse)
	require.True(t, ok)
	require.Equal(t, uint8(1), c.OverallFormat)
}

func TestStartupMessageRoundTrip(t *testing.T) {
	clt, srv := pipePair(t)
	cs := NewClientSide(srv)

	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":     "alice",
			"database": "appdb",
		},
	}
	go func() {
		b, _ := startup.Encode(nil)
		_, _ = clt.Write(b)
	}()

	_ = srv.SetReadDeadline(time.Now().Add(2 * time.Second))
	msg, err := cs.ReceiveStartup()
	require.NoError(t, err)
	sm, ok := msg.(*pgproto3.StartupMessage)
	require.True(t, ok)
	require.Equal(t, "alice", sm.Parameters["user"])
	require.Equal(t, "appdb", sm.Parameters["database"])
}
