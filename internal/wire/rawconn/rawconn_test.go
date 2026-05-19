package rawconn

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

// makeMsg builds a raw Postgres frontend message (tag + 4-byte len + body).
func makeMsg(tag byte, body []byte) []byte {
	msg := make([]byte, 5+len(body))
	msg[0] = tag
	binary.BigEndian.PutUint32(msg[1:5], uint32(4+len(body)))
	copy(msg[5:], body)
	return msg
}

// makeQueryMsg builds a raw Query message.
func makeQueryMsg(sql string) []byte {
	body := append([]byte(sql), 0) // null-terminated
	return makeMsg(TagQuery, body)
}

// makeParseMsg builds a raw Parse message.
func makeParseMsg(name, query string) []byte {
	var body []byte
	body = append(body, []byte(name)...)
	body = append(body, 0) // null after name
	body = append(body, []byte(query)...)
	body = append(body, 0) // null after query
	// 2-byte num_param_oids = 0
	body = append(body, 0, 0)
	return makeMsg(TagParse, body)
}

// makeExecuteMsg builds a raw Execute message (no body = 5 bytes).
func makeExecuteMsg() []byte {
	return makeMsg(TagExecute, nil)
}

// makeSyncMsg builds a raw Sync message (no body = 5 bytes).
func makeSyncMsg() []byte {
	return makeMsg(TagSync, nil)
}

func TestExtractQuerySQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"simple", "SELECT 1"},
		{"with spaces", "SELECT * FROM users WHERE id = 1"},
		{"empty", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := makeQueryMsg(tt.sql)
			got := ExtractQuerySQL(msg)
			if got != tt.sql {
				t.Errorf("ExtractQuerySQL() = %q, want %q", got, tt.sql)
			}
		})
	}
}

func TestExtractParseFields(t *testing.T) {
	tests := []struct {
		name     string
		stmtName string
		query    string
	}{
		{"named", "stmt1", "SELECT 1"},
		{"unnamed", "", "SELECT 1"},
		{"empty_name", "", "SELECT 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := makeParseMsg(tt.stmtName, tt.query)
			gotName, gotQuery := ExtractParseFields(msg)
			if gotName != tt.stmtName {
				t.Errorf("name = %q, want %q", gotName, tt.stmtName)
			}
			if gotQuery != tt.query {
				t.Errorf("query = %q, want %q", gotQuery, tt.query)
			}
		})
	}
}

func TestExtractParseParamOIDs(t *testing.T) {
	// Build a Parse with 2 param OIDs: 23 (int4) and 25 (text).
	var body []byte
	body = append(body, "stmt"...)
	body = append(body, 0)
	body = append(body, "SELECT $1"...)
	body = append(body, 0)
	// 2 param OIDs
	body = append(body, 0, 2) // num = 2
	body = append(body, 0, 0, 0, 23) // int4
	body = append(body, 0, 0, 0, 25) // text
	msg := makeMsg(TagParse, body)

	oids := ExtractParseParamOIDs(msg)
	if len(oids) != 2 {
		t.Fatalf("got %d OIDs, want 2", len(oids))
	}
	if oids[0] != 23 || oids[1] != 25 {
		t.Errorf("oids = %v, want [23, 25]", oids)
	}
}

func TestRawConnReadMessage(t *testing.T) {
	// Build a sequence of messages.
	var all []byte
	all = append(all, makeQueryMsg("SELECT 1")...)
	all = append(all, makeExecuteMsg()...)
	all = append(all, makeSyncMsg()...)

	conn := &dummyConn{r: bytes.NewReader(all)}
	rc := New(conn)

	// Read Query
	tag, raw, err := rc.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if tag != TagQuery {
		t.Errorf("tag = %c, want Q", tag)
	}
	sql := ExtractQuerySQL(raw)
	if sql != "SELECT 1" {
		t.Errorf("sql = %q, want %q", sql, "SELECT 1")
	}

	// Read Execute
	tag, raw, err = rc.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if tag != TagExecute {
		t.Errorf("tag = %c, want E", tag)
	}
	if len(raw) != 5 {
		t.Errorf("len(raw) = %d, want 5", len(raw))
	}

	// Read Sync
	tag, raw, err = rc.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if tag != TagSync {
		t.Errorf("tag = %c, want H", tag)
	}

	// EOF
	_, _, err = rc.ReadMessage()
	if err != io.EOF {
		t.Errorf("err = %v, want EOF", err)
	}
}

func TestIsBoring(t *testing.T) {
	if !IsBoring(TagExecute) {
		t.Error("Execute should be boring")
	}
	if !IsBoring(TagSync) {
		t.Error("Sync should be boring")
	}
	if !IsBoring(TagFlush) {
		t.Error("Flush should be boring")
	}
	if IsBoring(TagQuery) {
		t.Error("Query should not be boring")
	}
	if IsBoring(TagParse) {
		t.Error("Parse should not be boring")
	}
}

func TestIsDrainTrigger(t *testing.T) {
	if !IsDrainTrigger(TagQuery) {
		t.Error("Query should trigger drain")
	}
	if !IsDrainTrigger(TagSync) {
		t.Error("Sync should trigger drain")
	}
	if IsDrainTrigger(TagBind) {
		t.Error("Bind should not trigger drain")
	}
	if IsDrainTrigger(TagExecute) {
		t.Error("Execute should not trigger drain")
	}
}

// dummyConn wraps a reader as a net.Conn (write side is discarded).
type dummyConn struct {
	r io.Reader
}

func (d *dummyConn) Read(p []byte) (int, error)          { return d.r.Read(p) }
func (d *dummyConn) Write(p []byte) (int, error)         { return len(p), nil }
func (d *dummyConn) Close() error                        { return nil }
func (d *dummyConn) LocalAddr() net.Addr                 { return nil }
func (d *dummyConn) RemoteAddr() net.Addr                { return nil }
func (d *dummyConn) SetDeadline(_ time.Time) error       { return nil }
func (d *dummyConn) SetReadDeadline(_ time.Time) error   { return nil }
func (d *dummyConn) SetWriteDeadline(_ time.Time) error  { return nil }
