// Command handshake performs a full Postgres startup handshake against
// pgrouter and prints what comes back. Useful for P.2.3 live verification.
//
// Usage: go run ./test/handshake <addr>
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func main() {
	addr := ":6432"
	user := "alice"
	db := "appdb"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	if len(os.Args) > 2 {
		user = os.Args[2]
	}
	if len(os.Args) > 3 {
		db = os.Args[3]
	}
	c, err := net.Dial("tcp", addr)
	if err != nil {
		die("dial: %v", err)
	}
	defer c.Close()

	// 1. SSLRequest (will be declined with 'N').
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], 8)
	binary.BigEndian.PutUint32(buf[4:8], 80877103)
	if _, err := c.Write(buf); err != nil {
		die("write ssl: %v", err)
	}
	resp := make([]byte, 1)
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(c, resp); err != nil {
		die("read ssl resp: %v", err)
	}
	fmt.Printf("[ssl decline] = %q\n", resp[0])

	// 2. StartupMessage.
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":             user,
			"database":         db,
			"application_name": "handshake",
			"client_encoding":  "UTF8",
		},
	}
	enc, err := startup.Encode(nil)
	if err != nil {
		die("encode startup: %v", err)
	}
	if _, err := c.Write(enc); err != nil {
		die("write startup: %v", err)
	}

	// 3. Read response messages until ReadyForQuery.
	fe := pgproto3.NewFrontend(c, c)
	for {
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		m, err := fe.Receive()
		if err != nil {
			die("receive: %v", err)
		}
		switch t := m.(type) {
		case *pgproto3.AuthenticationOk:
			fmt.Println("[AuthenticationOk]")
		case *pgproto3.ParameterStatus:
			fmt.Printf("[ParameterStatus] %s = %q\n", t.Name, t.Value)
		case *pgproto3.BackendKeyData:
			fmt.Printf("[BackendKeyData] pid=%d secret_hex=%x\n", t.ProcessID, t.SecretKey)
		case *pgproto3.ReadyForQuery:
			fmt.Printf("[ReadyForQuery] tx_status=%q\n", t.TxStatus)
			fmt.Println("HANDSHAKE COMPLETE")
			return
		default:
			fmt.Printf("[%T] %+v\n", t, t)
		}
	}
}

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}
