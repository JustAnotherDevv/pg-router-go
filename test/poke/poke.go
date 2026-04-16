// Command poke is a tiny live-verification client for pgrouter.
// It opens TCP, sends a SSLRequest (expects 'N'), then a StartupMessage,
// then closes. Useful for P.2.2 verification without psql.
//
// Usage: go run ./test/poke <addr>
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
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	c, err := net.Dial("tcp", addr)
	if err != nil {
		die("dial: %v", err)
	}
	defer c.Close()

	// 1. SSLRequest.
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], 8)
	binary.BigEndian.PutUint32(buf[4:8], 80877103)
	if _, err := c.Write(buf); err != nil {
		die("write ssl: %v", err)
	}
	// Expect 'N'.
	resp := make([]byte, 1)
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(c, resp); err != nil {
		die("read ssl resp: %v", err)
	}
	fmt.Printf("ssl response: %q\n", resp[0])

	// 2. StartupMessage.
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":             "alice",
			"database":         "appdb",
			"application_name": "poke",
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
	fmt.Println("startup sent")

	// In PoC P.2.2: pgrouter logs + closes. Read until EOF.
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	out := make([]byte, 256)
	n, _ := c.Read(out)
	if n > 0 {
		fmt.Printf("server bytes after startup: %x\n", out[:n])
	} else {
		fmt.Println("no further bytes (expected for PoC P.2.2)")
	}
}

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}
