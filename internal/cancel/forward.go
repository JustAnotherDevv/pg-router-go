package cancel

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

// CancelMagic is the int32 magic value that distinguishes a
// CancelRequest from a StartupMessage.
const CancelMagic uint32 = 80877102

// ForwardCancel opens a fresh TCP connection to `target.BackendAddr`
// and writes a CancelRequest carrying the upstream's real PID + secret.
//
// The connection is closed immediately after the write — pgwire
// servers don't reply to CancelRequest.
//
// Timeout protects against an unresponsive backend.
func ForwardCancel(ctx context.Context, target Target, timeout time.Duration) error {
	if target.BackendAddr == "" {
		return fmt.Errorf("forward cancel: missing backend addr")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	d := net.Dialer{Timeout: timeout}
	c, err := d.DialContext(ctx, "tcp", target.BackendAddr)
	if err != nil {
		return fmt.Errorf("forward cancel: dial: %w", err)
	}
	defer c.Close()
	_ = c.SetWriteDeadline(time.Now().Add(timeout))

	// Layout (16 bytes total):
	//   int32 length=16
	//   int32 magic=80877102
	//   int32 ProcessID
	//   int32 SecretKey   (we always emit the classic 4-byte form)
	var buf [16]byte
	binary.BigEndian.PutUint32(buf[0:4], 16)
	binary.BigEndian.PutUint32(buf[4:8], CancelMagic)
	binary.BigEndian.PutUint32(buf[8:12], target.BackendProcessID)
	if len(target.BackendSecretKey) >= 4 {
		copy(buf[12:16], target.BackendSecretKey[:4])
	}
	if _, err := c.Write(buf[:]); err != nil {
		return fmt.Errorf("forward cancel: write: %w", err)
	}
	return nil
}
