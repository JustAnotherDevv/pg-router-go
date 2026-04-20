// Unix domain socket listener.
//
// Listens on `${unix_socket_dir}/.s.PGSQL.${listen_port}` — the same
// path PgBouncer + libpq use, so applications configured for libpq's
// `host=/path/to/dir port=6432` find pgrouter without changes.
//
// PgBouncer convention: socket file has mode 0777 (or whatever the
// admin specifies); the parent directory's permissions actually gate
// access. We honour `unix_socket_mode` from config (default 0777).
//
// On Windows AF_UNIX exists (Win10+ / Go 1.15+) but peer-cred lookup
// does not — so listening still works, but ServerAuthOptions of type
// "peer" will deny the conn since there's no cred to compare against.

package listener

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
)

// NewUnix creates a Unix domain socket listener at
// `${dir}/.s.PGSQL.${port}`.
//
// `mode` is the octal file mode applied to the socket inode (e.g.
// "0777"). Empty string defaults to 0777 (PgBouncer-compat).
//
// The socket file is removed if it already exists — a previous unclean
// shutdown leaves a stale inode that would otherwise cause EADDRINUSE.
func NewUnix(dir string, port int, mode string, log *slog.Logger) (*Listener, error) {
	if log == nil {
		log = slog.Default()
	}
	if dir == "" {
		return nil, fmt.Errorf("unix_socket_dir is empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	path := filepath.Join(dir, ".s.PGSQL."+strconv.Itoa(port))
	// Best-effort: drop a stale socket inode.
	_ = os.Remove(path)

	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen unix %s: %w", path, err)
	}
	// Apply mode. Failing chmod isn't fatal — the socket still works.
	parsedMode := uint32(0o777)
	if mode != "" {
		if m, err := strconv.ParseUint(mode, 8, 32); err == nil {
			parsedMode = uint32(m)
		} else {
			log.Warn("invalid unix_socket_mode; using 0777",
				"value", mode, "err", err)
		}
	}
	if err := os.Chmod(path, os.FileMode(parsedMode)); err != nil {
		log.Warn("chmod unix socket failed", "path", path, "err", err)
	}
	return &Listener{
		addr: path,
		ln:   ln,
		log:  log,
	}, nil
}
