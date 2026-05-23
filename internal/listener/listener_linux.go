//go:build linux

package listener

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"syscall"
)

// NewReuseport creates a Listener with SO_REUSEPORT, allowing multiple
// processes to bind to the same port. Requires Linux 3.9+.
// Uses raw syscalls because net.ListenConfig.Control doesn't reliably
// set SO_REUSEPORT before bind in all Go versions.
func NewReuseport(addr string, log *slog.Logger) (*Listener, error) {
	if log == nil {
		log = slog.Default()
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("split addr: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("parse port: %w", err)
	}
	var ip net.IP
	if host == "" || host == "0.0.0.0" {
		ip = net.IPv4zero
	} else {
		ip = net.ParseIP(host)
		if ip == nil {
			return nil, fmt.Errorf("parse host: %s", host)
		}
	}

	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("socket: %w", err)
	}
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	// SO_REUSEPORT = 15 on Linux
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 15, 1)

	sa := &syscall.SockaddrInet4{Port: port}
	copy(sa.Addr[:], ip.To4())
	if err := syscall.Bind(fd, sa); err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("bind reuseport %s: %w", addr, err)
	}
	if err := syscall.Listen(fd, 128); err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("listen reuseport %s: %w", addr, err)
	}

	file := os.NewFile(uintptr(fd), "tcp-reuseport")
	ln, err := net.FileListener(file)
	_ = file.Close()
	if err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("file listener: %w", err)
	}

	return &Listener{
		addr: addr,
		ln:   ln,
		log:  log,
	}, nil
}

// NewFromFD wraps an already-bound file descriptor into a Listener.
// Used by worker processes that inherit the parent's listening socket.
func NewFromFD(addr string, fd int, log *slog.Logger) (*Listener, error) {
	if log == nil {
		log = slog.Default()
	}
	file := os.NewFile(uintptr(fd), "inherited-listener")
	if file == nil {
		return nil, fmt.Errorf("invalid fd %d", fd)
	}
	ln, err := net.FileListener(file)
	_ = file.Close()
	if err != nil {
		return nil, fmt.Errorf("file listener fd %d: %w", fd, err)
	}
	return &Listener{
		addr: addr,
		ln:   ln,
		log:  log,
	}, nil
}

// Fd returns the underlying file descriptor of the listener.
func (l *Listener) Fd() (int, error) {
	type fileConn interface {
		File() (*os.File, error)
	}
	if f, ok := l.ln.(fileConn); ok {
		file, err := f.File()
		if err != nil {
			return -1, err
		}
		defer file.Close()
		return int(file.Fd()), nil
	}
	return -1, fmt.Errorf("cannot get fd from listener")
}
