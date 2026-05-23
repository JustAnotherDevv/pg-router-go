//go:build !linux

package listener

import (
	"fmt"
	"log/slog"
)

func NewReuseport(addr string, log *slog.Logger) (*Listener, error) {
	return nil, fmt.Errorf("SO_REUSEPORT only supported on Linux")
}

func NewFromFD(addr string, fd int, log *slog.Logger) (*Listener, error) {
	return nil, fmt.Errorf("NewFromFD only supported on Linux")
}

func (l *Listener) Fd() (int, error) {
	return -1, fmt.Errorf("Fd only supported on Linux")
}
