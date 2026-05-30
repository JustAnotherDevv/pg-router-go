//go:build linux

package auth

import (
	"fmt"
	"net"
	"strconv"
	"syscall"
)

// peerUID extracts the OS uid of the peer of uc via SO_PEERCRED.
func peerUID(uc *net.UnixConn) (string, error) {
	raw, err := uc.SyscallConn()
	if err != nil {
		return "", fmt.Errorf("syscall conn: %w", err)
	}
	var ucred *syscall.Ucred
	var sysErr error
	err = raw.Control(func(fd uintptr) {
		ucred, sysErr = syscall.GetsockoptUcred(int(fd),
			syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	})
	if err != nil {
		return "", fmt.Errorf("control: %w", err)
	}
	if sysErr != nil {
		return "", fmt.Errorf("getsockopt SO_PEERCRED: %w", sysErr)
	}
	if ucred == nil {
		return "", fmt.Errorf("ucred nil")
	}
	return strconv.FormatUint(uint64(ucred.Uid), 10), nil
}
