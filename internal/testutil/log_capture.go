package testutil

import (
	"bytes"
	"log/slog"
	"sync"
)

// CaptureLog returns a *slog.Logger whose Debug+ output is written to
// buf as text. Use for substring assertions on log output.
func CaptureLog(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// SyncBuffer is a goroutine-safe bytes.Buffer wrapper for use as a
// slog handler sink when the logger writes from one goroutine and
// assertions poll from another. Bare bytes.Buffer triggers -race.
type SyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *SyncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}
func (s *SyncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}
func (s *SyncBuffer) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]byte, s.buf.Len())
	copy(out, s.buf.Bytes())
	return out
}
