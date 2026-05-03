package cancel

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// SweepUnbound drops Allocate()'d entries that were never Bind()'d and
// are older than ttl. Covers the panic-leak path where servePooled
// dies between Allocate and Bind without the deferred Release firing.
func TestTrackerSweepUnboundDropsOrphans(t *testing.T) {
	tr := NewTracker()
	now := time.Unix(1_700_000_000, 0)
	tr.nowFn = func() time.Time { return now }

	// 3 unbound (orphan) + 1 bound.
	orphan1, err := tr.Allocate()
	require.NoError(t, err)
	orphan2, err := tr.Allocate()
	require.NoError(t, err)
	_, err = tr.Allocate()
	require.NoError(t, err)

	bound, err := tr.Allocate()
	require.NoError(t, err)
	tr.Bind(bound, Target{BackendAddr: "10.0.0.1:5432", BackendProcessID: 42})

	require.Equal(t, 4, tr.Len())

	// Advance time past ttl. Sweep should drop the 3 unbound, keep
	// the bound one (lifetime is Release()'s job).
	now = now.Add(2 * time.Hour)
	dropped := tr.SweepUnbound(time.Hour)
	require.Equal(t, 3, dropped)
	require.Equal(t, 1, tr.Len())

	// Bound entry still resolvable.
	_, err = tr.Lookup(bound)
	require.NoError(t, err)

	// Orphans gone.
	_, err = tr.Lookup(orphan1)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = tr.Lookup(orphan2)
	require.ErrorIs(t, err, ErrNotFound)
}

// SweepUnbound is a no-op when ttl <= 0; guards against accidental
// mass-drop if a config goof feeds 0.
func TestTrackerSweepZeroTTLNoOp(t *testing.T) {
	tr := NewTracker()
	_, err := tr.Allocate()
	require.NoError(t, err)
	require.Equal(t, 0, tr.SweepUnbound(0))
	require.Equal(t, 1, tr.Len())
}

func TestTrackerAllocateUnique(t *testing.T) {
	tr := NewTracker()
	keys := map[Key]bool{}
	for i := 0; i < 100; i++ {
		k, err := tr.Allocate()
		require.NoError(t, err)
		require.False(t, keys[k], "duplicate Key allocated: %v", k)
		keys[k] = true
		require.NotZero(t, k.ProcessID)
	}
	require.Equal(t, 100, tr.Len())
}

func TestTrackerLookupUnboundFails(t *testing.T) {
	tr := NewTracker()
	k, err := tr.Allocate()
	require.NoError(t, err)
	_, err = tr.Lookup(k)
	require.ErrorIs(t, err, ErrNotFound, "allocated but not bound = not found")
}

func TestTrackerBindLookup(t *testing.T) {
	tr := NewTracker()
	k, err := tr.Allocate()
	require.NoError(t, err)
	target := Target{
		BackendAddr:      "10.0.0.1:5432",
		BackendProcessID: 12345,
		BackendSecretKey: []byte{0xde, 0xad, 0xbe, 0xef},
	}
	tr.Bind(k, target)
	got, err := tr.Lookup(k)
	require.NoError(t, err)
	require.Equal(t, target, got)
}

func TestTrackerRebind(t *testing.T) {
	tr := NewTracker()
	k, _ := tr.Allocate()
	tr.Bind(k, Target{BackendAddr: "a:5432", BackendProcessID: 1})
	tr.Bind(k, Target{BackendAddr: "b:5432", BackendProcessID: 2})
	got, _ := tr.Lookup(k)
	require.Equal(t, "b:5432", got.BackendAddr)
}

func TestTrackerRelease(t *testing.T) {
	tr := NewTracker()
	k, _ := tr.Allocate()
	tr.Bind(k, Target{BackendAddr: "a:5432", BackendProcessID: 1})
	tr.Release(k)
	_, err := tr.Lookup(k)
	require.ErrorIs(t, err, ErrNotFound)
	tr.Release(k) // double-release is a noop
}

func TestTrackerConcurrent(t *testing.T) {
	tr := NewTracker()
	var wg sync.WaitGroup
	wg.Add(50)
	for i := 0; i < 50; i++ {
		i := i
		go func() {
			defer wg.Done()
			k, err := tr.Allocate()
			require.NoError(t, err)
			tr.Bind(k, Target{
				BackendAddr:      "127.0.0.1:5432",
				BackendProcessID: uint32(i + 1),
				BackendSecretKey: []byte{0, 0, 0, byte(i)},
			})
			_, err = tr.Lookup(k)
			require.NoError(t, err)
			tr.Release(k)
		}()
	}
	wg.Wait()
	require.Equal(t, 0, tr.Len())
}

func TestForwardCancelWritesCorrectPacket(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	recvCh := make(chan []byte, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 16)
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := io.ReadFull(c, buf); err == nil {
			recvCh <- buf
		}
	}()

	target := Target{
		BackendAddr:      ln.Addr().String(),
		BackendProcessID: 0xA1B2C3D4,
		BackendSecretKey: []byte{0x11, 0x22, 0x33, 0x44},
	}
	require.NoError(t, ForwardCancel(context.Background(), target, time.Second))

	select {
	case buf := <-recvCh:
		require.Equal(t, uint32(16), binary.BigEndian.Uint32(buf[0:4]))
		require.Equal(t, CancelMagic, binary.BigEndian.Uint32(buf[4:8]))
		require.Equal(t, uint32(0xA1B2C3D4), binary.BigEndian.Uint32(buf[8:12]))
		require.Equal(t, []byte{0x11, 0x22, 0x33, 0x44}, buf[12:16])
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive cancel packet")
	}
}

func TestForwardCancelMissingAddr(t *testing.T) {
	err := ForwardCancel(context.Background(), Target{}, time.Second)
	require.Error(t, err)
}

func TestForwardCancelDialError(t *testing.T) {
	// Connect to a closed port — should error fast.
	err := ForwardCancel(context.Background(),
		Target{BackendAddr: "127.0.0.1:1"}, 200*time.Millisecond)
	require.Error(t, err)
}
