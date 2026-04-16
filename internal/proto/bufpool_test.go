package proto

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufPoolReuse(t *testing.T) {
	bp := GetBuf()
	require.GreaterOrEqual(t, cap(*bp), initialBufCap)
	require.Equal(t, 0, len(*bp))

	*bp = append(*bp, 'a', 'b', 'c')
	require.Equal(t, 3, len(*bp))

	PutBuf(bp)

	// Next Get returns a buffer with len=0 (we reset on Get).
	bp2 := GetBuf()
	require.Equal(t, 0, len(*bp2))
	PutBuf(bp2)
}

func TestBufPoolDropsOversized(t *testing.T) {
	huge := make([]byte, 0, 128*1024) // > 64 KiB cap
	PutBuf(&huge)
	// The next GetBuf should not necessarily return the huge buffer
	// (we can't easily test "dropped" deterministically with sync.Pool,
	// but at minimum: PutBuf must not panic and the buffer length is
	// reset on the way out).
	bp := GetBuf()
	require.Equal(t, 0, len(*bp))
	PutBuf(bp)
}

func TestBufPoolNilSafe(t *testing.T) {
	require.NotPanics(t, func() { PutBuf(nil) })
}

func TestBufPoolConcurrent(t *testing.T) {
	// Smoke-test concurrent access: under -race this catches data races
	// even though sync.Pool is documented safe.
	const goroutines = 32
	const iterations = 500
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				bp := GetBuf()
				*bp = append(*bp, byte(j))
				PutBuf(bp)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkBufPool(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bp := GetBuf()
		*bp = append(*bp, 'x', 'y', 'z')
		PutBuf(bp)
	}
}
