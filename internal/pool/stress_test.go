package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
)

// TestStressAcquireReleaseConcurrent fires many goroutines acquiring +
// releasing in tight loops. Under -race the test catches data races in
// the wait-queue / idle-stack / counter paths.
func TestStressAcquireReleaseConcurrent(t *testing.T) {
	const (
		workers    = 32
		iterations = 200
		poolSize   = 4
	)

	dialed := atomic.Int64{}
	dial := countingDial(&dialed)
	p := New("stress", dial, Config{
		DefaultPoolSize: poolSize,
		QueryWait:       2 * time.Second,
		Log:             testutil.Discard,
	})
	defer p.Close()

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < iterations; i++ {
				c, err := p.Acquire(ctx)
				if err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				// Yield occasionally to interleave.
				if i%17 == 0 {
					time.Sleep(time.Microsecond)
				}
				p.Release(c, false)
			}
		}()
	}
	wg.Wait()

	st := p.Stats()
	if int64(st.TotalAcquired) != int64(workers*iterations) {
		t.Errorf("expected %d acquires, got %d", workers*iterations, st.TotalAcquired)
	}
	if int(dialed.Load()) > poolSize {
		t.Errorf("expected at most %d dials, got %d", poolSize, dialed.Load())
	}
	if st.Active != 0 {
		t.Errorf("residual active: %d", st.Active)
	}
}

// TestStressCancelStorm fires many Acquire goroutines whose contexts get
// cancelled before slots are available. Verifies cancelWaiter cleans up
// properly under load.
func TestStressCancelStorm(t *testing.T) {
	const workers = 100

	p := New("cancel-storm", okDial, Config{
		DefaultPoolSize: 1,
		QueryWait:       time.Hour,
		Log:             testutil.Discard,
	})
	defer p.Close()

	// Saturate.
	holder, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Many waiters with short-lived ctx.
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
			defer cancel()
			_, _ = p.Acquire(ctx)
		}()
	}
	wg.Wait()

	// All waiters should be gone.
	if p.Stats().Waiters != 0 {
		t.Errorf("waiters not cleaned up: %d", p.Stats().Waiters)
	}

	// Pool is still usable.
	p.Release(holder, false)
	c, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire after storm: %v", err)
	}
	p.Release(c, false)
}
