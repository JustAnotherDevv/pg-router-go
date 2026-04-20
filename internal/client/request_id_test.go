package client

import (
	"regexp"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRequestIDFormat(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{12}$`)
	for i := 0; i < 32; i++ {
		id := newRequestID()
		require.Truef(t, re.MatchString(id), "got %q", id)
	}
}

func TestNewRequestIDUniqueUnderConcurrency(t *testing.T) {
	const N = 2000
	var wg sync.WaitGroup
	results := make(chan string, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- newRequestID()
		}()
	}
	wg.Wait()
	close(results)
	seen := make(map[string]struct{}, N)
	for id := range results {
		_, dup := seen[id]
		require.Falsef(t, dup, "duplicate request id %q", id)
		seen[id] = struct{}{}
	}
}
