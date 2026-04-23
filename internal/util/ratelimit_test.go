package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTokenBucketBurstThenRefill(t *testing.T) {
	b := NewTokenBucket(3, 10) // capacity 3, refill 10/s
	require.True(t, b.Take())
	require.True(t, b.Take())
	require.True(t, b.Take())
	require.False(t, b.Take(), "bucket should be empty after burst")
	time.Sleep(120 * time.Millisecond) // ~1.2 tokens refilled
	require.True(t, b.Take())
	require.False(t, b.Take(), "second take should be denied — only ~0.2 left")
}

func TestTokenBucketCappedAtCapacity(t *testing.T) {
	b := NewTokenBucket(2, 100)
	time.Sleep(50 * time.Millisecond) // 5 tokens would refill if uncapped
	require.True(t, b.Take())
	require.True(t, b.Take())
	require.False(t, b.Take(), "should not exceed capacity=2")
}

func TestTokenBucketAvailable(t *testing.T) {
	b := NewTokenBucket(10, 1)
	require.Equal(t, 10.0, b.Available())
	require.True(t, b.Take())
	avail := b.Available()
	require.LessOrEqual(t, avail, 10.0)
	require.GreaterOrEqual(t, avail, 8.9)
}

func TestTokenBucketTakeN(t *testing.T) {
	b := NewTokenBucket(10, 1)
	require.True(t, b.TakeN(5))
	require.True(t, b.TakeN(5))
	require.False(t, b.TakeN(5), "all 10 consumed")
}
