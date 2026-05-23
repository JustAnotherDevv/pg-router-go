package util

import "sync"

// BufferPool is a reusable []byte pool with configurable default capacity.
// Oversized buffers (>64 KiB) are dropped on Put to avoid pinning large
// CopyData or result-set buffers.
type BufferPool struct {
	pool sync.Pool
}

// NewBufferPool creates a pool whose default buffer has the given capacity.
func NewBufferPool(defaultCap int) *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, defaultCap)
				return &b
			},
		},
	}
}

// Get returns a buffer with at least minCap capacity, reset to len=0.
func (p *BufferPool) Get(minCap int) *[]byte {
	bp := p.pool.Get().(*[]byte)
	if cap(*bp) < minCap {
		nb := make([]byte, minCap)
		return &nb
	}
	*bp = (*bp)[:0]
	return bp
}

// GetSized returns a buffer with len=minSize (pre-sized for direct writes).
func (p *BufferPool) GetSized(minSize int) *[]byte {
	bp := p.pool.Get().(*[]byte)
	if cap(*bp) < minSize {
		nb := make([]byte, minSize)
		return &nb
	}
	*bp = (*bp)[:minSize]
	return bp
}

// Put returns a buffer to the pool. Nil or oversized (>64 KiB) buffers
// are silently dropped.
func (p *BufferPool) Put(bp *[]byte) {
	if bp == nil {
		return
	}
	if cap(*bp) > 64*1024 {
		return
	}
	*bp = (*bp)[:0]
	p.pool.Put(bp)
}
