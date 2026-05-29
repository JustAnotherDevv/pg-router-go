package proto

import "sync"

// initialBufCap is the initial allocation size for a pooled buffer.
// Tuned for typical pgwire messages: Query strings of 1-4 KiB are common;
// DataRow / ParameterStatus often <512 B. 4 KiB hits the common case in
// one allocation while still being page-sized.
const initialBufCap = 4 * 1024

// bufPool recycles []byte slices used for encoding/forwarding messages.
// All callers MUST PutBuf when they're done — leaking these adds GC
// pressure but is not a correctness bug.
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, initialBufCap)
		return &b
	},
}

// GetBuf returns a recycled empty slice with at least initialBufCap
// capacity. Use it for short-lived encoding.
func GetBuf() *[]byte {
	bp := bufPool.Get().(*[]byte)
	*bp = (*bp)[:0]
	return bp
}

// PutBuf returns a buffer to the pool. Pass the SAME pointer GetBuf
// returned. We cap retained buffers at 64 KiB to avoid pinning huge
// CopyData payloads in the pool indefinitely.
func PutBuf(bp *[]byte) {
	if bp == nil {
		return
	}
	if cap(*bp) > 64*1024 {
		return // drop oversized buffers; let GC reclaim
	}
	*bp = (*bp)[:0]
	bufPool.Put(bp)
}
