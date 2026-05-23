package splice

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// msg builds a synthetic Postgres backend message: tag + 4-byte
// length (length includes itself) + body. Body is optional.
func msg(t *testing.T, tag byte, body []byte) []byte {
	t.Helper()
	length := 4 + len(body)
	if length < 4 {
		t.Fatalf("invalid length %d", length)
	}
	out := make([]byte, 1+length)
	out[0] = tag
	binary.BigEndian.PutUint32(out[1:5], uint32(length))
	copy(out[5:], body)
	return out
}

func TestClassify_AllBoring(t *testing.T) {
	boring := []byte{'D', 'T', 'C', '1', '2', 'n', 'I', 's'}
	for _, tag := range boring {
		if Classify(tag) != ClassBoring {
			t.Errorf("tag 0x%02x (%c): want ClassBoring, got %d", tag, tag, Classify(tag))
		}
	}
}

func TestClassify_Terminator(t *testing.T) {
	if Classify('Z') != ClassTerminator {
		t.Errorf("tag 'Z' should be ClassTerminator, got %d", Classify('Z'))
	}
}

func TestClassify_CopyIn(t *testing.T) {
	if Classify('G') != ClassCopyIn {
		t.Errorf("tag 'G' should be ClassCopyIn, got %d", Classify('G'))
	}
}

func TestClassify_Interesting(t *testing.T) {
	interesting := []byte{'S', 'K', 'A', 'N', 'H', 'W', 'v'}
	for _, tag := range interesting {
		if Classify(tag) != ClassInteresting {
			t.Errorf("tag 0x%02x (%c): want ClassInteresting, got %d", tag, tag, Classify(tag))
		}
	}
	// ErrorResponse is its own class for dispatch flexibility.
	if Classify('E') != ClassError {
		t.Errorf("ErrorResponse want ClassError, got %d", Classify('E'))
	}
}

func TestClassify_UnknownIsBoring(t *testing.T) {
	// Any unknown tag defaults to boring (we forward what we don't
	// understand — the client can decide).
	for _, tag := range []byte{0x00, '!', 'z', 0xff} {
		if Classify(tag) != ClassBoring {
			t.Errorf("tag 0x%02x: want ClassBoring (default), got %d", tag, Classify(tag))
		}
	}
}

// drainReader is a test helper that drives DrainSplice against a
// (src, dst) pair and returns the bytes written to dst along with
// any tag put back into the putback reader.
func drainReader(t *testing.T, src, dst []byte, bufsize int) (written []byte, putback []byte, err error) {
	t.Helper()
	srcR := bytes.NewReader(src)
	pb := NewPutbackReader(srcR)
	var dstBuf bytes.Buffer
	err = DrainSplice(&dstBuf, pb, bufsize)
	if errors.Is(err, ErrSpliceStop) {
		// Capture the putback bytes for inspection.
		var peek [HeaderSize]byte
		n, _ := pb.Read(peek[:])
		putback = append(putback, peek[:n]...)
	} else if err == io.EOF {
		err = nil
	}
	return dstBuf.Bytes(), putback, err
}

func TestDrainSplice_AllBoring(t *testing.T) {
	// Three DataRows + one CommandComplete.
	src := bytes.Join([][]byte{
		msg(t, 'D', []byte{0, 1, 'x'}),   // 1-col DataRow
		msg(t, 'D', []byte{0, 1, 'y'}),   // 1-col DataRow
		msg(t, 'D', []byte{0, 1, 'z'}),   // 1-col DataRow
		msg(t, 'C', []byte("SELECT 3")),  // CommandComplete
	}, nil)
	written, putback, err := drainReader(t, src, nil, 64)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(written, src) {
		t.Errorf("boring pass-through corrupted bytes:\n got %x\n want %x", written, src)
	}
	if len(putback) != 0 {
		t.Errorf("expected no putback on all-boring run, got %d bytes", len(putback))
	}
}

func TestDrainSplice_StopsAtInteresting(t *testing.T) {
	// Two boring DataRows, then an interesting ErrorResponse.
	interesting := msg(t, 'E', []byte{
		'S', // Severity field
		'V', 0, 0, 0, 4, 'F', 'A', 'T', 'A',
		'C', 0, 0, 0, 5, '5', '7', 'P', '0', '1',
		0, // terminator
	})
	src := bytes.Join([][]byte{
		msg(t, 'D', []byte{0, 1, 'a'}),
		msg(t, 'D', []byte{0, 1, 'b'}),
		interesting,
	}, nil)
	written, putback, err := drainReader(t, src, nil, 64)
	if !errors.Is(err, ErrSpliceStop) {
		t.Fatalf("want ErrSpliceStop, got %v", err)
	}
	// First 2 boring messages should be forwarded verbatim.
	wantForwarded := bytes.Join([][]byte{
		msg(t, 'D', []byte{0, 1, 'a'}),
		msg(t, 'D', []byte{0, 1, 'b'}),
	}, nil)
	if !bytes.Equal(written, wantForwarded) {
		t.Errorf("forwarded bytes mismatch:\n got %x\n want %x", written, wantForwarded)
	}
	// The 5-byte header of the interesting message must have been
	// put back so the next Receive() can decode it.
	if len(putback) != HeaderSize {
		t.Fatalf("want %d putback bytes, got %d (%x)", HeaderSize, len(putback), putback)
	}
	if putback[0] != 'E' {
		t.Errorf("putback tag = %c, want E", putback[0])
	}
	// The protocol encodes length INCLUDING the 4 length bytes, so
	// the putback length field equals 4 + bodyLen = msg total - 1
	// (no tag byte in length). The total msg = 1 (tag) + 4 (length) + body.
	body := []byte{
		'S',
		'V', 0, 0, 0, 4, 'F', 'A', 'T', 'A',
		'C', 0, 0, 0, 5, '5', '7', 'P', '0', '1',
		0,
	}
	wantLen := uint32(4 + len(body))
	gotLen := binary.BigEndian.Uint32(putback[1:5])
	if gotLen != wantLen {
		t.Errorf("putback length = %d, want %d (body=%d bytes)", gotLen, wantLen, len(body))
	}
}

func TestDrainSplice_StopsAtTerminator(t *testing.T) {
	src := bytes.Join([][]byte{
		msg(t, 'D', []byte{0, 1, 'x'}),
		msg(t, 'C', []byte("SELECT 1")),
		msg(t, 'Z', []byte{'I'}), // ReadyForQuery, idle
	}, nil)
	_, putback, err := drainReader(t, src, nil, 64)
	if !errors.Is(err, ErrSpliceStop) {
		t.Fatalf("want ErrSpliceStop at RFQ, got %v", err)
	}
	if len(putback) != HeaderSize || putback[0] != 'Z' {
		t.Errorf("expected putback of 'Z' header, got %x", putback)
	}
}

func TestDrainSplice_StopsAtCopyIn(t *testing.T) {
	src := bytes.Join([][]byte{
		msg(t, 'T', []byte("col1 col2")), // RowDescription
		msg(t, 'G', []byte{0, 0, 0, 0}),  // CopyInResponse
	}, nil)
	_, putback, err := drainReader(t, src, nil, 64)
	if !errors.Is(err, ErrSpliceStop) {
		t.Fatalf("want ErrSpliceStop at CopyIn, got %v", err)
	}
	if len(putback) != HeaderSize || putback[0] != 'G' {
		t.Errorf("expected putback of 'G' header, got %x", putback)
	}
}

func TestDrainSplice_ZeroLengthBoring(t *testing.T) {
	// EmptyQuery: 5 bytes total, body is empty.
	src := msg(t, 'I', nil)
	written, putback, err := drainReader(t, src, nil, 64)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(written, src) {
		t.Errorf("zero-length boring body lost bytes:\n got %x\n want %x", written, src)
	}
	if len(putback) != 0 {
		t.Errorf("unexpected putback: %x", putback)
	}
}

func TestDrainSplice_BodyLargerThanBuffer(t *testing.T) {
	// Body is 10 bytes; bufsize is 8 (only 3 free after header).
	// DrainSplice should fall back to two-write path.
	body := []byte{0, 8, 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'}
	src := msg(t, 'D', body)
	written, _, err := drainReader(t, src, nil, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(written, src) {
		t.Errorf("oversize body corrupted bytes:\n got %x\n want %x", written, src)
	}
}

func TestDrainSplice_MixedBoringAndLarge(t *testing.T) {
	// Small boring, large boring (>buf), small boring, RFQ.
	small1 := msg(t, 'D', []byte{0, 1, 'a'})
	big := msg(t, 'T', bytes.Repeat([]byte{'x'}, 100))
	small2 := msg(t, 'C', []byte("SELECT 1"))
	rfq := msg(t, 'Z', []byte{'I'})

	src := bytes.Join([][]byte{small1, big, small2, rfq}, nil)
	srcR := bytes.NewReader(src)
	pb := NewPutbackReader(srcR)
	var dstBuf bytes.Buffer
	err := DrainSplice(&dstBuf, pb, 16)
	if !errors.Is(err, ErrSpliceStop) {
		t.Fatalf("want ErrSpliceStop, got %v", err)
	}
	want := bytes.Join([][]byte{small1, big, small2}, nil)
	if !bytes.Equal(dstBuf.Bytes(), want) {
		t.Errorf("mixed run forwarded bytes mismatch")
	}
	// Now read the putback and verify it's the RFQ header.
	peek := make([]byte, HeaderSize)
	n, _ := pb.Read(peek)
	if n != HeaderSize || peek[0] != 'Z' {
		t.Errorf("expected RFQ putback, got %x", peek[:n])
	}
	// And the rest of the source (just the RFQ body) should still
	// be readable through the wrapped reader.
	rest, _ := io.ReadAll(pb)
	if !bytes.Equal(rest, []byte{'I'}) {
		t.Errorf("RFQ body not consumable after putback: %x", rest)
	}
}

func TestDrainSplice_EmptySrc(t *testing.T) {
	written, _, err := drainReader(t, nil, nil, 64)
	if err != nil {
		t.Errorf("empty src should return nil err (EOF swallowed), got %v", err)
	}
	if len(written) != 0 {
		t.Errorf("empty src should produce no output, got %x", written)
	}
}

func TestDrainSplice_TruncatedHeader(t *testing.T) {
	// 3 bytes (less than 5-byte header).
	src := []byte{'D', 0, 0}
	_, _, err := drainReader(t, src, nil, 64)
	if !errors.Is(err, io.ErrUnexpectedEOF) && err != io.EOF {
		t.Errorf("truncated header: want EOF/UnexpectedEOF, got %v", err)
	}
}

func TestDrainSplice_NegativeLength(t *testing.T) {
	// 5-byte header claiming length=2 (less than 4) → bodyLen = -2.
	src := []byte{'D', 0, 0, 0, 2}
	_, _, err := drainReader(t, src, nil, 64)
	if err == nil || err == ErrSpliceStop {
		t.Errorf("negative length should produce a real error, got %v", err)
	}
}

// PutbackReader tests

func TestPutbackReader_ReadAfterPutback(t *testing.T) {
	src := bytes.NewReader([]byte("hello world"))
	pb := NewPutbackReader(src)
	// Consume "hello".
	first := make([]byte, 5)
	n, _ := pb.Read(first)
	if n != 5 || string(first) != "hello" {
		t.Fatalf("first read: got %q (n=%d), want %q", first[:n], n, "hello")
	}
	// Put back "X".
	pb.Putback([]byte("X"))
	// First read returns the putback byte (1 byte).
	one := make([]byte, 1)
	n, _ = pb.Read(one)
	if n != 1 || one[0] != 'X' {
		t.Fatalf("expected putback byte X, got %q (n=%d)", one[:n], n)
	}
	// Next read drains the rest from the underlying reader.
	rest, _ := io.ReadAll(pb)
	if string(rest) != " world" {
		t.Errorf("after putback drained: got %q, want %q", rest, " world")
	}
}

func TestPutbackReader_Overflow(t *testing.T) {
	// Try to put back more than HeaderSize bytes. Extra bytes are
	// silently dropped; we keep the LAST HeaderSize.
	src := bytes.NewReader([]byte("hello"))
	pb := NewPutbackReader(src)
	pb.Putback([]byte("ABCDEFGHIJ")) // 10 bytes, > HeaderSize (5)
	rest := make([]byte, 5)
	n, _ := pb.Read(rest)
	if n != 5 || string(rest) != "FGHIJ" {
		t.Errorf("overflow: got %q (n=%d), want last 5 chars %q", rest[:n], n, "FGHIJ")
	}
}

func TestPutbackReader_Putback(t *testing.T) {
	// Putback inserts at front; when buffer is empty, it's straightforward.
	pb := NewPutbackReader(bytes.NewReader([]byte("abc")))
	pb.Putback([]byte("XY"))
	buf := make([]byte, 5)
	n, err := pb.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || string(buf[:n]) != "XY" {
		t.Errorf("Read after Putback = %q, want %q", string(buf[:n]), "XY")
	}
	// Then the underlying reader returns the rest.
	n, err = pb.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || string(buf[:n]) != "abc" {
		t.Errorf("Read after Putback = %q, want %q", string(buf[:n]), "abc")
	}
}

// Benchmark: prove splice is faster than allocating a typed struct
// for DataRow (the dominant message in a SELECT result set).
func BenchmarkDrainSplice_BoringBoringBoring(b *testing.B) {
	// 10 DataRows of ~16 bytes each.
	row := makeMsg('D', []byte{0, 1, 0, 0, 0, 12, 'c', 'o', 'l', '_', 'v', 'a', 'l', 'u', 'e', '!', '!', '!'})
	src := bytes.Repeat(row, 10)
	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pb := NewPutbackReader(bytes.NewReader(src))
		var dst bytes.Buffer
		_ = DrainSplice(&dst, pb, 4096)
	}
}

// makeMsg is a testing.T-free variant of msg for benchmarks.
func makeMsg(tag byte, body []byte) []byte {
	length := 4 + len(body)
	out := make([]byte, 1+length)
	out[0] = tag
	binary.BigEndian.PutUint32(out[1:5], uint32(length))
	copy(out[5:], body)
	return out
}
