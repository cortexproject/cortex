package util

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDecompressRequest_GzipDecompressionBomb guards the
// io.LimitReader(gzReader, maxSize+1) cap that decompressFromReader puts on gzip
// output. It feeds a tiny gzip payload that inflates to far more than maxSize (a
// "zip bomb") and asserts the buffered body never exceeds maxSize+1.
//
// This is a white-box test on purpose. The only signal that distinguishes the
// cap being present from absent is how many bytes are decompressed into memory,
// and that is not observable through ParseProtoReader: both paths return an
// error, and the compressed input is already bounded by an outer LimitReader, so
// counting bytes read from the source cannot tell them apart. Asserting on
// len(body) is deterministic and parallel-safe, unlike reading the process-wide,
// monotonic runtime.MemStats.TotalAlloc, which concurrent goroutines or parallel
// tests would inflate. Removing the cap lets the reader inflate the bomb to
// megabytes, so len(body) blows past maxSize+1 and this test fails, which is what
// makes it a real regression guard rather than a no-op.
func TestDecompressRequest_GzipDecompressionBomb(t *testing.T) {
	const maxSize = 4096                 // 4 KB limit on decompressed output
	uncompressed := make([]byte, 32<<20) // 32 MB of zeros, compresses to a few KB
	require.Greater(t, len(uncompressed), maxSize)

	var compressed bytes.Buffer
	gzw := gzip.NewWriter(&compressed)
	_, err := gzw.Write(uncompressed)
	require.NoError(t, err)
	require.NoError(t, gzw.Close())

	body, _ := decompressRequest(bytes.NewReader(compressed.Bytes()), 0, maxSize, Gzip, nil)

	require.LessOrEqual(t, len(body), maxSize+1,
		"gzip decompression buffered %d bytes, exceeding the maxSize+1 cap; the io.LimitReader guard is missing", len(body))
}
