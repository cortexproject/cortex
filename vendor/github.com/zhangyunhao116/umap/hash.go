package umap

import "math/bits"

// Using FxHash: https://searchfox.org/mozilla-central/rev/633345116df55e2d37be9be6555aa739656c5a7d/mfbt/HashFunctions.h
// Benchmarks for different hash algorithms: https://github.com/tkaitchuck/aHash/blob/master/FAQ.md#how-is-ahash-so-fast

func hashUint64(x uint64) uint64 {
	const K = 0x517cc1b727220a95
	return (bits.RotateLeft64(x, 5) ^ x) * K
}
