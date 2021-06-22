package jump

import "github.com/cespare/xxhash"

// Hash consistently chooses a hash bucket number in the range [0, numBuckets) for the given key.
// numBuckets must be >= 1.
//
// Copied from github.com/dgryski/go-jump/blob/master/jump.go
func Hash(key string, numBuckets int) int32 {
	var cs uint64 = xxhash.Sum64String(key)
	var b int64 = -1
	var j int64

	for j < int64(numBuckets) {
		b = j
		cs = cs*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((cs>>33)+1)))
	}

	return int32(b)
}
