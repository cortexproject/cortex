package util

import (
	"crypto/md5"
	"encoding/binary"
)

// ShuffleShardSeed returns seed for random number generator, computed from provided identifier.
func ShuffleShardSeed(identifier string) int64 {
	// Use the identifier to compute an hash we'll use to seed the random.
	hasher := md5.New()
	hasher.Write([]byte(identifier)) // nolint:errcheck
	checksum := hasher.Sum(nil)

	// Generate the seed based on the first 64 bits of the checksum.
	return int64(binary.BigEndian.Uint64(checksum))
}
