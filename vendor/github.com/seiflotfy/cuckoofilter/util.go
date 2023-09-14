package cuckoo

import (
	metro "github.com/dgryski/go-metro"
)

var (
	altHash = [256]uint{}
	masks   = [65]uint{}
)

func init() {
	for i := 0; i < 256; i++ {
		altHash[i] = (uint(metro.Hash64([]byte{byte(i)}, 1337)))
	}
	for i := uint(0); i <= 64; i++ {
		masks[i] = (1 << i) - 1
	}
}

func getAltIndex(fp fingerprint, i uint, bucketPow uint) uint {
	mask := masks[bucketPow]
	hash := altHash[fp] & mask
	return (i & mask) ^ hash
}

func getFingerprint(hash uint64) byte {
	// Use least significant bits for fingerprint.
	fp := byte(hash%255 + 1)
	return fp
}

// getIndicesAndFingerprint returns the 2 bucket indices and fingerprint to be used
func getIndexAndFingerprint(data []byte, bucketPow uint) (uint, fingerprint) {
	hash := metro.Hash64(data, 1337)
	fp := getFingerprint(hash)
	// Use most significant bits for deriving index.
	i1 := uint(hash>>32) & masks[bucketPow]
	return i1, fingerprint(fp)
}

func getNextPow2(n uint64) uint {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return uint(n)
}
