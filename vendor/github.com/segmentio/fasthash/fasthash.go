package fasthash

import (
	"encoding/binary"
	"hash"
)

// HashString64 makes a hashing function from the hashing algorithm returned by f.
func HashString64(f func() hash.Hash64) func(string) uint64 {
	return func(s string) uint64 {
		h := f()
		h.Write([]byte(s))
		return h.Sum64()
	}
}

// HashUint64 makes a hashing function from the hashing algorithm return by f.
func HashUint64(f func() hash.Hash64) func(uint64) uint64 {
	return func(u uint64) uint64 {
		b := [8]byte{}
		binary.BigEndian.PutUint64(b[:], u)
		h := f()
		h.Write(b[:])
		return h.Sum64()
	}
}
