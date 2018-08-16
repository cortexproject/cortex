package fasthash

import (
	"encoding/binary"
	"hash"
)

// HashString32 makes a hashing function from the hashing algorithm returned by f.
func HashString32(f func() hash.Hash32) func(string) uint32 {
	return func(s string) uint32 {
		h := f()
		h.Write([]byte(s))
		return h.Sum32()
	}
}

// HashUint32 makes a hashing function from the hashing algorithm return by f.
func HashUint32(f func() hash.Hash32) func(uint32) uint32 {
	return func(u uint32) uint32 {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], u)
		h := f()
		h.Write(b[:])
		return h.Sum32()
	}
}
