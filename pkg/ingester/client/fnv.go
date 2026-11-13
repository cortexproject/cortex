package client

import "github.com/cortexproject/cortex/pkg/util"

// These hash helpers moved to pkg/util, so that the ring package can share them.
// They are kept here so that code importing them from this package keeps working.

// HashNew32 initializes a new fnv32 hash value.
//
// Deprecated: use util.HashNew32.
func HashNew32() uint32 {
	return util.HashNew32()
}

// HashAdd32 adds a string to a fnv32 hash value, returning the updated hash.
//
// Deprecated: use util.HashAdd32.
func HashAdd32(h uint32, s string) uint32 {
	return util.HashAdd32(h, s)
}

// HashAddByte32 adds a byte to a fnv32 hash value, returning the updated hash.
//
// Deprecated: use util.HashAddByte32.
func HashAddByte32(h uint32, b byte) uint32 {
	return util.HashAddByte32(h, b)
}
