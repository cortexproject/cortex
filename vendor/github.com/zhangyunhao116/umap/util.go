package umap

import (
	"math/bits"
	"unsafe"
)

// ptrSize is the size of a pointer in bytes - unsafe.Sizeof(uintptr(0)) but as an ideal constant.
// It is also the size of the machine's native word size (that is, 4 on 32-bit systems, 8 on 64-bit).
const ptrSize = 4 << (^uintptr(0) >> 63)

func bmapPointer(p unsafe.Pointer, n uint) *bmapuint64 {
	return (*bmapuint64)(add(p, unsafe.Sizeof(bmapuint64{})*uintptr(n)))
}

func nextPowerOfTwo(length int) uint {
	shift := bits.Len(uint(length))
	return uint(1) << (shift & (ptrSize*8 - 1)) // 1 << shift, optimized for code generation
}

//go:nosplit
func add(p unsafe.Pointer, x uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(p) + x)
}

// growThresholdUint64 returns (capcity - capcity*loadFactor)
func growThresholdUint64(bucketmask uint64) int {
	// Same as (capcity - capcity*loadFactor), but faster.
	return int((bucketmask + 1) * emptyItemInBucket)
}
