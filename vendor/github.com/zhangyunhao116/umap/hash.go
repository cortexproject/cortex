package umap

import "math/bits"

func hashUint64(x uint64) uint64 {
	// Mixhash: https://github.com/zhangyunhao116/mixhash
	return _mix(x^0xcbdf2e5c79d30006, 0xfc56be937d474100)
}

func _mix(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi ^ lo
}
