package tsdb

import (
	"github.com/oklog/ulid/v2"

	"github.com/cortexproject/cortex/pkg/util"
)

// HashBlockID returns a 32-bit hash of the block ID useful for
// ring-based sharding.
func HashBlockID(id ulid.ULID) uint32 {
	h := util.HashNew32()
	for _, b := range id {
		h = util.HashAddByte32(h, b)
	}
	return h
}
