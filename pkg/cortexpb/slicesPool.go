package cortexpb

import (
	"math"
	"sync"
)

type byteSlicePools struct {
	pools []sync.Pool
}

func NewSlicePool(pools int) *byteSlicePools {
	sp := byteSlicePools{}
	sp.init(pools)
	return &sp
}

func (sp *byteSlicePools) init(pools int) {
	sp.pools = make([]sync.Pool, pools)
	for i := 0; i < pools; i++ {
		size := int(math.Pow(2, float64(i)))
		sp.pools[i] = sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, size)
			},
		}
	}
}

func (sp *byteSlicePools) getSlice(size int) []byte {
	index := int(math.Ceil(math.Log2(float64(size))))

	if index < 0 || index >= len(sp.pools) {
		return make([]byte, size)
	}

	s := sp.pools[index].Get().([]byte)
	return s[:size]
}

func (sp *byteSlicePools) reuseSlice(s []byte) {
	index := int(math.Ceil(math.Log2(float64(cap(s)))))

	if index < 0 || index >= len(sp.pools) {
		return
	}

	sp.pools[index].Put(s)
}
