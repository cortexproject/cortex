package cortexpb

import (
	"math"
	"sync"
)

const (
	minPoolSizePower = 5
)

type byteSlicePools struct {
	pools []sync.Pool
}

func newSlicePool(pools int) *byteSlicePools {
	sp := byteSlicePools{}
	sp.init(pools)
	return &sp
}

func (sp *byteSlicePools) init(pools int) {
	sp.pools = make([]sync.Pool, pools)
	for i := 0; i < pools; i++ {
		size := int(math.Pow(2, float64(i+minPoolSizePower)))
		sp.pools[i] = sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 0, size)
				return &buf
			},
		}
	}
}

func (sp *byteSlicePools) getSlice(size int) *[]byte {
	index := int(math.Ceil(math.Log2(float64(size)))) - minPoolSizePower

	if index >= len(sp.pools) {
		buf := make([]byte, size)
		return &buf
	}

	// if the size is < than the minPoolSizePower we return an array from the first pool
	if index < 0 {
		index = 0
	}

	s := sp.pools[index].Get().(*[]byte)
	*s = (*s)[:size]
	return s
}

func (sp *byteSlicePools) reuseSlice(s *[]byte) {
	index := int(math.Floor(math.Log2(float64(cap(*s))))) - minPoolSizePower

	if index >= len(sp.pools) || index < 0 {
		return
	}

	sp.pools[index].Put(s)
}
