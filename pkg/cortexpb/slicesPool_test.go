package cortexpb

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFuzzyByteSlicePools(t *testing.T) {
	sut := newSlicePool(20)
	maxByteSize := int(math.Pow(2, 20+minPoolSizePower-1))

	for i := 0; i < 1000; i++ {
		size := rand.Int() % maxByteSize
		s := sut.getSlice(size)
		assert.Equal(t, len(*s), size)
		sut.reuseSlice(s)
	}
}
