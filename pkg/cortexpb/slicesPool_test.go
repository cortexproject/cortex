package cortexpb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteSlicePools(t *testing.T) {
	sut := newSlicePool(20)
	for i := 0; i < 1024*1024; i = i + 128 {
		s := sut.getSlice(i)
		assert.Equal(t, len(*s), i)
		sut.reuseSlice(s)
	}
}
