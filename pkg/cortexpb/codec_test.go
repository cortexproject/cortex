package cortexpb

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"
)

func TestMergingBuffers(t *testing.T) {
	codec := &cortexCodec{}
	wr := &WriteRequest{}

	data, err := codec.Marshal(wr)
	require.NoError(t, err)

	wr1 := &WriteRequest{}
	wr2 := &WriteRequest{}
	require.NoError(t, codec.Unmarshal(data, wr1))
	require.NoError(t, codec.Unmarshal(data, wr2))
	// should not be the same reference but should have the same value
	require.False(t, isSameReference(wr1.bs, wr2.bs))
	require.NotEmpty(t, wr1.bs)
	require.Equal(t, wr1.bs, wr2.bs)
	finalWr := &WriteRequest{}
	finalWr.MergeBuffers(wr1)
	require.Empty(t, wr1.bs)
	finalWr.MergeBuffers(wr2)
	require.Empty(t, wr2.bs)
	require.Len(t, finalWr.bs, 2)
	finalWr.Free()
	require.Empty(t, finalWr.bs)
}

func isSameReference(a, b mem.BufferSlice) bool {
	return len(a) > 0 && len(b) > 0 &&
		unsafe.Pointer(&a[0]) == unsafe.Pointer(&b[0])
}
