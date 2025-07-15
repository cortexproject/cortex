package cortexpb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"
)

type wrappedBufferPool struct {
	inner    mem.BufferPool
	getCount int
	putCount int
}

func (w *wrappedBufferPool) Get(length int) *[]byte {
	w.getCount++
	return w.inner.Get(length)
}

func (w *wrappedBufferPool) Put(b *[]byte) {
	w.putCount++
	w.inner.Put(b)
}

func (w *wrappedBufferPool) Reset() {
	w.getCount = 0
	w.putCount = 0
}

func TestNoopBufferWhenNotReleasableMessage(t *testing.T) {
	codec := &cortexCodec{
		noOpBufferPool:    &wrappedBufferPool{inner: mem.NopBufferPool{}},
		defaultBufferPool: &wrappedBufferPool{inner: mem.DefaultBufferPool()},
	}

	tc := map[string]struct {
		noopBufferGets    int
		defaultBufferGets int
		m                 any
	}{
		"releasable": {
			noopBufferGets:    0,
			defaultBufferGets: 1,
			m: &WriteRequest{
				Metadata: []*MetricMetadata{
					{
						Unit: strings.Repeat("a", 10000),
					},
				},
			},
		},
		"not releasable": {
			noopBufferGets:    1,
			defaultBufferGets: 0,
			m: &MetricMetadata{
				Unit: strings.Repeat("a", 10000),
			},
		},
	}

	for name, tc := range tc {
		t.Run(name, func(t *testing.T) {
			data, err := codec.Marshal(tc.m)
			require.NoError(t, err)

			// lets split the buffer into 2 so we force get another buffer from the pool
			r := data.Reader()
			size := r.Remaining()
			b1 := make([]byte, size/2)
			b2 := make([]byte, (size/2)+1)
			buffer1 := mem.NewBuffer(&b1, mem.NopBufferPool{})
			buffer2 := mem.NewBuffer(&b2, mem.NopBufferPool{})

			_, err = r.Read(b1)
			require.NoError(t, err)
			_, err = r.Read(b2)
			require.NoError(t, err)

			codec.noOpBufferPool.(*wrappedBufferPool).Reset()
			codec.defaultBufferPool.(*wrappedBufferPool).Reset()
			require.NoError(t, codec.Unmarshal(mem.BufferSlice{buffer1, buffer2}, tc.m))
			require.Equal(t, tc.noopBufferGets, codec.noOpBufferPool.(*wrappedBufferPool).getCount)
			require.Equal(t, tc.defaultBufferGets, codec.defaultBufferPool.(*wrappedBufferPool).getCount)
		})
	}
}
