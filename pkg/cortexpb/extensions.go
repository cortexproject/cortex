package cortexpb

import (
	"context"
	"fmt"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/cortexproject/cortex/pkg/tenant"
)

const maxBufferSize = 1024
const signVersion = "v1"

var byteSlicePool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, maxBufferSize)
		return &b
	},
}

func (w *WriteRequest) VerifySign(ctx context.Context, signature string) (bool, error) {
	s, err := w.Sign(ctx)
	return s == signature, err
}

func (w *WriteRequest) Sign(ctx context.Context) (string, error) {
	u, err := tenant.TenantID(ctx)
	if err != nil {
		return "", err
	}

	// Use xxhash.Sum64(b) for fast path as it's faster.
	bp := byteSlicePool.Get().(*[]byte)
	b := (*bp)[:0]
	defer byteSlicePool.Put(bp)
	b = append(b, u...)

	for _, s := range w.Timeseries {
		for i, v := range s.Labels {
			if len(b)+len(v.Name)+len(v.Value)+2 >= cap(b) {
				// If labels entry is {maxBufferSize}+ do not allocate whole entry.
				h := xxhash.New()
				_, _ = h.Write(b)
				for _, v := range s.Labels[i:] {
					_, _ = h.WriteString(v.Name)
					_, _ = h.WriteString(v.Value)
				}
				return fmt.Sprintf("%v/%v", signVersion, h.Sum64()), nil
			}

			b = append(b, v.Name...)
			b = append(b, v.Value...)
		}
	}

	return fmt.Sprintf("%v/%v", signVersion, xxhash.Sum64(b)), nil
}
