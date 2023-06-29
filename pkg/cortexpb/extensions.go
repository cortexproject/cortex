package cortexpb

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"

	"github.com/cortexproject/cortex/pkg/tenant"
)

const maxBufferSize = 1024
const signVersion = "v1"

var signerPool = sync.Pool{
	New: func() interface{} {
		return newSigner()
	},
}

type signer struct {
	h         *xxhash.Digest
	b         []byte
	optimized bool
}

func newSigner() *signer {
	s := &signer{
		h: xxhash.New(),
		b: make([]byte, 0, maxBufferSize),
	}
	s.Reset()
	return s
}

func (s *signer) Reset() {
	s.h.Reset()
	s.b = s.b[:0]
	s.optimized = true
}

func (s *signer) WriteString(val string) {
	switch {
	case !s.optimized:
		_, _ = s.h.WriteString(val)
	case len(s.b)+len(val) > cap(s.b):
		// If labels val does not fit in the []byte we fall back to not allocate the whole entry.
		_, _ = s.h.Write(s.b)
		_, _ = s.h.WriteString(val)
		s.optimized = false
	default:
		// Use xxhash.Sum64(b) for fast path as it's faster.
		s.b = append(s.b, val...)
	}
}

func (s *signer) Sum64() uint64 {
	if s.optimized {
		return xxhash.Sum64(s.b)
	}

	return s.h.Sum64()
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

	s := signerPool.Get().(*signer)
	defer func() {
		s.Reset()
		signerPool.Put(s)
	}()
	s.WriteString(u)

	for _, md := range w.Metadata {
		s.WriteString(strconv.Itoa(int(md.Type)))
		s.WriteString(md.MetricFamilyName)
		s.WriteString(md.Help)
		s.WriteString(md.Unit)
	}

	for _, ts := range w.Timeseries {
		for _, lbl := range ts.Labels {
			s.WriteString(lbl.Name)
			s.WriteString(lbl.Value)
		}

		for _, ex := range ts.Exemplars {
			for _, lbl := range ex.Labels {
				s.WriteString(lbl.Name)
				s.WriteString(lbl.Value)
			}
		}
	}

	return fmt.Sprintf("%v/%v", signVersion, s.Sum64()), nil
}
