package storegateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/pool"
)

type chunkBytesPool struct {
	pool *pool.BucketedBytes

	// Metrics.
	poolByteStats *prometheus.CounterVec
}

func newChunkBytesPool(minBucketSize, maxBucketSize int, maxChunkPoolBytes uint64, reg prometheus.Registerer) (*chunkBytesPool, error) {
	upstream, err := pool.NewBucketedBytes(minBucketSize, maxBucketSize, 2, maxChunkPoolBytes)
	if err != nil {
		return nil, err
	}

	return &chunkBytesPool{
		pool: upstream,
		poolByteStats: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_bucket_store_chunk_pool_bytes_total",
			Help: "Total bytes number of bytes pooled by operation.",
		}, []string{"operation", "stats"}),
	}, nil
}

func (p *chunkBytesPool) Get(sz int) (*[]byte, error) {
	buffer, err := p.pool.Get(sz)
	if err != nil {
		return buffer, err
	}

	p.poolByteStats.WithLabelValues("Get", "Requested").Add(float64(sz))
	p.poolByteStats.WithLabelValues("Get", "Cap").Add(float64(cap(*buffer)))

	return buffer, err
}

func (p *chunkBytesPool) Put(b *[]byte) {
	p.poolByteStats.WithLabelValues("Put", "Len").Add(float64(len(*b)))
	p.poolByteStats.WithLabelValues("Put", "Cap").Add(float64(cap(*b)))
	p.pool.Put(b)
}
