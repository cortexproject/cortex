package storegateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/pool"
)

type chunkBytesPool struct {
	pool *pool.BucketedBytes

	// Metrics.
	requestedBytes prometheus.Counter
	returnedBytes  prometheus.Counter

	poolByteStats *prometheus.CounterVec
}

func newChunkBytesPool(minBucketSize, maxBucketSize int, maxChunkPoolBytes uint64, reg prometheus.Registerer) (*chunkBytesPool, error) {
	upstream, err := pool.NewBucketedBytes(minBucketSize, maxBucketSize, 2, maxChunkPoolBytes)
	if err != nil {
		return nil, err
	}

	return &chunkBytesPool{
		pool: upstream,
		requestedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_chunk_pool_requested_bytes_total",
			Help: "Total bytes requested to chunk bytes pool.",
		}),
		returnedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_chunk_pool_returned_bytes_total",
			Help: "Total bytes returned by the chunk bytes pool.",
		}),
		poolByteStats: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_bucket_store_chunk_pool_bytes_total",
			Help: "Total bytes polled by the chunk bytes pool by operation.",
		}, []string{"operation", "stats"}),
	}, nil
}

func (p *chunkBytesPool) Get(sz int) (*[]byte, error) {
	buffer, err := p.pool.Get(sz)
	if err != nil {
		return buffer, err
	}

	p.requestedBytes.Add(float64(sz))
	p.returnedBytes.Add(float64(cap(*buffer)))

	p.poolByteStats.WithLabelValues("Get", "requested").Add(float64(sz))
	p.poolByteStats.WithLabelValues("Get", "len").Add(float64(len(*buffer)))
	p.poolByteStats.WithLabelValues("Get", "cap").Add(float64(cap(*buffer)))

	return buffer, err
}

func (p *chunkBytesPool) Put(b *[]byte) {
	p.poolByteStats.WithLabelValues("Put", "len").Add(float64(len(*b)))
	p.poolByteStats.WithLabelValues("Put", "cap").Add(float64(cap(*b)))
	p.pool.Put(b)
}
