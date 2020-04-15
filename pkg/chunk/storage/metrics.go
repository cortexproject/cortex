package storage

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
)

type metricsChunkClient struct {
	client chunk.Client

	metrics chunkClientMetrics
}

func newMetricsChunkClient(client chunk.Client, metrics chunkClientMetrics) metricsChunkClient {
	return metricsChunkClient{
		client:  client,
		metrics: metrics,
	}
}

type chunkClientMetrics struct {
	chunksPutPerUser         *prometheus.CounterVec
	chunksSizePutPerUser     *prometheus.CounterVec
	chunksFetchedPerUser     *prometheus.CounterVec
	chunksSizeFetchedPerUser *prometheus.CounterVec
}

func newChunkClientMetrics(reg prometheus.Registerer) chunkClientMetrics {
	return chunkClientMetrics{
		chunksPutPerUser: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "chunk_store_stored_chunks_total",
			Help:      "Total stored chunks per user.",
		}, []string{"user"}),
		chunksSizePutPerUser: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "chunk_store_stored_chunk_bytes_total",
			Help:      "Total bytes stored in chunks per user.",
		}, []string{"user"}),
		chunksFetchedPerUser: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "chunk_store_fetched_chunks_total",
			Help:      "Total fetched chunks per user.",
		}, []string{"user"}),
		chunksSizeFetchedPerUser: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "chunk_store_fetched_chunk_bytes_total",
			Help:      "Total bytes fetched in chunks per user.",
		}, []string{"user"}),
	}
}

func (c metricsChunkClient) Stop() {
	c.client.Stop()
}

func (c metricsChunkClient) PutChunks(ctx context.Context, chunks []chunk.Chunk) error {
	if err := c.client.PutChunks(ctx, chunks); err != nil {
		return err
	}

	if len(chunks) == 0 {
		return nil
	}

	// For PutChunks, we explicitly encode the userID in the chunk and don't use context.
	userSizes := map[string]int{}
	userCounts := map[string]int{}

	for _, c := range chunks {
		userSizes[c.UserID] += c.Data.Size()
		userCounts[c.UserID]++
	}

	for user, size := range userSizes {
		c.metrics.chunksSizePutPerUser.WithLabelValues(user).Add(float64(size))
	}

	for user, num := range userCounts {
		c.metrics.chunksPutPerUser.WithLabelValues(user).Add(float64(num))
	}

	return nil
}

func (c metricsChunkClient) GetChunks(ctx context.Context, chunks []chunk.Chunk) ([]chunk.Chunk, error) {
	chks, err := c.client.GetChunks(ctx, chunks)
	if err != nil {
		return chks, err
	}

	user, err := user.ExtractOrgID(ctx)
	// Should never happen.
	if err != nil {
		return nil, err
	}

	size := 0
	num := 0
	for _, c := range chks {
		num++
		size += c.Data.Size()
	}

	c.metrics.chunksFetchedPerUser.WithLabelValues(user).Add(float64(num))
	c.metrics.chunksSizeFetchedPerUser.WithLabelValues(user).Add(float64(size))

	return chks, nil
}

func (c metricsChunkClient) DeleteChunk(ctx context.Context, chunkID string) error {
	return c.client.DeleteChunk(ctx, chunkID)
}
