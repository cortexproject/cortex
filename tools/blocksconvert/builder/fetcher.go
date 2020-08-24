package builder

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/cache"
)

type fetcher struct {
	userID string

	client chunk.Client

	fetchedChunks     prometheus.Counter
	fetchedChunksSize prometheus.Counter
}

func newFetcher(userID string, client chunk.Client, chunksCache cache.Cache, fetchedChunks, fetchedChunksSize prometheus.Counter) (*fetcher, error) {
	return &fetcher{
		client:            client,
		userID:            userID,
		fetchedChunks:     fetchedChunks,
		fetchedChunksSize: fetchedChunksSize,
	}, nil
}

func (f *fetcher) fetchChunks(ctx context.Context, chunkIDs []string) ([]chunk.Chunk, error) {
	chunks := make([]chunk.Chunk, 0, len(chunkIDs))

	for _, cid := range chunkIDs {
		c, err := chunk.ParseExternalKey(f.userID, cid)
		if err != nil {
			return nil, err
		}

		chunks = append(chunks, c)
	}

	cs, err := f.client.GetChunks(ctx, chunks)
	for _, c := range cs {
		f.fetchedChunks.Inc()
		enc, nerr := c.Encoded()
		if nerr != nil {
			return nil, nerr
		}
		f.fetchedChunksSize.Add(float64(len(enc)))
	}

	return cs, err
}
