package builder

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/cache"
)

type fetcher struct {
	f      *chunk.Fetcher
	userID string

	fetchedChunks     prometheus.Counter
	fetchedChunksSize prometheus.Counter
}

func newFetcher(userID string, client chunk.Client, chunksCache cache.Cache, fetchedChunks, fetchedChunksSize prometheus.Counter) (*fetcher, error) {
	f, err := chunk.NewChunkFetcher(chunksCache, false, client)
	if err != nil {
		return nil, err
	}

	return &fetcher{
		f:                 f,
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

	cs, err := f.f.FetchChunks(ctx, chunks, chunkIDs)
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

func (f *fetcher) stop() {
	f.f.Stop()
}
