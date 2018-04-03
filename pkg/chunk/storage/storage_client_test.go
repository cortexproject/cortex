package storage

import (
	"context"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/weaveworks/cortex/pkg/chunk"
)

func TestChunksBasic(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, client chunk.StorageClient) {
		const batchSize = 50
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Write a few batches of chunks.
		written := []string{}
		for i := 0; i < 50; i++ {
			chunks := []chunk.Chunk{}
			for j := 0; j < batchSize; j++ {
				chunk := dummyChunkFor(model.Now(), model.Metric{
					model.MetricNameLabel: "foo",
					"index":               model.LabelValue(strconv.Itoa(i*batchSize + j)),
				})
				chunks = append(chunks, chunk)
				_, err := chunk.Encode() // Need to encode it, side effect calculates crc
				require.NoError(t, err)
				written = append(written, chunk.ExternalKey())
			}

			err := client.PutChunks(ctx, chunks)
			require.NoError(t, err)
		}

		// Get a few batches of chunks.
		for i := 0; i < 50; i++ {
			keysToGet := map[string]struct{}{}
			chunksToGet := []chunk.Chunk{}
			for len(chunksToGet) < batchSize {
				key := written[rand.Intn(len(written))]
				if _, ok := keysToGet[key]; ok {
					continue
				}
				keysToGet[key] = struct{}{}
				chunk, err := chunk.ParseExternalKey(userID, key)
				require.NoError(t, err)
				chunksToGet = append(chunksToGet, chunk)
			}

			chunksWeGot, err := client.GetChunks(ctx, chunksToGet)
			require.NoError(t, err)
			require.Equal(t, len(chunksToGet), len(chunksWeGot))

			sort.Sort(chunk.ByKey(chunksToGet))
			sort.Sort(chunk.ByKey(chunksWeGot))
			for j := 0; j < len(chunksWeGot); j++ {
				require.Equal(t, chunksToGet[i].ExternalKey(), chunksWeGot[i].ExternalKey(), strconv.Itoa(i))
			}
		}
	})
}
