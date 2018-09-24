package storage

import (
	"context"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/testutils"
	"github.com/prometheus/common/model"
)

func TestChunksBasic(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, _ chunk.IndexClient, client chunk.ObjectClient) {
		const batchSize = 5
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		// Write a few batches of chunks.
		written := []string{}
		for i := 0; i < 5; i++ {
			keys, chunks, err := testutils.CreateChunks(i, batchSize, model.Now())
			require.NoError(t, err)
			written = append(written, keys...)
			err = client.PutChunks(ctx, chunks)
			require.NoError(t, err)
		}

		// Get a few batches of chunks.
		for batch := 0; batch < 50; batch++ {
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

			sort.Sort(ByKey(chunksToGet))
			sort.Sort(ByKey(chunksWeGot))
			for i := 0; i < len(chunksWeGot); i++ {
				require.Equal(t, chunksToGet[i].ExternalKey(), chunksWeGot[i].ExternalKey(), strconv.Itoa(i))
			}
		}
	})
}

func TestStreamChunks(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, client chunk.StorageClient, schema chunk.SchemaConfig) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, chunks, err := testutils.CreateChunks(0, 2000, model.Now())
		require.NoError(t, err)

		err = client.PutChunks(ctx, chunks)
		require.NoError(t, err)

		batch := client.NewStreamBatch()
		if batch == nil {
			return
		}
		tablename := schema.ChunkTables.TableFor(model.Now().Add(-time.Hour))
		batch.Add(tablename, "userID", 0, 240)

		var retrievedChunks []chunk.Chunk
		var wg sync.WaitGroup
		out := make(chan []chunk.Chunk)
		go func() {
			wg.Add(1)
			for c := range out {
				retrievedChunks = append(retrievedChunks, c...)
			}
			wg.Done()
		}()

		err = client.StreamChunks(context.Background(), batch, out)
		require.NoError(t, err)

		close(out)
		wg.Wait()
		require.Equal(t, 2000, len(retrievedChunks))

		sort.Sort(ByKey(retrievedChunks))
		sort.Sort(ByKey(chunks))
		for j := 0; j < len(retrievedChunks); j++ {
			require.Equal(t, chunks[j].ExternalKey(), retrievedChunks[j].ExternalKey(), strconv.Itoa(j))
		}
	})
}

// TestStreamChunksByUserID ensures StreamChunks clients honors the userID batch option
func TestStreamChunksByUserID(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, client chunk.StorageClient, schema chunk.SchemaConfig) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, chunks, err := testutils.CreateChunks(0, 2000, model.Now())
		require.NoError(t, err)

		err = client.PutChunks(ctx, chunks)
		require.NoError(t, err)

		_, chunks, err = testutils.CreateChunks(0, 2000, model.Now(), testutils.User("userIDNew"))
		require.NoError(t, err)

		err = client.PutChunks(ctx, chunks)
		require.NoError(t, err)

		batch := client.NewStreamBatch()
		if batch == nil {
			return
		}
		tablename := schema.ChunkTables.TableFor(model.Now().Add(-time.Hour))
		batch.Add(tablename, "userIDNew", 0, 240)

		var retrievedChunks []chunk.Chunk
		var wg sync.WaitGroup
		out := make(chan []chunk.Chunk)
		go func() {
			wg.Add(1)
			for c := range out {
				var fingerprint string
				if len(c) > 0 {
					fingerprint = c[0].Fingerprint.String()
				}
				for _, chun := range c {
					require.Equal(t, chun.Fingerprint.String(), fingerprint)
				}
				retrievedChunks = append(retrievedChunks, c...)
			}
			wg.Done()
		}()

		err = client.StreamChunks(context.Background(), batch, out)
		require.NoError(t, err)

		close(out)
		wg.Wait()
		require.Equal(t, 2000, len(retrievedChunks))

		sort.Sort(ByKey(retrievedChunks))
		sort.Sort(ByKey(chunks))
		for j := 0; j < len(retrievedChunks); j++ {
			require.Equal(t, retrievedChunks[j].UserID, "userIDNew")
			require.Equal(t, chunks[j].ExternalKey(), retrievedChunks[j].ExternalKey(), strconv.Itoa(j))
		}
	})
}
