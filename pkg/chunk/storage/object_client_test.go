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
)

func TestChunksBasic(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, _ chunk.IndexClient, client chunk.ObjectClient) {
		const batchSize = 5
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		// Write a few batches of chunks.
		written := []string{}
		for i := 0; i < 5; i++ {
			keys, chunks, err := testutils.CreateChunks(i, batchSize, testutils.From(model.Now()))
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

// TestScanner ensures Streamer clients honors the userID batch option
func TestScanner(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, _ chunk.IndexClient, client chunk.ObjectClient) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		batch := client.NewScanner()
		if batch == nil {
			return
		}
		schema := testutils.DefaultSchemaConfig("")

		tablename, err := schema.ChunkTableFor(model.Now().Add(-time.Hour))
		require.NoError(t, err)

		request := chunk.ScanRequest{
			Table: tablename,
			User:  "userIDNew",
			Shard: -1,
		}

		_, chunks, err := testutils.CreateChunks(0, 2000)
		require.NoError(t, err)

		err = client.PutChunks(ctx, chunks)
		require.NoError(t, err)

		_, chunks, err = testutils.CreateChunks(0, 2000, testutils.User("userIDNew"))
		require.NoError(t, err)

		err = client.PutChunks(ctx, chunks)
		require.NoError(t, err)

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

		err = batch.Scan(context.Background(), request, out)
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
