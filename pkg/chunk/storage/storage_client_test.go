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
	"github.com/weaveworks/cortex/pkg/chunk/aws"
	"github.com/weaveworks/cortex/pkg/chunk/gcp"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

var fixtures = append(aws.Fixtures, gcp.Fixtures...)

func TestStoreChunks(t *testing.T) {
	for _, fixture := range fixtures {
		t.Run(fixture.Name(), func(t *testing.T) {
			storageClient, tableClient, schemaConfig, err := fixture.Clients()
			require.NoError(t, err)
			defer fixture.Teardown()

			tableManager, err := chunk.NewTableManager(schemaConfig, 12*time.Hour, tableClient)
			require.NoError(t, err)

			err = tableManager.SyncTables(context.Background())
			require.NoError(t, err)

			testStorageClientChunks(t, storageClient)
		})
	}
}

func TestStoreIndex(t *testing.T) {
	for _, fixture := range fixtures {
		t.Run(fixture.Name(), func(t *testing.T) {
			storageClient, tableClient, schemaConfig, err := fixture.Clients()
			require.NoError(t, err)
			defer fixture.Teardown()

			tableManager, err := chunk.NewTableManager(schemaConfig, 12*time.Hour, tableClient)
			require.NoError(t, err)

			err = tableManager.SyncTables(context.Background())
			require.NoError(t, err)

			testStorageClientIndex(t, storageClient, tableClient)
		})
	}
}

func testStorageClientChunks(t *testing.T, client chunk.StorageClient) {
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
}

func testStorageClientIndex(t *testing.T, client chunk.StorageClient, tableClient chunk.TableClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type writeBatch []struct {
		tableName, hashValue string
		rangeValue           []byte
		value                []byte
	}

	cases := []struct {
		batch writeBatch

		query []chunk.IndexQuery
	}{
		{
			batch: writeBatch{
				{
					tableName:  "t1",
					hashValue:  "hash1",
					rangeValue: []byte("range1"),
					value:      []byte("1"),
				},
				{
					tableName:  "t1",
					hashValue:  "hash1",
					rangeValue: []byte("range2"),
					value:      []byte("2"),
				},
				{
					tableName:  "t1",
					hashValue:  "hash1",
					rangeValue: []byte("range3"),
					value:      []byte("3"),
				},
				{
					tableName:  "t1",
					hashValue:  "hash1",
					rangeValue: []byte("bleepboop"),
					value:      []byte("4"),
				},
				{
					tableName:  "t1",
					hashValue:  "hash2",
					rangeValue: []byte("bleepboop"),
					value:      []byte("5"),
				},
			},

			query: []chunk.IndexQuery{
				{
					TableName:        "t1",
					HashValue:        "hash1",
					RangeValuePrefix: []byte("range"),
				},
				{
					TableName:       "t1",
					HashValue:       "hash1",
					RangeValueStart: []byte("range2"),
				},
			},
		},
	}

	for _, c := range cases {
		mockSC := chunk.NewMockStorage()
		require.NoError(t, mockSC.CreateTable(ctx, chunk.TableDesc{
			Name: c.batch[0].tableName,
		}))
		require.NoError(t, tableClient.CreateTable(ctx, chunk.TableDesc{
			Name:             c.batch[0].tableName,
			ProvisionedRead:  100000,
			ProvisionedWrite: 100000,
		}))

		mockWB := mockSC.NewWriteBatch()
		actualWB := client.NewWriteBatch()

		for _, b := range c.batch {
			mockWB.Add(b.tableName, b.hashValue, b.rangeValue, b.value)
			actualWB.Add(b.tableName, b.hashValue, b.rangeValue, b.value)
		}

		require.NoError(t, mockSC.BatchWrite(ctx, mockWB))
		require.NoError(t, client.BatchWrite(ctx, actualWB))

		type readVal struct {
			rangeValue string
			value      []byte
		}

		for _, qry := range c.query {
			expVals := []readVal{}
			gotVals := []readVal{}

			err := mockSC.QueryPages(ctx, qry, func(result chunk.ReadBatch, lp bool) bool {
				for i := 0; i < result.Len(); i++ {
					expVals = append(expVals, readVal{
						rangeValue: string(result.RangeValue(i)),
						value:      result.Value(i),
					})
				}
				return !lp
			})
			require.NoError(t, err)

			err = client.QueryPages(ctx, qry, func(result chunk.ReadBatch, lp bool) bool {
				for i := 0; i < result.Len(); i++ {
					gotVals = append(gotVals, readVal{
						rangeValue: string(result.RangeValue(i)),
						value:      result.Value(i),
					})
				}
				return !lp
			})
			require.NoError(t, err)

			sort.Slice(expVals, func(i, j int) bool {
				return expVals[i].rangeValue < expVals[j].rangeValue
			})
			sort.Slice(gotVals, func(i, j int) bool {
				return gotVals[i].rangeValue < gotVals[j].rangeValue
			})

			require.Equal(t, expVals, gotVals)
		}
	}
}

const userID = "userID"

func dummyChunk(now model.Time) chunk.Chunk {
	return dummyChunkFor(now, model.Metric{
		model.MetricNameLabel: "foo",
		"bar":  "baz",
		"toms": "code",
	})
}

func dummyChunkFor(now model.Time, metric model.Metric) chunk.Chunk {
	cs, _ := promchunk.New().Add(model.SamplePair{Timestamp: now, Value: 0})
	chunk := chunk.NewChunk(
		userID,
		metric.Fingerprint(),
		metric,
		cs[0],
		now.Add(-time.Hour),
		now,
	)
	// Force checksum calculation.
	_, err := chunk.Encode()
	if err != nil {
		panic(err)
	}
	return chunk
}
