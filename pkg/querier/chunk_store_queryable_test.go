package querier

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
)

func TestChunkQueryable(t *testing.T) {
	for _, testcase := range testcases {
		for _, encoding := range encodings {
			for _, query := range queries {
				t.Run(fmt.Sprintf("%s/%s/%s", testcase.name, encoding.name, query.query), func(t *testing.T) {
					store, from := makeMockChunkStore(t, 24, encoding.e)
					queryable := newChunkStoreQueryable(store, testcase.f)
					testQuery(t, queryable, from, query)
				})
			}
		}
	}
}

type mockChunkStore struct {
	chunks []chunk.Chunk
}

func (m mockChunkStore) Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {
	return m.chunks, nil
}

func makeMockChunkStore(t require.TestingT, numChunks int, encoding promchunk.Encoding) (mockChunkStore, model.Time) {
	var (
		chunks = make([]chunk.Chunk, 0, numChunks)
		from   = model.Time(0)
	)
	for i := 0; i < numChunks; i++ {
		c := mkChunk(t, from, from.Add(samplesPerChunk*sampleRate), sampleRate, encoding)
		chunks = append(chunks, c)
		from = from.Add(chunkOffset)
	}
	return mockChunkStore{chunks}, from
}

func mkChunk(t require.TestingT, mint, maxt model.Time, step time.Duration, encoding promchunk.Encoding) chunk.Chunk {
	metric := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo"},
	}
	pc, err := promchunk.NewForEncoding(encoding)
	require.NoError(t, err)
	for i := mint; i.Before(maxt); i = i.Add(step) {
		nc, err := pc.Add(model.SamplePair{
			Timestamp: i,
			Value:     model.SampleValue(float64(i)),
		})
		require.NoError(t, err)
		require.Nil(t, nc)
	}
	return chunk.NewChunk(userID, fp, metric, pc, mint, maxt)
}

func TestPartitionChunksOutputIsSortedByLabels(t *testing.T) {
	allChunks := []chunk.Chunk{}

	const count = 10
	for i := count; i > 0; i-- {
		ch := mkChunk(t, model.Time(0), model.Time(1000), time.Millisecond, promchunk.Bigchunk)
		ch.Metric[0].Value = fmt.Sprintf("%02d", i)

		allChunks = append(allChunks, ch)
	}

	res := partitionChunks(allChunks, 0, 1000, mergeChunks)

	for i := 1; i <= count; i++ {
		require.True(t, res.Next())
		require.Equal(t, labels.Labels{{Name: model.MetricNameLabel, Value: fmt.Sprintf("%02d", i)}}, res.At().Labels())
	}
	require.False(t, res.Next())
}
