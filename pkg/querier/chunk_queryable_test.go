package querier

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaveworks/cortex/pkg/chunk"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
	"github.com/weaveworks/cortex/pkg/util"
)

const (
	userID          = "userID"
	fp              = 1
	chunkOffset     = 6 * time.Minute
	chunkLength     = 3 * time.Hour
	sampleRate      = 15 * time.Second
	samplesPerChunk = chunkLength / sampleRate
)

var (
	queryables = []struct {
		name string
		f    func(ChunkStore) storage.Queryable
	}{
		{"matrixes", newChunkQueryable},
		{"iterators", newIterChunkQueryable},
	}

	encodings = []struct {
		name string
		e    promchunk.Encoding
	}{
		{"DoubleDelta", promchunk.DoubleDelta},
		{"Varbit", promchunk.Varbit},
	}
)

func TestChunkQueryable(t *testing.T) {
	for _, q := range queryables {
		for _, encoding := range encodings {
			t.Run(fmt.Sprintf("%s/%s", q.name, encoding.name), func(t *testing.T) {
				store, from := makeMockChunkStore(t, 24*30, encoding.e)
				queryable := q.f(store)
				testQuery(t, queryable, from)
			})
		}
	}
}

type mockChunkStore struct {
	chunks []chunk.Chunk
}

func (m mockChunkStore) Get(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {
	return m.chunks, nil
}

func makeMockChunkStore(t require.TestingT, numChunks int, encoding promchunk.Encoding) (ChunkStore, model.Time) {
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
	metric := model.Metric{
		model.MetricNameLabel: "foo",
	}
	pc, err := promchunk.NewForEncoding(encoding)
	require.NoError(t, err)
	for i := mint; i.Before(maxt); i = i.Add(step) {
		pcs, err := pc.Add(model.SamplePair{
			Timestamp: i,
			Value:     model.SampleValue(float64(i)),
		})
		require.NoError(t, err)
		require.Len(t, pcs, 1)
		pc = pcs[0]
	}
	return chunk.NewChunk(userID, fp, metric, pc, mint, maxt)
}

func testQuery(t require.TestingT, queryable storage.Queryable, end model.Time) *promql.Result {
	from, through, step := time.Unix(0, 0), end.Time(), sampleRate*4
	engine := promql.NewEngine(util.Logger, nil, 10, 1*time.Minute)
	query, err := engine.NewRangeQuery(queryable, "rate(foo[1m])", from, through, step)
	require.NoError(t, err)

	r := query.Exec(context.Background())
	m, err := r.Matrix()
	require.NoError(t, err)
	require.Len(t, m, 1)

	series := m[0]
	assert.Equal(t, labels.Labels{}, series.Metric)
	assert.Equal(t, int(through.Sub(from)/step), len(series.Points))
	ts := int64(step / time.Millisecond)
	for _, point := range series.Points {
		assert.Equal(t, ts, point.T)
		assert.Equal(t, 1000.0, point.V)
		ts += int64(step / time.Millisecond)
	}
	return r
}
