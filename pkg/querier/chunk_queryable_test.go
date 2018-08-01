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
	"github.com/weaveworks/cortex/pkg/querier/batch"
	"github.com/weaveworks/cortex/pkg/util"
)

const (
	userID          = "userID"
	fp              = 1
	chunkOffset     = 1 * time.Hour
	chunkLength     = 3 * time.Hour
	sampleRate      = 15 * time.Second
	samplesPerChunk = chunkLength / sampleRate
)

type query struct {
	query    string
	labels   labels.Labels
	samples  func(from, through time.Time, step time.Duration) int
	expected func(t int64) (int64, float64)
}

var (
	queryables = []struct {
		name string
		f    func(ChunkStore) storage.Queryable
	}{
		{"matrixes", newChunkQueryable},
		{"iterators", newIterChunkQueryable(newChunkMergeIterator)},
		{"batches", newIterChunkQueryable(batch.NewChunkMergeIterator)},
	}

	encodings = []struct {
		name string
		e    promchunk.Encoding
	}{
		{"DoubleDelta", promchunk.DoubleDelta},
		//		{"Varbit", promchunk.Varbit},
	}

	queries = []query{
		//		{
		//			query: "foo",
		//			labels: labels.Labels{
		//				labels.Label{"__name__", "foo"},
		//			},
		//			samples: func(from, through time.Time, step time.Duration) int {
		//				return int(through.Sub(from)/step) + 1
		//			},
		//			expected: func(t int64) (int64, float64) {
		//				return t, float64(t)
		//			},
		//		},

		{
			query:  "rate(foo[1m])",
			labels: labels.Labels{},
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from) / step)
			},
			expected: func(t int64) (int64, float64) {
				return t + int64((sampleRate*4)/time.Millisecond), 1000.0
			},
		},
	}
)

func TestChunkQueryable(t *testing.T) {
	for _, queryable := range queryables {
		for _, encoding := range encodings {
			for _, query := range queries {
				t.Run(fmt.Sprintf("%s/%s/%s", queryable.name, encoding.name, query.query), func(t *testing.T) {
					store, from := makeMockChunkStore(t, 24*15, encoding.e)
					queryable := queryable.f(store)
					testQuery(t, queryable, from, query)
				})
			}
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

func testQuery(t require.TestingT, queryable storage.Queryable, end model.Time, q query) *promql.Result {
	from, through, step := time.Unix(0, 0), end.Time(), sampleRate*4
	engine := promql.NewEngine(util.Logger, nil, 10, 1*time.Minute)
	query, err := engine.NewRangeQuery(queryable, q.query, from, through, step)
	require.NoError(t, err)

	r := query.Exec(context.Background())
	m, err := r.Matrix()
	require.NoError(t, err)

	require.Len(t, m, 1)
	series := m[0]
	assert.Equal(t, q.labels, series.Metric)
	//assert.Equal(t, q.samples(from, through, step), len(series.Points))
	//var ts int64
	//for _, point := range series.Points {
	//	expectedTime, expectedValue := q.expected(ts)
	//	//assert.Equal(t, expectedTime, point.T)
	//	//assert.Equal(t, expectedValue, point.V)
	//	ts += int64(step / time.Millisecond)
	//}
	return r
}
