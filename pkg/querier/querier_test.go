package querier

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	promchunk "github.com/cortexproject/cortex/pkg/prom1/storage/local/chunk"
	"github.com/cortexproject/cortex/pkg/querier/batch"
	"github.com/cortexproject/cortex/pkg/querier/iterators"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/wire"
	"github.com/weaveworks/common/user"
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
	step     time.Duration
}

var (
	testcases = []struct {
		name string
		f    chunkIteratorFunc
	}{
		{"matrixes", mergeChunks},
		{"iterators", iterators.NewChunkMergeIterator},
		{"batches", batch.NewChunkMergeIterator},
	}

	encodings = []struct {
		name string
		e    promchunk.Encoding
	}{
		{"DoubleDelta", promchunk.DoubleDelta},
		{"Varbit", promchunk.Varbit},
		{"Bigchunk", promchunk.Bigchunk},
	}

	queries = []query{
		// Windowed rates with small step;  This will cause BufferedIterator to read
		// all the samples.
		{
			query:  "rate(foo[1m])",
			step:   sampleRate * 4,
			labels: labels.Labels{},
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from) / step)
			},
			expected: func(t int64) (int64, float64) {
				return t + int64((sampleRate*4)/time.Millisecond), 1000.0
			},
		},

		// Very simple single-point gets, with low step.  Performance should be
		// similar to above.
		{
			query: "foo",
			step:  sampleRate * 4,
			labels: labels.Labels{
				labels.Label{"__name__", "foo"},
			},
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from)/step) + 1
			},
			expected: func(t int64) (int64, float64) {
				return t, float64(t)
			},
		},

		// Rates with large step; excersise everything.
		{
			query:  "rate(foo[1m])",
			step:   sampleRate * 4 * 10,
			labels: labels.Labels{},
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from) / step)
			},
			expected: func(t int64) (int64, float64) {
				return t + int64((sampleRate*4)/time.Millisecond)*10, 1000.0
			},
		},

		// Single points gets with large step; excersise Seek performance.
		{
			query: "foo",
			step:  sampleRate * 4 * 10,
			labels: labels.Labels{
				labels.Label{"__name__", "foo"},
			},
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from)/step) + 1
			},
			expected: func(t int64) (int64, float64) {
				return t, float64(t)
			},
		},
	}
)

func TestQuerier(t *testing.T) {
	var cfg Config
	util.DefaultValues(&cfg)

	for _, query := range queries {
		for _, encoding := range encodings {
			for _, streaming := range []bool{false, true} {
				for _, iterators := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/streaming=%t/iterators=%t", query.query, encoding.name, streaming, iterators), func(t *testing.T) {
						cfg.IngesterStreaming = streaming
						cfg.Iterators = iterators
						cfg.metricsRegisterer = nil

						chunkStore, through := makeMockChunkStore(t, 24, encoding.e)
						distributor := mockDistibutorFor(t, chunkStore, through)

						queryable, _ := New(cfg, distributor, chunkStore)
						testQuery(t, queryable, through, query)
					})
				}
			}
		}
	}
}

// mockDistibutorFor duplicates the chunks in the mockChunkStore into the mockDistributor
// so we can test everything is dedupe correctly.
func mockDistibutorFor(t *testing.T, cs mockChunkStore, through model.Time) *mockDistributor {
	chunks, err := chunkcompat.ToChunks(cs.chunks)
	require.NoError(t, err)

	tsc := client.TimeSeriesChunk{
		Labels: []client.LabelPair{{Name: wire.Bytes(model.MetricNameLabel), Value: wire.Bytes("foo")}},
		Chunks: chunks,
	}
	matrix, err := chunk.ChunksToMatrix(context.Background(), cs.chunks, 0, through)
	require.NoError(t, err)

	result := &mockDistributor{
		m: matrix,
		r: []client.TimeSeriesChunk{tsc},
	}
	return result
}

func testQuery(t require.TestingT, queryable storage.Queryable, end model.Time, q query) *promql.Result {
	from, through, step := time.Unix(0, 0), end.Time(), q.step
	engine := promql.NewEngine(util.Logger, nil, 10, 1*time.Minute)
	query, err := engine.NewRangeQuery(queryable, q.query, from, through, step)
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), "0")
	r := query.Exec(ctx)
	m, err := r.Matrix()
	require.NoError(t, err)

	require.Len(t, m, 1)
	series := m[0]
	require.Equal(t, q.labels, series.Metric)
	require.Equal(t, q.samples(from, through, step), len(series.Points))
	var ts int64
	for i, point := range series.Points {
		expectedTime, expectedValue := q.expected(ts)
		require.Equal(t, expectedTime, point.T, strconv.Itoa(i))
		require.Equal(t, expectedValue, point.V, strconv.Itoa(i))
		ts += int64(step / time.Millisecond)
	}
	return r
}
