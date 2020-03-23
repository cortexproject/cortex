package querier

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk/purger"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
	"github.com/cortexproject/cortex/pkg/querier/batch"
	"github.com/cortexproject/cortex/pkg/querier/iterators"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/flagext"
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
				labels.Label{Name: model.MetricNameLabel, Value: "foo"},
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
				labels.Label{Name: model.MetricNameLabel, Value: "foo"},
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
	flagext.DefaultValues(&cfg)

	for _, query := range queries {
		for _, encoding := range encodings {
			for _, streaming := range []bool{false, true} {
				for _, iterators := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/streaming=%t/iterators=%t", query.query, encoding.name, streaming, iterators), func(t *testing.T) {
						cfg.IngesterStreaming = streaming
						cfg.Iterators = iterators

						chunkStore, through := makeMockChunkStore(t, 24, encoding.e)
						distributor := mockDistibutorFor(t, chunkStore, through)

						queryable, _ := New(cfg, distributor, NewChunkStoreQueryable(cfg, chunkStore), purger.NewTombstonesLoader(nil), nil)
						testQuery(t, queryable, through, query)
					})
				}
			}
		}
	}
}

func TestNoHistoricalQueryToIngester(t *testing.T) {
	testCases := []struct {
		name                 string
		mint, maxt           time.Time
		hitIngester          bool
		queryIngestersWithin time.Duration
	}{
		{
			name:                 "hit-test1",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now().Add(1 * time.Hour),
			hitIngester:          true,
			queryIngestersWithin: 1 * time.Hour,
		},
		{
			name:                 "hit-test2",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now().Add(-59 * time.Minute),
			hitIngester:          true,
			queryIngestersWithin: 1 * time.Hour,
		},
		{ // Skipping ingester is disabled.
			name:                 "hit-test2",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now().Add(-50 * time.Minute),
			hitIngester:          true,
			queryIngestersWithin: 0,
		},
		{
			name:                 "dont-hit-test1",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now().Add(-100 * time.Minute),
			hitIngester:          false,
			queryIngestersWithin: 1 * time.Hour,
		},
		{
			name:                 "dont-hit-test2",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now().Add(-61 * time.Minute),
			hitIngester:          false,
			queryIngestersWithin: 1 * time.Hour,
		},
	}

	dir, err := ioutil.TempDir("", t.Name())
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)
	queryTracker := promql.NewActiveQueryTracker(dir, 10, util.Logger)

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             util.Logger,
		ActiveQueryTracker: queryTracker,
		MaxSamples:         1e6,
		Timeout:            1 * time.Minute,
	})
	cfg := Config{}
	for _, ingesterStreaming := range []bool{true, false} {
		cfg.IngesterStreaming = ingesterStreaming
		for _, c := range testCases {
			cfg.QueryIngestersWithin = c.queryIngestersWithin
			t.Run(fmt.Sprintf("IngesterStreaming=%t,test=%s", cfg.IngesterStreaming, c.name), func(t *testing.T) {
				chunkStore, _ := makeMockChunkStore(t, 24, encodings[0].e)
				distributor := &errDistributor{}

				queryable, _ := New(cfg, distributor, NewChunkStoreQueryable(cfg, chunkStore), purger.NewTombstonesLoader(nil), nil)
				query, err := engine.NewRangeQuery(queryable, "dummy", c.mint, c.maxt, 1*time.Minute)
				require.NoError(t, err)

				ctx := user.InjectOrgID(context.Background(), "0")
				r := query.Exec(ctx)
				_, err = r.Matrix()

				if c.hitIngester {
					// If the ingester was hit, the distributor always returns errDistributorError.
					require.Error(t, err)
					require.Equal(t, errDistributorError.Error(), err.Error())
				} else {
					// If the ingester was hit, there would have been an error from errDistributor.
					require.NoError(t, err)
				}
			})
		}
	}

}

func TestNoFutureQueries(t *testing.T) {
	testCases := []struct {
		name               string
		mint, maxt         time.Time
		hitStores          bool
		maxQueryIntoFuture time.Duration
	}{
		{
			name:               "hit-test1",
			mint:               time.Now().Add(-5 * time.Hour),
			maxt:               time.Now().Add(1 * time.Hour),
			hitStores:          true,
			maxQueryIntoFuture: 10 * time.Minute,
		},
		{
			name:               "hit-test2",
			mint:               time.Now().Add(-5 * time.Hour),
			maxt:               time.Now().Add(-59 * time.Minute),
			hitStores:          true,
			maxQueryIntoFuture: 10 * time.Minute,
		},
		{ // Skipping stores is disabled.
			name:               "max-query-into-future-disabled",
			mint:               time.Now().Add(500 * time.Hour),
			maxt:               time.Now().Add(5000 * time.Hour),
			hitStores:          true,
			maxQueryIntoFuture: 0,
		},
		{ // Still hit because of staleness.
			name:               "hit-test3",
			mint:               time.Now().Add(12 * time.Minute),
			maxt:               time.Now().Add(60 * time.Minute),
			hitStores:          true,
			maxQueryIntoFuture: 10 * time.Minute,
		},
		{
			name:               "dont-hit-test1",
			mint:               time.Now().Add(100 * time.Minute),
			maxt:               time.Now().Add(5 * time.Hour),
			hitStores:          false,
			maxQueryIntoFuture: 10 * time.Minute,
		},
		{
			name:               "dont-hit-test2",
			mint:               time.Now().Add(16 * time.Minute),
			maxt:               time.Now().Add(60 * time.Minute),
			hitStores:          false,
			maxQueryIntoFuture: 10 * time.Minute,
		},
	}

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:     util.Logger,
		MaxSamples: 1e6,
		Timeout:    1 * time.Minute,
	})

	cfg := Config{}
	flagext.DefaultValues(&cfg)

	for _, ingesterStreaming := range []bool{true, false} {
		cfg.IngesterStreaming = ingesterStreaming
		for _, c := range testCases {
			cfg.MaxQueryIntoFuture = c.maxQueryIntoFuture
			t.Run(fmt.Sprintf("IngesterStreaming=%t,test=%s", cfg.IngesterStreaming, c.name), func(t *testing.T) {
				chunkStore := &errChunkStore{}
				distributor := &errDistributor{}

				queryable, _ := New(cfg, distributor, chunkStore, purger.NewTombstonesLoader(nil), nil)
				query, err := engine.NewRangeQuery(queryable, "dummy", c.mint, c.maxt, 1*time.Minute)
				require.NoError(t, err)

				ctx := user.InjectOrgID(context.Background(), "0")
				r := query.Exec(ctx)
				_, err = r.Matrix()

				if c.hitStores {
					// If the ingester was hit, the distributor always returns errDistributorError.
					require.Error(t, err)
					require.Equal(t, errDistributorError.Error(), err.Error())
				} else {
					// If the ingester was hit, there would have been an error from errDistributor.
					require.NoError(t, err)
				}
			})
		}
	}
}

// mockDistibutorFor duplicates the chunks in the mockChunkStore into the mockDistributor
// so we can test everything is dedupe correctly.
func mockDistibutorFor(t *testing.T, cs mockChunkStore, through model.Time) *mockDistributor {
	chunks, err := chunkcompat.ToChunks(cs.chunks)
	require.NoError(t, err)

	tsc := client.TimeSeriesChunk{
		Labels: []client.LabelAdapter{{Name: model.MetricNameLabel, Value: "foo"}},
		Chunks: chunks,
	}
	matrix, err := chunk.ChunksToMatrix(context.Background(), cs.chunks, 0, through)
	require.NoError(t, err)

	result := &mockDistributor{
		m: matrix,
		r: &client.QueryStreamResponse{Chunkseries: []client.TimeSeriesChunk{tsc}},
	}
	return result
}

func testQuery(t testing.TB, queryable storage.Queryable, end model.Time, q query) *promql.Result {
	dir, err := ioutil.TempDir("", "test_query")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)
	queryTracker := promql.NewActiveQueryTracker(dir, 10, util.Logger)

	from, through, step := time.Unix(0, 0), end.Time(), q.step
	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             util.Logger,
		ActiveQueryTracker: queryTracker,
		MaxSamples:         1e6,
		Timeout:            1 * time.Minute,
	})
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

type errChunkStore struct {
}

func (m *errChunkStore) Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {
	return nil, errDistributorError
}

func (m *errChunkStore) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return storage.NoopQuerier(), errDistributorError
}

type errDistributor struct{}

var errDistributorError = fmt.Errorf("errDistributorError")

func (m *errDistributor) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	return nil, errDistributorError
}
func (m *errDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	return nil, errDistributorError
}
func (m *errDistributor) LabelValuesForLabelName(context.Context, model.LabelName) ([]string, error) {
	return nil, errDistributorError
}
func (m *errDistributor) LabelNames(context.Context) ([]string, error) {
	return nil, errDistributorError
}
func (m *errDistributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return nil, errDistributorError
}

type emptyChunkStore struct {
	sync.Mutex
	called bool
}

func (c *emptyChunkStore) Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {
	c.Lock()
	defer c.Unlock()
	c.called = true
	return nil, nil
}

func (c *emptyChunkStore) IsCalled() bool {
	c.Lock()
	defer c.Unlock()
	return c.called
}

func TestShortTermQueryToLTS(t *testing.T) {
	testCases := []struct {
		name                 string
		mint, maxt           time.Time
		hitIngester          bool
		hitLTS               bool
		queryIngestersWithin time.Duration
		queryStoreAfter      time.Duration
	}{
		{
			name:                 "hit only ingester",
			mint:                 time.Now().Add(-5 * time.Minute),
			maxt:                 time.Now(),
			hitIngester:          true,
			hitLTS:               false,
			queryIngestersWithin: 1 * time.Hour,
			queryStoreAfter:      time.Hour,
		},
		{
			name:                 "hit both",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now(),
			hitIngester:          true,
			hitLTS:               true,
			queryIngestersWithin: 1 * time.Hour,
			queryStoreAfter:      time.Hour,
		},
		{
			name:                 "hit only storage",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now().Add(-2 * time.Hour),
			hitIngester:          false,
			hitLTS:               true,
			queryIngestersWithin: 1 * time.Hour,
			queryStoreAfter:      0,
		},
	}

	dir, err := ioutil.TempDir("", t.Name())
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)
	queryTracker := promql.NewActiveQueryTracker(dir, 10, util.Logger)

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             util.Logger,
		ActiveQueryTracker: queryTracker,
		MaxSamples:         1e6,
		Timeout:            1 * time.Minute,
	})

	cfg := Config{}
	flagext.DefaultValues(&cfg)

	for _, ingesterStreaming := range []bool{true, false} {
		cfg.IngesterStreaming = ingesterStreaming
		for _, c := range testCases {
			cfg.QueryIngestersWithin = c.queryIngestersWithin
			cfg.QueryStoreAfter = c.queryStoreAfter
			t.Run(fmt.Sprintf("IngesterStreaming=%t,test=%s", cfg.IngesterStreaming, c.name), func(t *testing.T) {
				chunkStore := &emptyChunkStore{}
				distributor := &errDistributor{}

				queryable, _ := New(cfg, distributor, NewChunkStoreQueryable(cfg, chunkStore), purger.NewTombstonesLoader(nil), nil)
				query, err := engine.NewRangeQuery(queryable, "dummy", c.mint, c.maxt, 1*time.Minute)
				require.NoError(t, err)

				ctx := user.InjectOrgID(context.Background(), "0")
				r := query.Exec(ctx)
				_, err = r.Matrix()

				if c.hitIngester {
					// If the ingester was hit, the distributor always returns errDistributorError.
					require.Error(t, err)
					require.Equal(t, errDistributorError.Error(), err.Error())
				} else {
					// If the ingester was hit, there would have been an error from errDistributor.
					require.NoError(t, err)
				}

				// Verify if the test did/did not hit the LTS
				time.Sleep(30 * time.Millisecond) // NOTE: Since this is a lazy querier there is a race condition between the response and chunk store being called
				require.Equal(t, c.hitLTS, chunkStore.IsCalled())
			})
		}
	}

}
