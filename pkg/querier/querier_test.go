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

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/mock"

	"github.com/cortexproject/cortex/pkg/chunk/purger"
	"github.com/cortexproject/cortex/pkg/util/validation"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/assert"
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

	const chunks = 24

	// Generate TSDB head with the same samples as makeMockChunkStore.
	db := mockTSDB(t, model.Time(0), int(chunks*samplesPerChunk), sampleRate, chunkOffset, int(samplesPerChunk))

	for _, query := range queries {
		for _, encoding := range encodings {
			for _, streaming := range []bool{false, true} {
				for _, iterators := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/streaming=%t/iterators=%t", query.query, encoding.name, streaming, iterators), func(t *testing.T) {
						cfg.IngesterStreaming = streaming
						cfg.Iterators = iterators

						chunkStore, through := makeMockChunkStore(t, chunks, encoding.e)
						distributor := mockDistibutorFor(t, chunkStore, through)

						overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
						require.NoError(t, err)

						queryables := []QueryableWithFilter{UseAlwaysQueryable(NewChunkStoreQueryable(cfg, chunkStore)), UseAlwaysQueryable(db)}
						queryable, _ := New(cfg, overrides, distributor, queryables, purger.NewTombstonesLoader(nil, nil), nil)
						testQuery(t, queryable, through, query)
					})
				}
			}
		}
	}
}

func mockTSDB(t *testing.T, mint model.Time, samples int, step, chunkOffset time.Duration, samplesPerChunk int) storage.Queryable {
	dir, err := ioutil.TempDir("", "tsdb")
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})

	opts := tsdb.DefaultOptions()
	opts.WALSegmentSize = -1 // Disable
	opts.NoLockfile = true

	// We use TSDB head only. By using full TSDB DB, and appending samples to it, closing it would cause unnecessary HEAD compaction, which slows down the test.
	head, err := tsdb.NewHead(nil, nil, nil, tsdb.ExponentialBlockRanges(opts.MinBlockDuration, 10, 3)[0], dir, chunkenc.NewPool(), opts.StripeSize, opts.SeriesLifecycleCallback)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = head.Close()
	})

	app := head.Appender(context.Background())

	l := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo"},
	}

	cnt := 0
	chunkStartTs := mint
	ts := chunkStartTs
	for i := 0; i < samples; i++ {
		_, err := app.Add(l, int64(ts), float64(ts))
		require.NoError(t, err)
		cnt++

		ts = ts.Add(step)

		if cnt%samplesPerChunk == 0 {
			// Simulate next chunk, restart timestamp.
			chunkStartTs = chunkStartTs.Add(chunkOffset)
			ts = chunkStartTs
		}
	}

	require.NoError(t, app.Commit())
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return tsdb.NewBlockQuerier(head, mint, maxt)
	})
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

				overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
				require.NoError(t, err)

				queryable, _ := New(cfg, overrides, distributor, []QueryableWithFilter{UseAlwaysQueryable(NewChunkStoreQueryable(cfg, chunkStore))}, purger.NewTombstonesLoader(nil, nil), nil)
				query, err := engine.NewRangeQuery(queryable, "dummy", c.mint, c.maxt, 1*time.Minute)
				require.NoError(t, err)

				ctx := user.InjectOrgID(context.Background(), "0")
				r := query.Exec(ctx)
				_, err = r.Matrix()

				if c.hitIngester {
					// If the ingester was hit, the distributor always returns errDistributorError. Prometheus
					// wrap any Select() error into "expanding series", so we do wrap it as well to have a match.
					require.Error(t, err)
					require.Equal(t, errors.Wrap(errDistributorError, "expanding series").Error(), err.Error())
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

				overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
				require.NoError(t, err)

				queryable, _ := New(cfg, overrides, distributor, []QueryableWithFilter{UseAlwaysQueryable(chunkStore)}, purger.NewTombstonesLoader(nil, nil), nil)
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

func TestQuerier_ValidateQueryTimeRange(t *testing.T) {
	const maxQueryLength = 30 * 24 * time.Hour

	tests := map[string]struct {
		query          string
		queryStartTime time.Time
		queryEndTime   time.Time
		expected       error
	}{
		"should allow query on short time range and rate time window close to the limit": {
			query:          "rate(foo[29d])",
			queryStartTime: time.Now().Add(-time.Hour),
			queryEndTime:   time.Now(),
			expected:       nil,
		},
		"should allow query on large time range close to the limit and short rate time window": {
			query:          "rate(foo[1m])",
			queryStartTime: time.Now().Add(-maxQueryLength).Add(time.Hour),
			queryEndTime:   time.Now(),
			expected:       nil,
		},
		"should forbid query on short time range and rate time window over the limit": {
			query:          "rate(foo[31d])",
			queryStartTime: time.Now().Add(-time.Hour),
			queryEndTime:   time.Now(),
			expected:       errors.New("expanding series: the query time range exceeds the limit (query length: 745h0m0s, limit: 720h0m0s)"),
		},
		"should forbid query on large time range over the limit and short rate time window": {
			query:          "rate(foo[1m])",
			queryStartTime: time.Now().Add(-maxQueryLength).Add(-time.Hour),
			queryEndTime:   time.Now(),
			expected:       errors.New("expanding series: the query time range exceeds the limit (query length: 721h1m0s, limit: 720h0m0s)"),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			var cfg Config
			flagext.DefaultValues(&cfg)

			limits := defaultLimitsConfig()
			limits.MaxQueryLength = maxQueryLength
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			// We don't need to query any data for this test, so an empty store is fine.
			chunkStore := &emptyChunkStore{}
			distributor := &emptyDistributor{}

			queryables := []QueryableWithFilter{UseAlwaysQueryable(NewChunkStoreQueryable(cfg, chunkStore))}
			queryable, _ := New(cfg, overrides, distributor, queryables, purger.NewTombstonesLoader(nil, nil), nil)

			// Create the PromQL engine to execute the query.
			engine := promql.NewEngine(promql.EngineOpts{
				Logger:             util.Logger,
				ActiveQueryTracker: nil,
				MaxSamples:         1e6,
				Timeout:            1 * time.Minute,
			})

			query, err := engine.NewRangeQuery(queryable, testData.query, testData.queryStartTime, testData.queryEndTime, time.Minute)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "test")
			r := query.Exec(ctx)

			if testData.expected != nil {
				require.NotNil(t, r.Err)
				assert.Equal(t, testData.expected.Error(), r.Err.Error())
			} else {
				assert.Nil(t, r.Err)
			}
		})
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

	result := &mockDistributor{}
	result.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(matrix, nil)
	result.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryStreamResponse{Chunkseries: []client.TimeSeriesChunk{tsc}}, nil)
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

func (m *errDistributor) MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error) {
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

type emptyDistributor struct{}

func (d *emptyDistributor) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	return nil, nil
}

func (d *emptyDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	return &client.QueryStreamResponse{}, nil
}

func (d *emptyDistributor) LabelValuesForLabelName(context.Context, model.LabelName) ([]string, error) {
	return nil, nil
}

func (d *emptyDistributor) LabelNames(context.Context) ([]string, error) {
	return nil, nil
}

func (d *emptyDistributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return nil, nil
}

func (d *emptyDistributor) MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error) {
	return nil, nil
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

				overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
				require.NoError(t, err)

				queryable, _ := New(cfg, overrides, distributor, []QueryableWithFilter{UseAlwaysQueryable(NewChunkStoreQueryable(cfg, chunkStore))}, purger.NewTombstonesLoader(nil, nil), nil)
				query, err := engine.NewRangeQuery(queryable, "dummy", c.mint, c.maxt, 1*time.Minute)
				require.NoError(t, err)

				ctx := user.InjectOrgID(context.Background(), "0")
				r := query.Exec(ctx)
				_, err = r.Matrix()

				if c.hitIngester {
					// If the ingester was hit, the distributor always returns errDistributorError. Prometheus
					// wrap any Select() error into "expanding series", so we do wrap it as well to have a match.
					require.Error(t, err)
					require.Equal(t, errors.Wrap(errDistributorError, "expanding series").Error(), err.Error())
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

func TestUseAlwaysQueryable(t *testing.T) {
	m := &mockQueryableWithFilter{}
	qwf := UseAlwaysQueryable(m)

	require.True(t, qwf.UseQueryable(time.Now(), 0, 0))
	require.False(t, m.useQueryableCalled)
}

func TestUseBeforeTimestamp(t *testing.T) {
	m := &mockQueryableWithFilter{}
	now := time.Now()
	qwf := UseBeforeTimestampQueryable(m, now.Add(-1*time.Hour))

	require.False(t, qwf.UseQueryable(now, util.TimeToMillis(now.Add(-5*time.Minute)), util.TimeToMillis(now)))
	require.False(t, m.useQueryableCalled)

	require.False(t, qwf.UseQueryable(now, util.TimeToMillis(now.Add(-1*time.Hour)), util.TimeToMillis(now)))
	require.False(t, m.useQueryableCalled)

	require.True(t, qwf.UseQueryable(now, util.TimeToMillis(now.Add(-1*time.Hour).Add(-time.Millisecond)), util.TimeToMillis(now)))
	require.False(t, m.useQueryableCalled) // UseBeforeTimestampQueryable wraps Queryable, and not QueryableWithFilter.
}

func TestStoreQueryable(t *testing.T) {
	m := &mockQueryableWithFilter{}
	now := time.Now()
	sq := storeQueryable{m, time.Hour}

	require.False(t, sq.UseQueryable(now, util.TimeToMillis(now.Add(-5*time.Minute)), util.TimeToMillis(now)))
	require.False(t, m.useQueryableCalled)

	require.False(t, sq.UseQueryable(now, util.TimeToMillis(now.Add(-1*time.Hour).Add(time.Millisecond)), util.TimeToMillis(now)))
	require.False(t, m.useQueryableCalled)

	require.True(t, sq.UseQueryable(now, util.TimeToMillis(now.Add(-1*time.Hour)), util.TimeToMillis(now)))
	require.True(t, m.useQueryableCalled) // storeQueryable wraps QueryableWithFilter, so it must call its UseQueryable method.
}

type mockQueryableWithFilter struct {
	useQueryableCalled bool
}

func (m *mockQueryableWithFilter) Querier(_ context.Context, _, _ int64) (storage.Querier, error) {
	return nil, nil
}

func (m *mockQueryableWithFilter) UseQueryable(_ time.Time, _, _ int64) bool {
	m.useQueryableCalled = true
	return true
}

func defaultLimitsConfig() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	return limits
}
