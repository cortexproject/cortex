package querier

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
	"github.com/cortexproject/cortex/pkg/purger"
	"github.com/cortexproject/cortex/pkg/querier/batch"
	"github.com/cortexproject/cortex/pkg/querier/iterators"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	chunkOffset     = 1 * time.Hour
	chunkLength     = 3 * time.Hour
	sampleRate      = 15 * time.Second
	samplesPerChunk = chunkLength / sampleRate
)

type wrappedQuerier struct {
	storage.Querier
	selectCallsArgs [][]interface{}
}

func (q *wrappedQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	q.selectCallsArgs = append(q.selectCallsArgs, []interface{}{sortSeries, hints, matchers})
	return q.Querier.Select(sortSeries, hints, matchers...)
}

type wrappedSampleAndChunkQueryable struct {
	QueryableWithFilter
	queriers []*wrappedQuerier
}

func (q *wrappedSampleAndChunkQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	querier, err := q.QueryableWithFilter.Querier(ctx, mint, maxt)
	wQuerier := &wrappedQuerier{Querier: querier}
	q.queriers = append(q.queriers, wQuerier)
	return wQuerier, err
}

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
		{"PrometheusXorChunk", promchunk.PrometheusXorChunk},
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

func TestShouldSortSeriesIfQueryingMultipleQueryables(t *testing.T) {
	start := time.Now().Add(-2 * time.Hour)
	end := time.Now()
	ctx := user.InjectOrgID(context.Background(), "0")
	var cfg Config
	flagext.DefaultValues(&cfg)
	overrides, err := validation.NewOverrides(DefaultLimitsConfig(), nil)
	const chunks = 1
	require.NoError(t, err)

	labelsSets := []labels.Labels{
		{
			{Name: model.MetricNameLabel, Value: "foo"},
			{Name: "order", Value: "1"},
		},
		{
			{Name: model.MetricNameLabel, Value: "foo"},
			{Name: "order", Value: "2"},
		},
	}

	db, samples := mockTSDB(t, labelsSets, model.Time(start.Unix()*1000), int(chunks*samplesPerChunk), sampleRate, chunkOffset, int(samplesPerChunk))
	samplePairs := []model.SamplePair{}

	for _, s := range samples {
		samplePairs = append(samplePairs, model.SamplePair{Timestamp: model.Time(s.TimestampMs), Value: model.SampleValue(s.Value)})
	}

	distributor := &MockDistributor{}

	unorderedResponse := client.QueryStreamResponse{
		Timeseries: []cortexpb.TimeSeries{
			{
				Labels: []cortexpb.LabelAdapter{
					{Name: model.MetricNameLabel, Value: "foo"},
					{Name: "order", Value: "2"},
				},
				Samples: samples,
			},
			{
				Labels: []cortexpb.LabelAdapter{
					{Name: model.MetricNameLabel, Value: "foo"},
					{Name: "order", Value: "1"},
				},
				Samples: samples,
			},
		},
	}

	unorderedResponseMatrix := model.Matrix{
		{
			Metric: util.LabelsToMetric(cortexpb.FromLabelAdaptersToLabels(unorderedResponse.Timeseries[0].Labels)),
			Values: samplePairs,
		},
		{
			Metric: util.LabelsToMetric(cortexpb.FromLabelAdaptersToLabels(unorderedResponse.Timeseries[1].Labels)),
			Values: samplePairs,
		},
	}

	distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&unorderedResponse, nil)
	distributor.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(unorderedResponseMatrix, nil)
	distributorQueryableStreaming := newDistributorQueryable(distributor, true, cfg.IngesterMetadataStreaming, batch.NewChunkMergeIterator, cfg.QueryIngestersWithin, cfg.QueryStoreForLabels)
	distributorQueryable := newDistributorQueryable(distributor, false, cfg.IngesterMetadataStreaming, batch.NewChunkMergeIterator, cfg.QueryIngestersWithin, cfg.QueryStoreForLabels)

	tCases := []struct {
		name                 string
		distributorQueryable QueryableWithFilter
		storeQueriables      []QueryableWithFilter
		sorted               bool
	}{
		{
			name:                 "should sort if querying 2 queryables",
			distributorQueryable: distributorQueryableStreaming,
			storeQueriables:      []QueryableWithFilter{UseAlwaysQueryable(db)},
			sorted:               true,
		},
		{
			name:                 "should not sort if querying only ingesters",
			distributorQueryable: distributorQueryableStreaming,
			storeQueriables:      []QueryableWithFilter{UseBeforeTimestampQueryable(db, start.Add(-1*time.Hour))},
			sorted:               false,
		},
		{
			name:                 "should not sort if querying only stores",
			distributorQueryable: UseBeforeTimestampQueryable(distributorQueryableStreaming, start.Add(-1*time.Hour)),
			storeQueriables:      []QueryableWithFilter{UseAlwaysQueryable(db)},
			sorted:               false,
		},
		{
			name:                 "should sort if querying 2 queryables with streaming off",
			distributorQueryable: distributorQueryable,
			storeQueriables:      []QueryableWithFilter{UseAlwaysQueryable(db)},
			sorted:               true,
		},
		{
			name:                 "should not sort if querying only ingesters with streaming off",
			distributorQueryable: distributorQueryable,
			storeQueriables:      []QueryableWithFilter{UseBeforeTimestampQueryable(db, start.Add(-1*time.Hour))},
			sorted:               false,
		},
		{
			name:                 "should not sort if querying only stores with streaming off",
			distributorQueryable: UseBeforeTimestampQueryable(distributorQueryable, start.Add(-1*time.Hour)),
			storeQueriables:      []QueryableWithFilter{UseAlwaysQueryable(db)},
			sorted:               false,
		},
	}

	for _, tc := range tCases {
		t.Run(tc.name, func(t *testing.T) {
			wDistributorQueriable := &wrappedSampleAndChunkQueryable{QueryableWithFilter: tc.distributorQueryable}
			var wQueriables []QueryableWithFilter
			for _, queriable := range tc.storeQueriables {
				wQueriables = append(wQueriables, &wrappedSampleAndChunkQueryable{QueryableWithFilter: queriable})
			}
			queryable := NewQueryable(wDistributorQueriable, wQueriables, batch.NewChunkMergeIterator, cfg, overrides, purger.NewNoopTombstonesLoader())
			queryTracker := promql.NewActiveQueryTracker(t.TempDir(), 10, log.NewNopLogger())

			engine := promql.NewEngine(promql.EngineOpts{
				Logger:             log.NewNopLogger(),
				ActiveQueryTracker: queryTracker,
				MaxSamples:         1e6,
				Timeout:            1 * time.Minute,
			})

			query, err := engine.NewRangeQuery(queryable, nil, "foo", start, end, 1*time.Minute)
			r := query.Exec(ctx)

			require.NoError(t, err)
			require.Equal(t, 2, r.Value.(promql.Matrix).Len())

			for _, queryable := range append(wQueriables, wDistributorQueriable) {
				var wQueryable = queryable.(*wrappedSampleAndChunkQueryable)
				if wQueryable.UseQueryable(time.Now(), start.Unix()*1000, end.Unix()*1000) {
					require.Equal(t, tc.sorted, wQueryable.queriers[0].selectCallsArgs[0][0])
				}
			}
		})
	}
}

func TestQuerier(t *testing.T) {
	var cfg Config
	flagext.DefaultValues(&cfg)

	const chunks = 24

	// Generate TSDB head with the same samples as makeMockChunkStore.
	lset := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo"},
	}
	db, _ := mockTSDB(t, []labels.Labels{lset}, model.Time(0), int(chunks*samplesPerChunk), sampleRate, chunkOffset, int(samplesPerChunk))

	for _, query := range queries {
		for _, encoding := range encodings {
			for _, streaming := range []bool{false, true} {
				for _, iterators := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/streaming=%t/iterators=%t", query.query, encoding.name, streaming, iterators), func(t *testing.T) {
						cfg.IngesterStreaming = streaming
						cfg.Iterators = iterators

						chunkStore, through := makeMockChunkStore(t, chunks, encoding.e)
						distributor := mockDistibutorFor(t, chunkStore, through)

						overrides, err := validation.NewOverrides(DefaultLimitsConfig(), nil)
						require.NoError(t, err)

						queryables := []QueryableWithFilter{UseAlwaysQueryable(NewMockStoreQueryable(cfg, chunkStore)), UseAlwaysQueryable(db)}
						queryable, _, _ := New(cfg, overrides, distributor, queryables, purger.NewNoopTombstonesLoader(), nil, log.NewNopLogger())
						testRangeQuery(t, queryable, through, query)
					})
				}
			}
		}
	}
}

func mockTSDB(t *testing.T, labels []labels.Labels, mint model.Time, samples int, step, chunkOffset time.Duration, samplesPerChunk int) (storage.Queryable, []cortexpb.Sample) {
	opts := tsdb.DefaultHeadOptions()
	opts.ChunkDirRoot = t.TempDir()
	// We use TSDB head only. By using full TSDB DB, and appending samples to it, closing it would cause unnecessary HEAD compaction, which slows down the test.
	head, err := tsdb.NewHead(nil, nil, nil, nil, opts, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = head.Close()
	})

	app := head.Appender(context.Background())
	rSamples := []cortexpb.Sample{}

	for _, lset := range labels {
		cnt := 0
		chunkStartTs := mint
		ts := chunkStartTs
		for i := 0; i < samples; i++ {
			_, err := app.Append(0, lset, int64(ts), float64(ts))
			rSamples = append(rSamples, cortexpb.Sample{TimestampMs: int64(ts), Value: float64(ts)})
			require.NoError(t, err)
			cnt++

			ts = ts.Add(step)

			if cnt%samplesPerChunk == 0 {
				// Simulate next chunk, restart timestamp.
				chunkStartTs = chunkStartTs.Add(chunkOffset)
				ts = chunkStartTs
			}
		}
	}

	require.NoError(t, app.Commit())

	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return tsdb.NewBlockQuerier(head, mint, maxt)
	}), rSamples
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

	queryTracker := promql.NewActiveQueryTracker(t.TempDir(), 10, log.NewNopLogger())

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             log.NewNopLogger(),
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

				overrides, err := validation.NewOverrides(DefaultLimitsConfig(), nil)
				require.NoError(t, err)

				queryable, _, _ := New(cfg, overrides, distributor, []QueryableWithFilter{UseAlwaysQueryable(NewMockStoreQueryable(cfg, chunkStore))}, purger.NewNoopTombstonesLoader(), nil, log.NewNopLogger())
				query, err := engine.NewRangeQuery(queryable, nil, "dummy", c.mint, c.maxt, 1*time.Minute)
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

func TestQuerier_ValidateQueryTimeRange_MaxQueryIntoFuture(t *testing.T) {
	const engineLookbackDelta = 5 * time.Minute

	now := time.Now()

	tests := map[string]struct {
		maxQueryIntoFuture time.Duration
		queryStartTime     time.Time
		queryEndTime       time.Time
		expectedSkipped    bool
		expectedStartTime  time.Time
		expectedEndTime    time.Time
	}{
		"should manipulate query if end time is after the limit": {
			maxQueryIntoFuture: 10 * time.Minute,
			queryStartTime:     now.Add(-5 * time.Hour),
			queryEndTime:       now.Add(1 * time.Hour),
			expectedStartTime:  now.Add(-5 * time.Hour).Add(-engineLookbackDelta),
			expectedEndTime:    now.Add(10 * time.Minute),
		},
		"should not manipulate query if end time is far in the future but limit is disabled": {
			maxQueryIntoFuture: 0,
			queryStartTime:     now.Add(-5 * time.Hour),
			queryEndTime:       now.Add(100 * time.Hour),
			expectedStartTime:  now.Add(-5 * time.Hour).Add(-engineLookbackDelta),
			expectedEndTime:    now.Add(100 * time.Hour),
		},
		"should not manipulate query if end time is in the future but below the limit": {
			maxQueryIntoFuture: 10 * time.Minute,
			queryStartTime:     now.Add(-100 * time.Minute),
			queryEndTime:       now.Add(5 * time.Minute),
			expectedStartTime:  now.Add(-100 * time.Minute).Add(-engineLookbackDelta),
			expectedEndTime:    now.Add(5 * time.Minute),
		},
		"should skip executing a query outside the allowed time range": {
			maxQueryIntoFuture: 10 * time.Minute,
			queryStartTime:     now.Add(50 * time.Minute),
			queryEndTime:       now.Add(60 * time.Minute),
			expectedSkipped:    true,
		},
	}

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:        log.NewNopLogger(),
		MaxSamples:    1e6,
		Timeout:       1 * time.Minute,
		LookbackDelta: engineLookbackDelta,
	})

	cfg := Config{}
	flagext.DefaultValues(&cfg)

	for _, ingesterStreaming := range []bool{true, false} {
		cfg.IngesterStreaming = ingesterStreaming
		for name, c := range tests {
			cfg.MaxQueryIntoFuture = c.maxQueryIntoFuture
			t.Run(fmt.Sprintf("%s (ingester streaming enabled = %t)", name, cfg.IngesterStreaming), func(t *testing.T) {
				// We don't need to query any data for this test, so an empty store is fine.
				chunkStore := &emptyChunkStore{}
				distributor := &MockDistributor{}
				distributor.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{}, nil)
				distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryStreamResponse{}, nil)

				overrides, err := validation.NewOverrides(DefaultLimitsConfig(), nil)
				require.NoError(t, err)

				queryables := []QueryableWithFilter{UseAlwaysQueryable(NewMockStoreQueryable(cfg, chunkStore))}
				queryable, _, _ := New(cfg, overrides, distributor, queryables, purger.NewNoopTombstonesLoader(), nil, log.NewNopLogger())
				query, err := engine.NewRangeQuery(queryable, nil, "dummy", c.queryStartTime, c.queryEndTime, time.Minute)
				require.NoError(t, err)

				ctx := user.InjectOrgID(context.Background(), "0")
				r := query.Exec(ctx)
				require.Nil(t, r.Err)

				_, err = r.Matrix()
				require.Nil(t, err)

				if !c.expectedSkipped {
					// Assert on the time range of the actual executed query (5s delta).
					delta := float64(5000)
					require.Len(t, distributor.Calls, 1)
					assert.InDelta(t, util.TimeToMillis(c.expectedStartTime), int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), delta)
					assert.InDelta(t, util.TimeToMillis(c.expectedEndTime), int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), delta)
				} else {
					// Ensure no query has been executed executed (because skipped).
					assert.Len(t, distributor.Calls, 0)
				}
			})
		}
	}
}

func TestQuerier_ValidateQueryTimeRange_MaxQueryLength(t *testing.T) {
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

			limits := DefaultLimitsConfig()
			limits.MaxQueryLength = model.Duration(maxQueryLength)
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			// We don't need to query any data for this test, so an empty store is fine.
			chunkStore := &emptyChunkStore{}
			distributor := &emptyDistributor{}

			queryables := []QueryableWithFilter{UseAlwaysQueryable(NewMockStoreQueryable(cfg, chunkStore))}
			queryable, _, _ := New(cfg, overrides, distributor, queryables, purger.NewNoopTombstonesLoader(), nil, log.NewNopLogger())

			// Create the PromQL engine to execute the query.
			engine := promql.NewEngine(promql.EngineOpts{
				Logger:             log.NewNopLogger(),
				ActiveQueryTracker: nil,
				MaxSamples:         1e6,
				Timeout:            1 * time.Minute,
			})

			query, err := engine.NewRangeQuery(queryable, nil, testData.query, testData.queryStartTime, testData.queryEndTime, time.Minute)
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

func TestQuerier_ValidateQueryTimeRange_MaxQueryLookback(t *testing.T) {
	const (
		engineLookbackDelta = 5 * time.Minute
		thirtyDays          = 30 * 24 * time.Hour
	)

	now := time.Now()

	tests := map[string]struct {
		maxQueryLookback          model.Duration
		query                     string
		queryStartTime            time.Time
		queryEndTime              time.Time
		expectedSkipped           bool
		expectedQueryStartTime    time.Time
		expectedQueryEndTime      time.Time
		expectedMetadataStartTime time.Time
		expectedMetadataEndTime   time.Time
	}{
		"should not manipulate time range for a query on short time range and rate time window close to the limit": {
			maxQueryLookback:          model.Duration(thirtyDays),
			query:                     "rate(foo[29d])",
			queryStartTime:            now.Add(-time.Hour),
			queryEndTime:              now,
			expectedQueryStartTime:    now.Add(-time.Hour).Add(-29 * 24 * time.Hour),
			expectedQueryEndTime:      now,
			expectedMetadataStartTime: now.Add(-time.Hour),
			expectedMetadataEndTime:   now,
		},
		"should not manipulate a query on large time range close to the limit and short rate time window": {
			maxQueryLookback:          model.Duration(thirtyDays),
			query:                     "rate(foo[1m])",
			queryStartTime:            now.Add(-thirtyDays).Add(time.Hour),
			queryEndTime:              now,
			expectedQueryStartTime:    now.Add(-thirtyDays).Add(time.Hour).Add(-time.Minute),
			expectedQueryEndTime:      now,
			expectedMetadataStartTime: now.Add(-thirtyDays).Add(time.Hour),
			expectedMetadataEndTime:   now,
		},
		"should manipulate a query on short time range and rate time window over the limit": {
			maxQueryLookback:          model.Duration(thirtyDays),
			query:                     "rate(foo[31d])",
			queryStartTime:            now.Add(-time.Hour),
			queryEndTime:              now,
			expectedQueryStartTime:    now.Add(-thirtyDays),
			expectedQueryEndTime:      now,
			expectedMetadataStartTime: now.Add(-time.Hour),
			expectedMetadataEndTime:   now,
		},
		"should manipulate a query on large time range over the limit and short rate time window": {
			maxQueryLookback:          model.Duration(thirtyDays),
			query:                     "rate(foo[1m])",
			queryStartTime:            now.Add(-thirtyDays).Add(-100 * time.Hour),
			queryEndTime:              now,
			expectedQueryStartTime:    now.Add(-thirtyDays),
			expectedQueryEndTime:      now,
			expectedMetadataStartTime: now.Add(-thirtyDays),
			expectedMetadataEndTime:   now,
		},
		"should skip executing a query outside the allowed time range": {
			maxQueryLookback: model.Duration(thirtyDays),
			query:            "rate(foo[1m])",
			queryStartTime:   now.Add(-thirtyDays).Add(-100 * time.Hour),
			queryEndTime:     now.Add(-thirtyDays).Add(-90 * time.Hour),
			expectedSkipped:  true,
		},
		"negative start time with max query lookback": {
			maxQueryLookback:          model.Duration(thirtyDays),
			queryStartTime:            time.Unix(-1000, 0),
			queryEndTime:              now,
			expectedMetadataStartTime: now.Add(-thirtyDays),
			expectedMetadataEndTime:   now,
		},
		"negative start time without max query lookback": {
			queryStartTime:            time.Unix(-1000, 0),
			queryEndTime:              now,
			expectedMetadataStartTime: time.Unix(0, 0),
			expectedMetadataEndTime:   now,
		},
	}

	// Create the PromQL engine to execute the queries.
	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             log.NewNopLogger(),
		ActiveQueryTracker: nil,
		MaxSamples:         1e6,
		LookbackDelta:      engineLookbackDelta,
		Timeout:            1 * time.Minute,
	})

	for _, ingesterStreaming := range []bool{true, false} {
		expectedMethodForLabelMatchers := "MetricsForLabelMatchers"
		expectedMethodForLabelNames := "LabelNames"
		expectedMethodForLabelValues := "LabelValuesForLabelName"
		if ingesterStreaming {
			expectedMethodForLabelMatchers = "MetricsForLabelMatchersStream"
			expectedMethodForLabelNames = "LabelNamesStream"
			expectedMethodForLabelValues = "LabelValuesForLabelNameStream"
		}

		for testName, testData := range tests {
			t.Run(testName, func(t *testing.T) {
				ctx := user.InjectOrgID(context.Background(), "test")

				var cfg Config
				flagext.DefaultValues(&cfg)
				cfg.IngesterStreaming = ingesterStreaming
				cfg.IngesterMetadataStreaming = ingesterStreaming

				limits := DefaultLimitsConfig()
				limits.MaxQueryLookback = testData.maxQueryLookback
				overrides, err := validation.NewOverrides(limits, nil)
				require.NoError(t, err)

				// We don't need to query any data for this test, so an empty store is fine.
				chunkStore := &emptyChunkStore{}
				queryables := []QueryableWithFilter{UseAlwaysQueryable(NewMockStoreQueryable(cfg, chunkStore))}

				t.Run("query range", func(t *testing.T) {
					if testData.query == "" {
						return
					}
					distributor := &MockDistributor{}
					distributor.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{}, nil)
					distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryStreamResponse{}, nil)

					queryable, _, _ := New(cfg, overrides, distributor, queryables, purger.NewNoopTombstonesLoader(), nil, log.NewNopLogger())
					require.NoError(t, err)

					query, err := engine.NewRangeQuery(queryable, nil, testData.query, testData.queryStartTime, testData.queryEndTime, time.Minute)
					require.NoError(t, err)

					r := query.Exec(ctx)
					require.Nil(t, r.Err)

					_, err = r.Matrix()
					require.Nil(t, err)

					if !testData.expectedSkipped {
						// Assert on the time range of the actual executed query (5s delta).
						delta := float64(5000)
						require.Len(t, distributor.Calls, 1)
						assert.InDelta(t, util.TimeToMillis(testData.expectedQueryStartTime), int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), delta)
						assert.InDelta(t, util.TimeToMillis(testData.expectedQueryEndTime), int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), delta)
					} else {
						// Ensure no query has been executed (because skipped).
						assert.Len(t, distributor.Calls, 0)
					}
				})

				t.Run("series", func(t *testing.T) {
					distributor := &MockDistributor{}
					distributor.On("MetricsForLabelMatchers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]metric.Metric{}, nil)
					distributor.On("MetricsForLabelMatchersStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]metric.Metric{}, nil)

					queryable, _, _ := New(cfg, overrides, distributor, queryables, purger.NewNoopTombstonesLoader(), nil, log.NewNopLogger())
					q, err := queryable.Querier(ctx, util.TimeToMillis(testData.queryStartTime), util.TimeToMillis(testData.queryEndTime))
					require.NoError(t, err)

					// We apply the validation here again since when initializing querier we change the start/end time,
					// but when querying series we don't validate again. So we should pass correct hints here.
					start, end, err := validateQueryTimeRange(ctx, "test", util.TimeToMillis(testData.queryStartTime), util.TimeToMillis(testData.queryEndTime), overrides, 0)
					// Skipped query will hit errEmptyTimeRange during validation.
					if !testData.expectedSkipped {
						require.NoError(t, err)
					}

					hints := &storage.SelectHints{
						Start: start,
						End:   end,
						Func:  "series",
					}
					matcher := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test")

					set := q.Select(false, hints, matcher)
					require.False(t, set.Next()) // Expected to be empty.
					require.NoError(t, set.Err())

					if !testData.expectedSkipped {
						// Assert on the time range of the actual executed query (5s delta).
						delta := float64(5000)
						require.Len(t, distributor.Calls, 1)
						assert.Equal(t, expectedMethodForLabelMatchers, distributor.Calls[0].Method)
						assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataStartTime), int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), delta)
						assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataEndTime), int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), delta)
					} else {
						// Ensure no query has been executed (because skipped).
						assert.Len(t, distributor.Calls, 0)
					}
				})

				t.Run("label names", func(t *testing.T) {
					distributor := &MockDistributor{}
					distributor.On("LabelNames", mock.Anything, mock.Anything, mock.Anything).Return([]string{}, nil)
					distributor.On("LabelNamesStream", mock.Anything, mock.Anything, mock.Anything).Return([]string{}, nil)

					queryable, _, _ := New(cfg, overrides, distributor, queryables, purger.NewNoopTombstonesLoader(), nil, log.NewNopLogger())
					q, err := queryable.Querier(ctx, util.TimeToMillis(testData.queryStartTime), util.TimeToMillis(testData.queryEndTime))
					require.NoError(t, err)

					_, _, err = q.LabelNames()
					require.NoError(t, err)

					if !testData.expectedSkipped {
						// Assert on the time range of the actual executed query (5s delta).
						delta := float64(5000)
						require.Len(t, distributor.Calls, 1)
						assert.Equal(t, expectedMethodForLabelNames, distributor.Calls[0].Method)
						assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataStartTime), int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), delta)
						assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataEndTime), int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), delta)
					} else {
						// Ensure no query has been executed (because skipped).
						assert.Len(t, distributor.Calls, 0)
					}
				})

				t.Run("label names with matchers", func(t *testing.T) {
					matchers := []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchNotEqual, "route", "get_user"),
					}
					distributor := &MockDistributor{}
					distributor.On("MetricsForLabelMatchers", mock.Anything, mock.Anything, mock.Anything, matchers).Return([]metric.Metric{}, nil)
					distributor.On("MetricsForLabelMatchersStream", mock.Anything, mock.Anything, mock.Anything, matchers).Return([]metric.Metric{}, nil)

					queryable, _, _ := New(cfg, overrides, distributor, queryables, purger.NewNoopTombstonesLoader(), nil, log.NewNopLogger())
					q, err := queryable.Querier(ctx, util.TimeToMillis(testData.queryStartTime), util.TimeToMillis(testData.queryEndTime))
					require.NoError(t, err)

					_, _, err = q.LabelNames(matchers...)
					require.NoError(t, err)

					if !testData.expectedSkipped {
						// Assert on the time range of the actual executed query (5s delta).
						delta := float64(5000)
						require.Len(t, distributor.Calls, 1)
						assert.Equal(t, expectedMethodForLabelMatchers, distributor.Calls[0].Method)
						args := distributor.Calls[0].Arguments
						assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataStartTime), int64(args.Get(1).(model.Time)), delta)
						assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataEndTime), int64(args.Get(2).(model.Time)), delta)
						assert.Equal(t, matchers, args.Get(3).([]*labels.Matcher))
					} else {
						// Ensure no query has been executed executed (because skipped).
						assert.Len(t, distributor.Calls, 0)
					}
				})

				t.Run("label values", func(t *testing.T) {
					distributor := &MockDistributor{}
					distributor.On("LabelValuesForLabelName", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]string{}, nil)
					distributor.On("LabelValuesForLabelNameStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]string{}, nil)

					queryable, _, _ := New(cfg, overrides, distributor, queryables, purger.NewNoopTombstonesLoader(), nil, log.NewNopLogger())
					q, err := queryable.Querier(ctx, util.TimeToMillis(testData.queryStartTime), util.TimeToMillis(testData.queryEndTime))
					require.NoError(t, err)

					_, _, err = q.LabelValues(labels.MetricName)
					require.NoError(t, err)

					if !testData.expectedSkipped {
						// Assert on the time range of the actual executed query (5s delta).
						delta := float64(5000)
						require.Len(t, distributor.Calls, 1)
						assert.Equal(t, expectedMethodForLabelValues, distributor.Calls[0].Method)
						assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataStartTime), int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), delta)
						assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataEndTime), int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), delta)
					} else {
						// Ensure no query has been executed executed (because skipped).
						assert.Len(t, distributor.Calls, 0)
					}
				})
			})
		}
	}
}

// Test max query length limit works with new validateQueryTimeRange function.
func TestValidateMaxQueryLength(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	for _, tc := range []struct {
		name              string
		start             time.Time
		end               time.Time
		expectedStartMs   int64
		expectedEndMs     int64
		maxQueryLength    time.Duration
		exceedQueryLength bool
	}{
		{
			name:              "normal params, not hit max query length",
			start:             now.Add(-time.Hour),
			end:               now,
			expectedStartMs:   util.TimeToMillis(now.Add(-time.Hour)),
			expectedEndMs:     util.TimeToMillis(now),
			maxQueryLength:    24 * time.Hour,
			exceedQueryLength: false,
		},
		{
			name:              "normal params, hit max query length",
			start:             now.Add(-100 * time.Hour),
			end:               now,
			expectedStartMs:   util.TimeToMillis(now.Add(-100 * time.Hour)),
			expectedEndMs:     util.TimeToMillis(now),
			maxQueryLength:    24 * time.Hour,
			exceedQueryLength: true,
		},
		{
			name:              "negative start",
			start:             time.Unix(-1000, 0),
			end:               now,
			expectedStartMs:   0,
			expectedEndMs:     util.TimeToMillis(now),
			maxQueryLength:    24 * time.Hour,
			exceedQueryLength: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limits := DefaultLimitsConfig()
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)
			startMs, endMs, err := validateQueryTimeRange(ctx, "test", util.TimeToMillis(tc.start), util.TimeToMillis(tc.end), overrides, 0)
			require.NoError(t, err)
			startTime := model.Time(startMs)
			endTime := model.Time(endMs)
			if tc.maxQueryLength > 0 {
				require.Equal(t, tc.exceedQueryLength, endTime.Sub(startTime) > tc.maxQueryLength)
			}
		})
	}
}

// mockDistibutorFor duplicates the chunks in the mockChunkStore into the mockDistributor
// so we can test everything is dedupe correctly.
func mockDistibutorFor(t *testing.T, cs mockChunkStore, through model.Time) *MockDistributor {
	chunks, err := chunkcompat.ToChunks(cs.chunks)
	require.NoError(t, err)

	tsc := client.TimeSeriesChunk{
		Labels: []cortexpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "foo"}},
		Chunks: chunks,
	}
	matrix, err := chunk.ChunksToMatrix(context.Background(), cs.chunks, 0, through)
	require.NoError(t, err)

	result := &MockDistributor{}
	result.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(matrix, nil)
	result.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryStreamResponse{Chunkseries: []client.TimeSeriesChunk{tsc}}, nil)
	return result
}

func testRangeQuery(t testing.TB, queryable storage.Queryable, end model.Time, q query) *promql.Result {
	queryTracker := promql.NewActiveQueryTracker(t.TempDir(), 10, log.NewNopLogger())

	from, through, step := time.Unix(0, 0), end.Time(), q.step
	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             log.NewNopLogger(),
		ActiveQueryTracker: queryTracker,
		MaxSamples:         1e6,
		Timeout:            1 * time.Minute,
	})
	query, err := engine.NewRangeQuery(queryable, nil, q.query, from, through, step)
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

type errDistributor struct{}

var errDistributorError = fmt.Errorf("errDistributorError")

func (m *errDistributor) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	return nil, errDistributorError
}
func (m *errDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	return nil, errDistributorError
}
func (m *errDistributor) QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error) {
	return nil, errDistributorError
}
func (m *errDistributor) LabelValuesForLabelName(context.Context, model.Time, model.Time, model.LabelName, ...*labels.Matcher) ([]string, error) {
	return nil, errDistributorError
}
func (m *errDistributor) LabelValuesForLabelNameStream(context.Context, model.Time, model.Time, model.LabelName, ...*labels.Matcher) ([]string, error) {
	return nil, errDistributorError
}
func (m *errDistributor) LabelNames(context.Context, model.Time, model.Time) ([]string, error) {
	return nil, errDistributorError
}
func (m *errDistributor) LabelNamesStream(context.Context, model.Time, model.Time) ([]string, error) {
	return nil, errDistributorError
}
func (m *errDistributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return nil, errDistributorError
}
func (m *errDistributor) MetricsForLabelMatchersStream(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
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

func (d *emptyDistributor) QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error) {
	return nil, nil
}

func (d *emptyDistributor) LabelValuesForLabelName(context.Context, model.Time, model.Time, model.LabelName, ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (d *emptyDistributor) LabelValuesForLabelNameStream(context.Context, model.Time, model.Time, model.LabelName, ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (d *emptyDistributor) LabelNames(context.Context, model.Time, model.Time) ([]string, error) {
	return nil, nil
}

func (d *emptyDistributor) LabelNamesStream(context.Context, model.Time, model.Time) ([]string, error) {
	return nil, nil
}

func (d *emptyDistributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return nil, nil
}

func (d *emptyDistributor) MetricsForLabelMatchersStream(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return nil, nil
}

func (d *emptyDistributor) MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error) {
	return nil, nil
}

type mockStore interface {
	Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error)
}

// NewMockStoreQueryable returns the storage.Queryable implementation against the chunks store.
func NewMockStoreQueryable(cfg Config, store mockStore) storage.Queryable {
	return newMockStoreQueryable(store, getChunksIteratorFunction(cfg))
}

func newMockStoreQueryable(store mockStore, chunkIteratorFunc chunkIteratorFunc) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &mockStoreQuerier{
			store:             store,
			chunkIteratorFunc: chunkIteratorFunc,
			ctx:               ctx,
			mint:              mint,
			maxt:              maxt,
		}, nil
	})
}

type mockStoreQuerier struct {
	store             mockStore
	chunkIteratorFunc chunkIteratorFunc
	ctx               context.Context
	mint, maxt        int64
}

// Select implements storage.Querier interface.
// The bool passed is ignored because the series is always sorted.
func (q *mockStoreQuerier) Select(_ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	userID, err := tenant.TenantID(q.ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	minT, maxT := q.mint, q.maxt
	if sp != nil {
		minT, maxT = sp.Start, sp.End
	}

	// We will hit this for /series lookup when -querier.query-store-for-labels-enabled is set.
	// If we don't skip here, it'll make /series lookups extremely slow as all the chunks will be loaded.
	// That flag is only to be set with blocks storage engine, and this is a protective measure.
	if sp != nil && sp.Func == "series" {
		return storage.EmptySeriesSet()
	}

	chunks, err := q.store.Get(q.ctx, userID, model.Time(minT), model.Time(maxT), matchers...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	return partitionChunks(chunks, q.mint, q.maxt, q.chunkIteratorFunc)
}

func (q *mockStoreQuerier) LabelValues(name string, labels ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (q *mockStoreQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (q *mockStoreQuerier) Close() error {
	return nil
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

	queryTracker := promql.NewActiveQueryTracker(t.TempDir(), 10, log.NewNopLogger())

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             log.NewNopLogger(),
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

				overrides, err := validation.NewOverrides(DefaultLimitsConfig(), nil)
				require.NoError(t, err)

				queryable, _, _ := New(cfg, overrides, distributor, []QueryableWithFilter{UseAlwaysQueryable(NewMockStoreQueryable(cfg, chunkStore))}, purger.NewNoopTombstonesLoader(), nil, log.NewNopLogger())
				query, err := engine.NewRangeQuery(queryable, nil, "dummy", c.mint, c.maxt, 1*time.Minute)
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

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		setup    func(cfg *Config)
		expected error
	}{
		"should pass with default config": {
			setup: func(cfg *Config) {},
		},
		"should pass if 'query store after' is enabled and shuffle-sharding is disabled": {
			setup: func(cfg *Config) {
				cfg.QueryStoreAfter = time.Hour
			},
		},
		"should pass if 'query store after' is enabled and shuffle-sharding is enabled with greater value": {
			setup: func(cfg *Config) {
				cfg.QueryStoreAfter = time.Hour
				cfg.ShuffleShardingIngestersLookbackPeriod = 2 * time.Hour
			},
		},
		"should fail if 'query store after' is enabled and shuffle-sharding is enabled with lesser value": {
			setup: func(cfg *Config) {
				cfg.QueryStoreAfter = time.Hour
				cfg.ShuffleShardingIngestersLookbackPeriod = time.Minute
			},
			expected: errShuffleShardingLookbackLessThanQueryStoreAfter,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := &Config{}
			flagext.DefaultValues(cfg)
			testData.setup(cfg)

			assert.Equal(t, testData.expected, cfg.Validate())
		})
	}
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
