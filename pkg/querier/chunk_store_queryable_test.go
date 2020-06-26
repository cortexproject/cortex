package querier

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/querier/chunkstore"
	"github.com/cortexproject/cortex/pkg/util"
)

// Make sure that chunkSeries implements SeriesWithChunks
var _ SeriesWithChunks = &chunkSeries{}

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
	var allChunks []chunk.Chunk

	const count = 10
	// go down, to add series in reversed order
	for i := count; i > 0; i-- {
		ch := mkChunk(t, model.Time(0), model.Time(1000), time.Millisecond, promchunk.Bigchunk)
		// mkChunk uses `foo` as metric name, so we rename metric to be unique
		ch.Metric[0].Value = fmt.Sprintf("%02d", i)

		allChunks = append(allChunks, ch)
	}

	res := partitionChunks(allChunks, 0, 1000, mergeChunks)

	// collect labels from each series
	var seriesLabels []labels.Labels
	for res.Next() {
		seriesLabels = append(seriesLabels, res.At().Labels())
	}

	require.Len(t, seriesLabels, count)
	require.True(t, sort.IsSorted(sortedByLabels(seriesLabels)))
}

type sortedByLabels []labels.Labels

func (b sortedByLabels) Len() int           { return len(b) }
func (b sortedByLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b sortedByLabels) Less(i, j int) bool { return labels.Compare(b[i], b[j]) < 0 }

func TestApiStatusCodes(t *testing.T) {
	for ix, tc := range []struct {
		err            error
		expectedString string
		expectedCode   int
	}{
		{
			err:            errors.New("some random error"),
			expectedString: "some random error",
			expectedCode:   500,
		},

		{
			err:            chunk.QueryError("special handling"), // handled specially by chunk_store_queryable
			expectedString: "special handling",
			expectedCode:   422,
		},

		{
			err:            promql.ErrTooManySamples("query execution"),
			expectedString: "too many samples",
			expectedCode:   422,
		},

		{
			err:            promql.ErrQueryCanceled("query execution"),
			expectedString: "query was canceled",
			expectedCode:   503,
		},

		{
			err:            promql.ErrQueryTimeout("query execution"),
			expectedString: "query timed out",
			expectedCode:   503,
		},

		// Unfortunately, queryable cannot return anything else than 500 or 422.
		{
			err:            httpgrpc.Errorf(http.StatusBadRequest, "test string"),
			expectedString: "test string",
			expectedCode:   500,
		},
	} {
		t.Run(fmt.Sprintf("%d", ix), func(t *testing.T) {
			r := createPrometheusAPI(testStore{err: tc.err})
			rec := httptest.NewRecorder()

			req := httptest.NewRequest("GET", "/api/v1/query?query=up", nil)
			req = req.WithContext(user.InjectOrgID(context.Background(), "test org"))

			r.ServeHTTP(rec, req)

			require.Equal(t, tc.expectedCode, rec.Code)
			require.Contains(t, rec.Body.String(), tc.expectedString)
		})
	}
}

func createPrometheusAPI(store chunkstore.ChunkStore) *route.Router {
	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             util.Logger,
		Reg:                nil,
		ActiveQueryTracker: nil,
		MaxSamples:         100,
		Timeout:            5 * time.Second,
	})

	queryable := newChunkStoreQueryable(store, mergeChunks)

	api := v1.NewAPI(
		engine,
		&sampleAndChunkQueryable{queryable},
		func(context.Context) v1.TargetRetriever { return &DummyTargetRetriever{} },
		func(context.Context) v1.AlertmanagerRetriever { return &DummyAlertmanagerRetriever{} },
		func() config.Config { return config.Config{} },
		map[string]string{}, // TODO: include configuration flags
		v1.GlobalURLOptions{},
		func(f http.HandlerFunc) http.HandlerFunc { return f },
		nil,   // Only needed for admin APIs.
		"",    // This is for snapshots, which is disabled when admin APIs are disabled. Hence empty.
		false, // Disable admin APIs.
		util.Logger,
		func(context.Context) v1.RulesRetriever { return &DummyRulesRetriever{} },
		0, 0, 0, // Remote read samples and concurrency limit.
		regexp.MustCompile(".*"),
		func() (v1.RuntimeInfo, error) { return v1.RuntimeInfo{}, errors.New("not implemented") },
		&v1.PrometheusVersion{},
	)

	promRouter := route.New().WithPrefix("/api/v1")
	api.Register(promRouter)

	return promRouter
}

type testStore struct {
	err error
}

func (t testStore) Get(context.Context, string, model.Time, model.Time, ...*labels.Matcher) ([]chunk.Chunk, error) {
	return nil, t.err
}
