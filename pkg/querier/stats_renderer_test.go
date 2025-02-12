package querier

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querier/stats"
)

type mockSampleAndChunkQueryable struct {
	queryableFn      func(mint, maxt int64) (storage.Querier, error)
	chunkQueryableFn func(mint, maxt int64) (storage.ChunkQuerier, error)
}

func (m mockSampleAndChunkQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return m.queryableFn(mint, maxt)
}

func (m mockSampleAndChunkQueryable) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return m.chunkQueryableFn(mint, maxt)
}

func Test_StatsRenderer(t *testing.T) {
	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples: 100,
		Timeout:    time.Second * 2,
	})
	mockQueryable := &mockSampleAndChunkQueryable{
		queryableFn: func(_, _ int64) (storage.Querier, error) {
			return mockQuerier{
				matrix: model.Matrix{
					{
						Metric: model.Metric{"__name__": "test", "foo": "bar"},
						Values: []model.SamplePair{
							{Timestamp: 1536673665000, Value: 0},
							{Timestamp: 1536673670000, Value: 1},
							{Timestamp: 1536673675000, Value: 2},
							{Timestamp: 1536673680000, Value: 3},
						},
					},
				},
			}, nil
		},
	}

	api := v1.NewAPI(
		engine,
		mockQueryable,
		nil,
		nil,
		func(context.Context) v1.ScrapePoolsRetriever { return nil },
		func(context.Context) v1.TargetRetriever { return &DummyTargetRetriever{} },
		func(context.Context) v1.AlertmanagerRetriever { return &DummyAlertmanagerRetriever{} },
		func() config.Config { return config.Config{} },
		map[string]string{},
		v1.GlobalURLOptions{},
		func(f http.HandlerFunc) http.HandlerFunc { return f },
		nil,   // Only needed for admin APIs.
		"",    // This is for snapshots, which is disabled when admin APIs are disabled. Hence empty.
		false, // Disable admin APIs.
		promslog.NewNopLogger(),
		func(context.Context) v1.RulesRetriever { return &DummyRulesRetriever{} },
		0, 0, 0, // Remote read samples and concurrency limit.
		false, // Not an agent.
		regexp.MustCompile(".*"),
		nil,
		&v1.PrometheusVersion{},
		nil,
		nil,
		prometheus.DefaultGatherer,
		nil,
		StatsRenderer,
		false,
		nil,
		false,
	)

	promRouter := route.New().WithPrefix("/api/v1")
	api.Register(promRouter)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/query_range?end=1536673680&query=test&start=1536673665&step=5", nil)
	ctx := context.Background()
	_, ctx = stats.ContextWithEmptyStats(ctx)
	req = req.WithContext(user.InjectOrgID(ctx, "user1"))

	rec := httptest.NewRecorder()
	promRouter.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	queryStats := stats.FromContext(ctx)
	assert.NotNil(t, queryStats)
	assert.Equal(t, uint64(4), queryStats.LoadPeakSamples())
	assert.Equal(t, uint64(4), queryStats.LoadScannedSamples())
}
