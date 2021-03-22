package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/querier"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

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
			err:            validation.LimitError("limit exceeded"),
			expectedString: "limit exceeded",
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

		// Status code 400 is remapped to 422 (only choice we have)
		{
			err:            httpgrpc.Errorf(http.StatusBadRequest, "test string"),
			expectedString: "test string",
			expectedCode:   422,
		},

		// 404 is also translated to 422
		{
			err:            httpgrpc.Errorf(http.StatusNotFound, "not found"),
			expectedString: "not found",
			expectedCode:   422,
		},

		// 505 is translated to 500
		{
			err:            httpgrpc.Errorf(http.StatusHTTPVersionNotSupported, "test"),
			expectedString: "test",
			expectedCode:   500,
		},

		{
			err:            context.DeadlineExceeded,
			expectedString: "context deadline exceeded",
			expectedCode:   500,
		},

		{
			err:            context.Canceled,
			expectedString: "context canceled",
			expectedCode:   422,
		},
	} {
		for k, q := range map[string]storage.SampleAndChunkQueryable{
			"error from queryable": testQueryable{err: tc.err},
			"error from querier":   testQueryable{q: testQuerier{err: tc.err}},
			"error from seriesset": testQueryable{q: testQuerier{s: testSeriesSet{err: tc.err}}},
		} {
			t.Run(fmt.Sprintf("%s/%d", k, ix), func(t *testing.T) {
				r := createPrometheusAPI(errorTranslateQueryable{q: q})
				rec := httptest.NewRecorder()

				req := httptest.NewRequest("GET", "/api/v1/query?query=up", nil)
				req = req.WithContext(user.InjectOrgID(context.Background(), "test org"))

				r.ServeHTTP(rec, req)

				require.Equal(t, tc.expectedCode, rec.Code)
				require.Contains(t, rec.Body.String(), tc.expectedString)
			})
		}
	}
}

func createPrometheusAPI(q storage.SampleAndChunkQueryable) *route.Router {
	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             util_log.Logger,
		Reg:                nil,
		ActiveQueryTracker: nil,
		MaxSamples:         100,
		Timeout:            5 * time.Second,
	})

	api := v1.NewAPI(
		engine,
		q,
		nil,
		nil,
		func(context.Context) v1.TargetRetriever { return &querier.DummyTargetRetriever{} },
		func(context.Context) v1.AlertmanagerRetriever { return &querier.DummyAlertmanagerRetriever{} },
		func() config.Config { return config.Config{} },
		map[string]string{}, // TODO: include configuration flags
		v1.GlobalURLOptions{},
		func(f http.HandlerFunc) http.HandlerFunc { return f },
		nil,   // Only needed for admin APIs.
		"",    // This is for snapshots, which is disabled when admin APIs are disabled. Hence empty.
		false, // Disable admin APIs.
		util_log.Logger,
		func(context.Context) v1.RulesRetriever { return &querier.DummyRulesRetriever{} },
		0, 0, 0, // Remote read samples and concurrency limit.
		regexp.MustCompile(".*"),
		func() (v1.RuntimeInfo, error) { return v1.RuntimeInfo{}, errors.New("not implemented") },
		&v1.PrometheusVersion{},
		prometheus.DefaultGatherer,
		nil,
	)

	promRouter := route.New().WithPrefix("/api/v1")
	api.Register(promRouter)

	return promRouter
}

type testQueryable struct {
	q   storage.Querier
	err error
}

func (t testQueryable) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	return nil, t.err
}

func (t testQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	if t.q != nil {
		return t.q, nil
	}
	return nil, t.err
}

type testQuerier struct {
	s   storage.SeriesSet
	err error
}

func (t testQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, t.err
}

func (t testQuerier) LabelNames() ([]string, storage.Warnings, error) {
	return nil, nil, t.err
}

func (t testQuerier) Close() error {
	return nil
}

func (t testQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if t.s != nil {
		return t.s
	}
	return storage.ErrSeriesSet(t.err)
}

type testSeriesSet struct {
	err error
}

func (t testSeriesSet) Next() bool {
	return false
}

func (t testSeriesSet) At() storage.Series {
	return nil
}

func (t testSeriesSet) Err() error {
	return t.err
}

func (t testSeriesSet) Warnings() storage.Warnings {
	return nil
}
