package instantquery

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestLimitsMiddleware_MaxQueryLength(t *testing.T) {
	t.Parallel()
	const (
		thirtyDays = 30 * 24 * time.Hour
	)

	wrongQuery := `up[`
	_, parserErr := parser.ParseExpr(wrongQuery)

	tests := map[string]struct {
		maxQueryLength time.Duration
		query          string
		expectedErr    string
	}{
		"should skip validation if max length is disabled": {
			maxQueryLength: 0,
		},
		"even though failed to parse expression, should return no error since request will pass to next middleware": {
			query:          `up[`,
			maxQueryLength: thirtyDays,
			expectedErr:    httpgrpc.Errorf(http.StatusBadRequest, "%s", parserErr.Error()).Error(),
		},
		"should succeed on a query not exceeding time range": {
			query:          `up`,
			maxQueryLength: thirtyDays,
		},
		"should succeed on a query not exceeding time range2": {
			query:          `up[29d]`,
			maxQueryLength: thirtyDays,
		},
		"should succeed on a query not exceeding time range3": {
			query:          `rate(up[29d]) + rate(test[29d])`,
			maxQueryLength: thirtyDays,
		},
		"should fail on a query exceeding time range": {
			query:          `rate(up[31d])`,
			maxQueryLength: thirtyDays,
			expectedErr:    "the query time range exceeds the limit",
		},
		"should fail on a query exceeding time range, work for multiple selects": {
			query:          `rate(up[20d]) + rate(up[20d] offset 20d)`,
			maxQueryLength: thirtyDays,
			expectedErr:    "the query time range exceeds the limit",
		},
		"shouldn't exceed time range when having multiple selects with offset": {
			query:          `rate(up[5m]) + rate(up[5m] offset 40d) + rate(up[5m] offset 80d)`,
			maxQueryLength: thirtyDays,
		},
	}

	for testName, testData := range tests {
		testData := testData
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			req := &tripperware.PrometheusRequest{Query: testData.query}

			limits := &mockLimits{maxQueryLength: testData.maxQueryLength}
			middleware := NewLimitsMiddleware(limits, 5*time.Minute)

			innerRes := tripperware.NewEmptyPrometheusResponse(true)
			inner := &mockHandler{}
			inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

			ctx := user.InjectOrgID(context.Background(), "test")
			outer := middleware.Wrap(inner)
			res, err := outer.Do(ctx, req)

			if testData.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), testData.expectedErr)
				assert.Nil(t, res)
				assert.Len(t, inner.Calls, 0)
			} else {
				// We expect the response returned by the inner handler.
				require.NoError(t, err)
				assert.Same(t, innerRes, res)

				// The time range of the request passed to the inner handler should have not been manipulated.
				require.Len(t, inner.Calls, 1)
			}
		})
	}
}

type mockLimits struct {
	validation.Overrides
	maxQueryLength time.Duration
}

func (m mockLimits) MaxQueryLength(string) time.Duration {
	return m.maxQueryLength
}

type mockHandler struct {
	mock.Mock
}

func (m *mockHandler) Do(ctx context.Context, req tripperware.Request) (tripperware.Response, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(tripperware.Response), args.Error(1)
}
