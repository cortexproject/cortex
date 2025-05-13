package ingester

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	util_math "github.com/cortexproject/cortex/pkg/util/math"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	defaultLimit          = -1
	defaultLimitPerMetric = -1
)

func Test_UserMetricsMetadata(t *testing.T) {
	userId := "user-1"

	reg := prometheus.NewPedanticRegistry()
	ingestionRate := util_math.NewEWMARate(0.2, instanceIngestionRateTickInterval)
	inflightPushRequests := util_math.MaxTracker{}
	maxInflightQueryRequests := util_math.MaxTracker{}

	m := newIngesterMetrics(reg,
		false,
		false,
		func() *InstanceLimits {
			return &InstanceLimits{}
		},
		ingestionRate,
		&inflightPushRequests,
		&maxInflightQueryRequests,
		false)

	limits := validation.Limits{}
	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)
	limiter := NewLimiter(overrides, nil, util.ShardingStrategyDefault, true, 1, false, "")

	userMetricsMetadata := newMetadataMap(limiter, m, validation.NewValidateMetrics(reg), userId)

	addMetricMetadata := func(name string, i int) {
		metadata := &cortexpb.MetricMetadata{
			MetricFamilyName: fmt.Sprintf("%s_%d", name, i),
			Type:             cortexpb.GAUGE,
			Help:             fmt.Sprintf("a help for %s", name),
			Unit:             fmt.Sprintf("a unit for %s", name),
		}

		err := userMetricsMetadata.add(name, metadata)
		require.NoError(t, err)
	}

	metadataNumPerMetric := 3
	for _, m := range []string{"metric1", "metric2"} {
		for i := range metadataNumPerMetric {
			addMetricMetadata(m, i)
		}
	}

	tests := []struct {
		description    string
		limit          int64
		limitPerMetric int64
		metric         string
		expectedLength int
	}{
		{
			description:    "limit: 1",
			limit:          1,
			limitPerMetric: defaultLimitPerMetric,
			expectedLength: 3,
		},
		{
			description:    "limit: 0",
			limit:          0,
			limitPerMetric: defaultLimitPerMetric,
			expectedLength: 0,
		},
		{
			description:    "limit_per_metric: 2",
			limit:          defaultLimit,
			limitPerMetric: 2,
			expectedLength: 4,
		},
		{
			description:    "limit: 0, limit_per_metric: 2",
			limit:          1,
			limitPerMetric: 2,
			expectedLength: 2,
		},
		{
			description:    "limit: 1, limit_per_metric: 0 (should be ignored)",
			limit:          1,
			limitPerMetric: 0,
			expectedLength: 3,
		},
		{
			description:    "metric: metric1",
			limit:          defaultLimit,
			limitPerMetric: defaultLimitPerMetric,
			metric:         "metric1",
			expectedLength: 3,
		},
		{
			description:    "metric: metric1, limit_per_metric: 2",
			limit:          defaultLimit,
			limitPerMetric: 2,
			metric:         "metric1",
			expectedLength: 2,
		},
		{
			description:    "not exist metric",
			limit:          1,
			limitPerMetric: defaultLimitPerMetric,
			metric:         "dummy",
			expectedLength: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			req := &client.MetricsMetadataRequest{Limit: test.limit, LimitPerMetric: test.limitPerMetric, Metric: test.metric}

			r := userMetricsMetadata.toClientMetadata(req)
			require.Equal(t, test.expectedLength, len(r))
		})
	}
}
