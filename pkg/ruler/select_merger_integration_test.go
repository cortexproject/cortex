package ruler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectMerger_Integration(t *testing.T) {
	rls := []rules.Rule{
		mustParseRule(t, `rate(http_requests_total{job="api",deploy_color="blue"}[5m])`),
		mustParseRule(t, `rate(http_requests_total{job="api",deploy_color="green"}[5m])`),
		mustParseRule(t, `rate(http_requests_total{job="api",deploy_color=~".*"}[5m])`),
	}

	// Step 1: Plan.
	plan := planMergedSelects(rls, 2)
	require.Len(t, plan, 1)
	assert.Equal(t, "http_requests_total", plan[0].metricName)

	allSeries := promql.Vector{
		{Metric: labels.FromStrings("__name__", "http_requests_total", "job", "api", "deploy_color", "blue"), T: 1000, F: 10},
		{Metric: labels.FromStrings("__name__", "http_requests_total", "job", "api", "deploy_color", "green"), T: 1000, F: 20},
		{Metric: labels.FromStrings("__name__", "http_requests_total", "job", "api", "deploy_color", "red"), T: 1000, F: 30},
	}

	var callCount atomic.Int32
	inner := func(_ context.Context, _ string, _ time.Time) (promql.Vector, error) {
		callCount.Add(1)
		return allSeries, nil
	}

	// Step 2: Create the context-aware QueryFunc and inject plan.
	qf := selectMergerQueryFunc(inner)
	ctx := withSelectMergerPlan(context.Background(), plan)
	ts := time.Now()

	// Step 3: First call triggers lazy prefetch (1 inner call).
	vecBlue, err := qf(ctx, `http_requests_total{job="api",deploy_color="blue"}`, ts)
	require.NoError(t, err)
	assert.Equal(t, int32(1), callCount.Load(), "prefetch should call inner once")
	assert.Len(t, vecBlue, 1)
	assert.Equal(t, "blue", vecBlue[0].Metric.Get("deploy_color"))

	// Step 4: Subsequent calls served from cache.
	vecGreen, err := qf(ctx, `http_requests_total{job="api",deploy_color="green"}`, ts)
	require.NoError(t, err)
	assert.Equal(t, int32(1), callCount.Load(), "should not call inner again")
	assert.Len(t, vecGreen, 1)
	assert.Equal(t, "green", vecGreen[0].Metric.Get("deploy_color"))

	vecAll, err := qf(ctx, `http_requests_total{job="api",deploy_color=~".*"}`, ts)
	require.NoError(t, err)
	assert.Equal(t, int32(1), callCount.Load())
	assert.Len(t, vecAll, 3)
}
