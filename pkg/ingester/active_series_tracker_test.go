package ingester

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestActiveSeriesTrackerConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		cfg       validation.ActiveSeriesTrackerConfig
		expectErr bool
	}{
		{name: "valid exact", cfg: validation.ActiveSeriesTrackerConfig{Name: "test", Matchers: `{__name__="api_requests_total"}`}},
		{name: "valid regex", cfg: validation.ActiveSeriesTrackerConfig{Name: "test", Matchers: `{__name__=~"api_.*"}`}},
		{name: "valid multiple", cfg: validation.ActiveSeriesTrackerConfig{Name: "test", Matchers: `{__name__=~"api_.*", job="gateway"}`}},
		{name: "invalid regex", cfg: validation.ActiveSeriesTrackerConfig{Name: "test", Matchers: `{__name__=~"[invalid"}`}, expectErr: true},
		{name: "empty matchers", cfg: validation.ActiveSeriesTrackerConfig{Name: "test", Matchers: ``}, expectErr: true},
		{name: "empty name", cfg: validation.ActiveSeriesTrackerConfig{Name: "", Matchers: `{__name__="foo"}`}, expectErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, tc.cfg.ParsedMatchers())
			}
		})
	}
}

func TestActiveSeriesTrackersConfig_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		cfg := validation.ActiveSeriesTrackersConfig{
			{Name: "a", Matchers: `{__name__="foo"}`},
			{Name: "b", Matchers: `{__name__=~"bar.*"}`},
		}
		require.NoError(t, cfg.Validate())
	})

	t.Run("duplicate names", func(t *testing.T) {
		cfg := validation.ActiveSeriesTrackersConfig{
			{Name: "dup", Matchers: `{__name__="foo"}`},
			{Name: "dup", Matchers: `{__name__="bar"}`},
		}
		assert.Error(t, cfg.Validate())
	})

	t.Run("nil is valid", func(t *testing.T) {
		var cfg validation.ActiveSeriesTrackersConfig
		require.NoError(t, cfg.Validate())
	})
}

func TestTrackerCounter_IncreaseDecrease(t *testing.T) {
	tc := newTrackerCounter()

	trackers := validation.ActiveSeriesTrackersConfig{
		{Name: "api", Matchers: `{__name__=~"api_.*"}`},
		{Name: "gateway", Matchers: `{job="gateway"}`},
	}
	require.NoError(t, trackers.Validate())

	// Simulate config load.
	tc.mu.Lock()
	tc.matchers = map[string][]*labels.Matcher{
		"api":     trackers[0].ParsedMatchers(),
		"gateway": trackers[1].ParsedMatchers(),
	}
	tc.mu.Unlock()

	// Add series.
	tc.increase(labels.FromStrings("__name__", "api_requests_total", "job", "gateway"))
	tc.increase(labels.FromStrings("__name__", "api_errors_total", "job", "gateway"))
	tc.increase(labels.FromStrings("__name__", "node_cpu_seconds", "job", "node"))

	tc.mu.Lock()
	assert.Equal(t, 2, tc.counts["api"])
	assert.Equal(t, 2, tc.counts["gateway"])
	tc.mu.Unlock()

	// Remove one series.
	tc.decrease(labels.FromStrings("__name__", "api_requests_total", "job", "gateway"))

	tc.mu.Lock()
	assert.Equal(t, 1, tc.counts["api"])
	assert.Equal(t, 1, tc.counts["gateway"])
	tc.mu.Unlock()
}

func TestTrackerCounter_UpdateMetrics(t *testing.T) {
	tc := newTrackerCounter()
	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test"}, []string{"user", "name"})

	trackers := validation.ActiveSeriesTrackersConfig{
		{Name: "api", Matchers: `{__name__=~"api_.*"}`},
		{Name: "node", Matchers: `{__name__=~"node_.*"}`},
	}
	require.NoError(t, trackers.Validate())

	tc.mu.Lock()
	tc.matchers = map[string][]*labels.Matcher{
		"api":  trackers[0].ParsedMatchers(),
		"node": trackers[1].ParsedMatchers(),
	}
	tc.counts = map[string]int{"api": 5, "node": 3}
	tc.mu.Unlock()

	tc.updateMetrics(gauge, "user-1", trackers)

	m := &dto.Metric{}
	require.NoError(t, gauge.WithLabelValues("user-1", "api").Write(m))
	assert.Equal(t, float64(5), m.GetGauge().GetValue())
	require.NoError(t, gauge.WithLabelValues("user-1", "node").Write(m))
	assert.Equal(t, float64(3), m.GetGauge().GetValue())

	// Remove "node" tracker — stale metric should be cleaned up.
	trackers2 := validation.ActiveSeriesTrackersConfig{
		{Name: "api", Matchers: `{__name__=~"api_.*"}`},
	}
	require.NoError(t, trackers2.Validate())
	tc.updateMetrics(gauge, "user-1", trackers2)

	// "node" metric deleted (re-creating gives 0).
	require.NoError(t, gauge.WithLabelValues("user-1", "node").Write(m))
	assert.Equal(t, float64(0), m.GetGauge().GetValue())
	// "api" still reported.
	require.NoError(t, gauge.WithLabelValues("user-1", "api").Write(m))
	assert.Equal(t, float64(5), m.GetGauge().GetValue())
}

func TestTrackerCounter_UpdateConfig_Backfill(t *testing.T) {
	tc := newTrackerCounter()

	// Without a DB, just sets matchers.
	trackers := validation.ActiveSeriesTrackersConfig{
		{Name: "api", Matchers: `{__name__=~"api_.*"}`},
	}
	require.NoError(t, trackers.Validate())
	tc.updateConfig(context.Background(), nil, trackers)

	tc.mu.Lock()
	assert.NotNil(t, tc.matchers["api"])
	assert.Equal(t, 0, tc.counts["api"])
	tc.mu.Unlock()
}

func TestMatchesAll(t *testing.T) {
	lbs := labels.FromStrings("__name__", "http_requests_total", "method", "GET", "status", "200")

	tests := []struct {
		name     string
		matchers []*labels.Matcher
		expected bool
	}{
		{
			name:     "all match",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"), labels.MustNewMatcher(labels.MatchEqual, "method", "GET")},
			expected: true,
		},
		{
			name:     "one doesn't match",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"), labels.MustNewMatcher(labels.MatchEqual, "method", "POST")},
			expected: false,
		},
		{
			name:     "regex match",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "__name__", "http_.*")},
			expected: true,
		},
		{
			name:     "empty matchers matches everything",
			matchers: nil,
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, matchesAll(lbs, tc.matchers))
		})
	}
}

// Ensure ActiveSeries still works (we kept matchesAll but removed ActiveForMatchers).
func TestActiveSeries_Basic(t *testing.T) {
	as := NewActiveSeries()
	now := time.Now()

	s := labels.FromStrings("__name__", "test", "job", "app")
	as.UpdateSeries(s, s.Hash(), now, false, func(l labels.Labels) labels.Labels { return l.Copy() })

	assert.Equal(t, 1, as.Active())
}
