package ingester

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// TestIngester_OwnedSeriesTrackingWithoutActiveSeriesMetrics checks that owned-series
// tracking works with -ingester.active-series-metrics-enabled=false. Both are built
// on the active series tracker, and that flag used to switch the tracker off, which
// silently disabled owned-series tracking along with it.
func TestIngester_OwnedSeriesTrackingWithoutActiveSeriesMetrics(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.ActiveSeriesMetricsEnabled = false
	// Cycles run only when the test calls updateActiveSeries.
	cfg.ActiveSeriesMetricsUpdatePeriod = time.Hour
	cfg.ownedSeriesCheckPeriod = time.Hour
	cfg.OwnedSeriesMetricsEnabled = true

	reg := prometheus.NewRegistry()
	i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, "", reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), i) })
	test.Poll(t, time.Second, ring.ACTIVE, func() any { return i.lifecycler.GetState() })

	ctx := user.InjectOrgID(context.Background(), userID)
	req, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "series"), 1, time.Now().UnixMilli())
	_, err = i.Push(ctx, req)
	require.NoError(t, err)

	db, err := i.getTSDB(userID)
	require.NoError(t, err)
	require.Equal(t, 1, db.activeSeries.Tracked(), "the push must be recorded in the tracker")

	i.updateActiveSeries(ctx)

	families, err := reg.Gather()
	require.NoError(t, err)
	exported := map[string]float64{}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			exported[family.GetName()] += metric.GetGauge().GetValue()
		}
	}
	require.Contains(t, exported, "cortex_ingester_owned_series")
	require.Equal(t, float64(1), exported["cortex_ingester_owned_series"])
	require.NotContains(t, exported, "cortex_ingester_active_series", "active series metrics are disabled")
	require.NotContains(t, exported, "cortex_ingester_active_native_histogram_series", "active series metrics are disabled")
}

// TestIngester_OwnedSeriesLimitEnforcementWithoutOwnedSeriesMetrics checks that
// enabling enforcement without owned-series tracking is reported once at startup,
// and that the limits then count every series in memory, as they do without it.
func TestIngester_OwnedSeriesLimitEnforcementWithoutOwnedSeriesMetrics(t *testing.T) {
	const warning = "ingester.owned-series-limit-enforcement-enabled has no effect"

	tests := map[string]struct {
		metricsEnabled     bool
		enforcementEnabled bool
		expectWarning      bool
	}{
		"enforcement without owned series metrics": {metricsEnabled: false, enforcementEnabled: true, expectWarning: true},
		"enforcement with owned series metrics":    {metricsEnabled: true, enforcementEnabled: true, expectWarning: false},
		"owned series metrics only":                {metricsEnabled: true, enforcementEnabled: false, expectWarning: false},
		"neither":                                  {metricsEnabled: false, enforcementEnabled: false, expectWarning: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.JoinAfter = 0
			cfg.OwnedSeriesMetricsEnabled = tc.metricsEnabled
			cfg.OwnedSeriesLimitEnforcementEnabled = tc.enforcementEnabled
			cfg.BlocksStorageConfig.TSDB.Dir = t.TempDir()
			cfg.BlocksStorageConfig.Bucket.Backend = "filesystem"
			cfg.BlocksStorageConfig.Bucket.Filesystem.Directory = t.TempDir()

			var logs bytes.Buffer
			i, err := New(cfg, validation.NewOverrides(defaultLimitsTestConfig(), nil), prometheus.NewRegistry(), log.NewLogfmtLogger(log.NewSyncWriter(&logs)), nil)
			require.NoError(t, err)

			if tc.expectWarning {
				require.Contains(t, logs.String(), warning)
			} else {
				require.NotContains(t, logs.String(), warning)
			}

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), i) })
			test.Poll(t, time.Second, ring.ACTIVE, func() any { return i.lifecycler.GetState() })

			ctx := user.InjectOrgID(context.Background(), userID)
			req, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "series"), 1, time.Now().UnixMilli())
			_, err = i.Push(ctx, req)
			require.NoError(t, err)

			db, err := i.getTSDB(userID)
			require.NoError(t, err)
			require.Equal(t, tc.metricsEnabled && tc.enforcementEnabled, db.ownedSeriesLimitEnabled,
				"limits may use the owned count only when both flags are set")
		})
	}
}

func TestConfig_ValidateOwnedSeries(t *testing.T) {
	valid := func() Config {
		cfg := Config{}
		flagext.DefaultValues(&cfg)
		cfg.OwnedSeriesMetricsEnabled = true
		return cfg
	}
	require.NoError(t, func() error { cfg := valid(); return cfg.Validate(nil) }())

	for name, breakIt := range map[string]func(*Config){
		"active series update":     func(cfg *Config) { cfg.ActiveSeriesMetricsUpdatePeriod = 0 },
		"lifecycler heartbeat off": func(cfg *Config) { cfg.LifecyclerConfig.HeartbeatPeriod = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := valid()
			breakIt(&cfg)
			require.Error(t, cfg.Validate(nil))
		})
	}
}
