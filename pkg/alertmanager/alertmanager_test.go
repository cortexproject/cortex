package alertmanager

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/test"
)

func TestDispatcherGroupLimits(t *testing.T) {
	for name, tc := range map[string]struct {
		groups           int
		groupsLimit      int
		expectedFailures int
	}{
		"no limit":   {groups: 5, groupsLimit: 0, expectedFailures: 0},
		"high limit": {groups: 5, groupsLimit: 10, expectedFailures: 0},
		"low limit":  {groups: 5, groupsLimit: 3, expectedFailures: 4}, // 2 groups that fail, 2 alerts per group = 4 failures
	} {
		t.Run(name, func(t *testing.T) {
			createAlertmanagerAndSendAlerts(t, tc.groups, tc.groupsLimit, tc.expectedFailures)
		})
	}
}

func createAlertmanagerAndSendAlerts(t *testing.T, alertGroups, groupsLimit, expectedFailures int) {
	user := "test"

	reg := prometheus.NewPedanticRegistry()
	am, err := New(&Config{
		UserID:          user,
		Logger:          log.NewNopLogger(),
		Limits:          &mockAlertManagerLimits{maxDispatcherAggregationGroups: groupsLimit},
		TenantDataDir:   t.TempDir(),
		ExternalURL:     &url.URL{Path: "/am"},
		ShardingEnabled: false,
	}, reg)
	require.NoError(t, err)
	defer am.StopAndWait()

	cfgRaw := `receivers:
- name: 'prod'

route:
  group_by: ['alertname']
  group_wait: 10ms
  group_interval: 10ms
  receiver: 'prod'`

	cfg, err := config.Load(cfgRaw)
	require.NoError(t, err)
	require.NoError(t, am.ApplyConfig(user, cfg, cfgRaw))

	now := time.Now()

	for i := 0; i < alertGroups; i++ {
		alertName := model.LabelValue(fmt.Sprintf("Alert-%d", i))

		inputAlerts := []*types.Alert{
			{
				Alert: model.Alert{
					Labels: model.LabelSet{
						"alertname": alertName,
						"a":         "b",
					},
					Annotations:  model.LabelSet{"foo": "bar"},
					StartsAt:     now,
					EndsAt:       now.Add(5 * time.Minute),
					GeneratorURL: "http://example.com/prometheus",
				},
				UpdatedAt: now,
				Timeout:   false,
			},

			{
				Alert: model.Alert{
					Labels: model.LabelSet{
						"alertname": alertName,
						"z":         "y",
					},
					Annotations:  model.LabelSet{"foo": "bar"},
					StartsAt:     now,
					EndsAt:       now.Add(5 * time.Minute),
					GeneratorURL: "http://example.com/prometheus",
				},
				UpdatedAt: now,
				Timeout:   false,
			},
		}
		require.NoError(t, am.alerts.Put(inputAlerts...))
	}

	// Give it some time, as alerts are sent to dispatcher asynchronously.
	test.Poll(t, 3*time.Second, nil, func() interface{} {
		return testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP alertmanager_dispatcher_aggregation_group_limit_reached_total Number of times when dispatcher failed to create new aggregation group due to limit.
		# TYPE alertmanager_dispatcher_aggregation_group_limit_reached_total counter
		alertmanager_dispatcher_aggregation_group_limit_reached_total %d
	`, expectedFailures)), "alertmanager_dispatcher_aggregation_group_limit_reached_total")
	})
}

var (
	alert1 = model.Alert{
		Labels:       model.LabelSet{"alert": "first"},
		Annotations:  model.LabelSet{"job": "test"},
		StartsAt:     time.Now(),
		EndsAt:       time.Now(),
		GeneratorURL: "some URL",
	}
	alert1Size = alertSize(alert1)

	alert2 = model.Alert{
		Labels:       model.LabelSet{"alert": "second"},
		Annotations:  model.LabelSet{"job": "test", "cluster": "prod"},
		StartsAt:     time.Now(),
		EndsAt:       time.Now(),
		GeneratorURL: "some URL",
	}
	alert2Size = alertSize(alert2)
)

type callbackOp struct {
	alert               *types.Alert
	existing            bool
	delete              bool // true=delete, false=insert.
	expectedInsertError error

	// expected values after operation.
	expectedCount     int
	expectedTotalSize int
}

func TestAlertsLimiterWithNoLimits(t *testing.T) {
	ops := []callbackOp{
		{alert: &types.Alert{Alert: alert1}, existing: false, expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert2}, existing: false, expectedCount: 2, expectedTotalSize: alert1Size + alert2Size},
		{alert: &types.Alert{Alert: alert2}, delete: true, expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert1}, delete: true, expectedCount: 0, expectedTotalSize: 0},
	}

	testLimiter(t, &mockAlertManagerLimits{}, ops)
}

func TestAlertsLimiterWithCountLimit(t *testing.T) {
	alert2WithMoreAnnotations := alert2
	alert2WithMoreAnnotations.Annotations = model.LabelSet{"job": "test", "cluster": "prod", "new": "super-long-annotation"}
	alert2WithMoreAnnotationsSize := alertSize(alert2WithMoreAnnotations)

	ops := []callbackOp{
		{alert: &types.Alert{Alert: alert1}, existing: false, expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert2}, existing: false, expectedInsertError: fmt.Errorf(errTooManyAlerts, 1), expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert1}, delete: true, expectedCount: 0, expectedTotalSize: 0},

		{alert: &types.Alert{Alert: alert2}, existing: false, expectedCount: 1, expectedTotalSize: alert2Size},
		// Update of existing alert works -- doesn't change count.
		{alert: &types.Alert{Alert: alert2WithMoreAnnotations}, existing: true, expectedCount: 1, expectedTotalSize: alert2WithMoreAnnotationsSize},
		{alert: &types.Alert{Alert: alert2}, delete: true, expectedCount: 0, expectedTotalSize: 0},
	}

	testLimiter(t, &mockAlertManagerLimits{maxAlertsCount: 1}, ops)
}

func TestAlertsLimiterWithSizeLimit(t *testing.T) {
	alert2WithMoreAnnotations := alert2
	alert2WithMoreAnnotations.Annotations = model.LabelSet{"job": "test", "cluster": "prod", "new": "super-long-annotation"}

	ops := []callbackOp{
		{alert: &types.Alert{Alert: alert1}, existing: false, expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert2}, existing: false, expectedInsertError: fmt.Errorf(errAlertsTooBig, alert2Size), expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert2WithMoreAnnotations}, existing: false, expectedInsertError: fmt.Errorf(errAlertsTooBig, alert2Size), expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert1}, delete: true, expectedCount: 0, expectedTotalSize: 0},

		{alert: &types.Alert{Alert: alert2}, existing: false, expectedCount: 1, expectedTotalSize: alert2Size},
		{alert: &types.Alert{Alert: alert2}, delete: true, expectedCount: 0, expectedTotalSize: 0},
	}

	// Prerequisite for this test. We set size limit to alert2Size, but inserting alert1 first will prevent insertion of alert2.
	require.True(t, alert2Size > alert1Size)

	testLimiter(t, &mockAlertManagerLimits{maxAlertsSizeBytes: alert2Size}, ops)
}

func TestAlertsLimiterWithSizeLimitAndAnnotationUpdate(t *testing.T) {
	alert2WithMoreAnnotations := alert2
	alert2WithMoreAnnotations.Annotations = model.LabelSet{"job": "test", "cluster": "prod", "new": "super-long-annotation"}
	alert2WithMoreAnnotationsSize := alertSize(alert2WithMoreAnnotations)

	// Updating alert with larger annotation that goes over the size limit fails.
	testLimiter(t, &mockAlertManagerLimits{maxAlertsSizeBytes: alert2Size}, []callbackOp{
		{alert: &types.Alert{Alert: alert2}, existing: false, expectedCount: 1, expectedTotalSize: alert2Size},
		{alert: &types.Alert{Alert: alert2WithMoreAnnotations}, existing: true, expectedInsertError: fmt.Errorf(errAlertsTooBig, alert2Size), expectedCount: 1, expectedTotalSize: alert2Size},
	})

	// Updating alert with larger annotations in the limit works fine.
	testLimiter(t, &mockAlertManagerLimits{maxAlertsSizeBytes: alert2WithMoreAnnotationsSize}, []callbackOp{
		{alert: &types.Alert{Alert: alert2}, existing: false, expectedCount: 1, expectedTotalSize: alert2Size},
		{alert: &types.Alert{Alert: alert2WithMoreAnnotations}, existing: true, expectedCount: 1, expectedTotalSize: alert2WithMoreAnnotationsSize},
		{alert: &types.Alert{Alert: alert2}, existing: true, expectedCount: 1, expectedTotalSize: alert2Size},
	})
}

// testLimiter sends sequence of alerts to limiter, and checks if limiter updated reacted correctly.
func testLimiter(t *testing.T, limits Limits, ops []callbackOp) {
	reg := prometheus.NewPedanticRegistry()

	limiter := newAlertsLimiter("test", limits, reg)

	for ix, op := range ops {
		if op.delete {
			limiter.PostDelete(op.alert)
		} else {
			err := limiter.PreStore(op.alert, op.existing)
			require.Equal(t, op.expectedInsertError, err, "op %d", ix)
			if err == nil {
				limiter.PostStore(op.alert, op.existing)
			}
		}

		count, totalSize := limiter.currentStats()

		assert.Equal(t, op.expectedCount, count, "wrong count, op %d", ix)
		assert.Equal(t, op.expectedTotalSize, totalSize, "wrong total size, op %d", ix)
	}
}
