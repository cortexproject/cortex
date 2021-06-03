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
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/test"
)

func TestDispatcherGroupLimits(t *testing.T) {
	for name, tc := range map[string]struct {
		alerts           int
		groupsLimit      int
		expectedFailures int
	}{
		"no limit":   {alerts: 5, groupsLimit: 0, expectedFailures: 0},
		"high limit": {alerts: 5, groupsLimit: 10, expectedFailures: 0},
		"low limit":  {alerts: 5, groupsLimit: 3, expectedFailures: 2},
	} {
		t.Run(name, func(t *testing.T) {
			createAlertmanagerAndSendAlerts(t, tc.alerts, tc.groupsLimit, tc.expectedFailures)
		})
	}
}

func createAlertmanagerAndSendAlerts(t *testing.T, alerts, groupsLimit, expectedFailures int) {
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

	for i := 0; i < alerts; i++ {
		inputAlerts := []*types.Alert{
			{
				Alert: model.Alert{
					Labels:       model.LabelSet{"alertname": model.LabelValue(fmt.Sprintf("Alert-%d", i))},
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
