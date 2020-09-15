// +build requires_docker

package integration

import (
	"context"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestAlertmanager(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs/user-1.yaml", []byte(cortexAlertmanagerUserConfigYaml)))

	alertmanager := e2ecortex.NewAlertmanager(
		"alertmanager",
		mergeFlags(
			AlertmanagerFlags,
			AlertmanagerLocalFlags,
		),
		"",
	)
	require.NoError(t, s.StartAndWaitReady(alertmanager))
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Equals(0), "cortex_alertmanager_config_invalid"))

	c, err := e2ecortex.NewClient("", "", alertmanager.HTTPEndpoint(), "", "user-1")
	require.NoError(t, err)

	cfg, err := c.GetAlertmanagerConfig(context.Background())
	require.NoError(t, err)

	// Ensure the returned status config matches alertmanager_test_fixtures/user-1.yaml
	require.NotNil(t, cfg)
	require.Equal(t, "example_receiver", cfg.Route.Receiver)
	require.Len(t, cfg.Route.GroupByStr, 1)
	require.Equal(t, "example_groupby", cfg.Route.GroupByStr[0])
	require.Len(t, cfg.Receivers, 1)
	require.Equal(t, "example_receiver", cfg.Receivers[0].Name)

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, AlertManager, alertmanager)
}

func TestAlertmanagerStoreAPI(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	minio := e2edb.NewMinio(9000, AlertmanagerS3Flags["-alertmanager.storage.s3.buckets"])
	require.NoError(t, s.StartAndWaitReady(minio))

	am := e2ecortex.NewAlertmanager(
		"alertmanager",
		mergeFlags(
			AlertmanagerFlags,
			AlertmanagerS3Flags,
		),
		"",
	)

	require.NoError(t, s.StartAndWaitReady(am))
	require.NoError(t, am.WaitSumMetrics(e2e.Equals(1), "alertmanager_cluster_members"))

	c, err := e2ecortex.NewClient("", "", am.HTTPEndpoint(), "", "user-1")
	require.NoError(t, err)

	_, err = c.GetAlertmanagerConfig(context.Background())
	require.Error(t, err)
	require.EqualError(t, err, e2ecortex.ErrNotFound.Error())

	err = c.SetAlertmanagerConfig(context.Background(), cortexAlertmanagerUserConfigYaml, map[string]string{})
	require.NoError(t, err)

	require.NoError(t, am.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_alertmanager_config_invalid"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1")),
		e2e.WaitMissingMetrics))

	cfg, err := c.GetAlertmanagerConfig(context.Background())
	require.NoError(t, err)

	// Ensure the returned status config matches the loaded config
	require.NotNil(t, cfg)
	require.Equal(t, "example_receiver", cfg.Route.Receiver)
	require.Len(t, cfg.Route.GroupByStr, 1)
	require.Equal(t, "example_groupby", cfg.Route.GroupByStr[0])
	require.Len(t, cfg.Receivers, 1)
	require.Equal(t, "example_receiver", cfg.Receivers[0].Name)

	err = c.SendAlertToAlermanager(context.Background(), &model.Alert{Labels: model.LabelSet{"foo": "bar"}})
	require.NoError(t, err)

	require.NoError(t, am.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_alertmanager_alerts_received_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1")),
		e2e.WaitMissingMetrics))

	err = c.DeleteAlertmanagerConfig(context.Background())
	require.NoError(t, err)

	// The deleted config is applied asynchronously, so we should wait until the metric
	// disappear for the specific user.
	require.NoError(t, am.WaitRemovedMetric("cortex_alertmanager_config_invalid", e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))

	cfg, err = c.GetAlertmanagerConfig(context.Background())
	require.Error(t, err)
	require.Nil(t, cfg)
	require.EqualError(t, err, "not found")
}
