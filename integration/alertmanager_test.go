// +build requires_docker

package integration

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	s3 "github.com/cortexproject/cortex/pkg/chunk/aws"
)

const simpleAlertmanagerConfig = `route:
  receiver: dummy
receivers:
  - name: dummy`

func TestAlertmanager(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs/user-1.yaml", []byte(cortexAlertmanagerUserConfigYaml)))

	alertmanager := e2ecortex.NewAlertmanager(
		"alertmanager",
		mergeFlags(
			AlertmanagerFlags(),
			AlertmanagerLocalFlags(),
		),
		"",
	)
	require.NoError(t, s.StartAndWaitReady(alertmanager))
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Equals(1), "cortex_alertmanager_config_last_reload_successful"))
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Greater(0), "cortex_alertmanager_config_hash"))

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

	// Test compression by inspecting the response Headers
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/api/v1/alerts", alertmanager.HTTPEndpoint()), nil)
	require.NoError(t, err)

	req.Header.Set("X-Scope-OrgID", "user-1")
	req.Header.Set("Accept-Encoding", "gzip")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Execute HTTP request
	res, err := http.DefaultClient.Do(req.WithContext(ctx))
	require.NoError(t, err)

	defer res.Body.Close()
	// We assert on the Vary header as the minimum response size for enabling compression is 1500 bytes.
	// This is enough to know whenever the handler for compression is enabled or not.
	require.Equal(t, "Accept-Encoding", res.Header.Get("Vary"))
}

func TestAlertmanagerStoreAPI(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(AlertmanagerFlags(), AlertmanagerS3Flags())

	minio := e2edb.NewMinio(9000, flags["-alertmanager.storage.s3.buckets"])
	require.NoError(t, s.StartAndWaitReady(minio))

	am := e2ecortex.NewAlertmanager(
		"alertmanager",
		flags,
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

	require.NoError(t, am.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_alertmanager_config_last_reload_successful"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1")),
		e2e.WaitMissingMetrics))
	require.NoError(t, am.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_alertmanager_config_last_reload_successful_seconds"},
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
	require.NoError(t, am.WaitRemovedMetric("cortex_alertmanager_config_last_reload_successful", e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))
	require.NoError(t, am.WaitRemovedMetric("cortex_alertmanager_config_last_reload_successful_seconds", e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))

	cfg, err = c.GetAlertmanagerConfig(context.Background())
	require.Error(t, err)
	require.Nil(t, cfg)
	require.EqualError(t, err, "not found")
}

func TestAlertmanagerClustering(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(AlertmanagerFlags(), AlertmanagerS3Flags())

	// Start dependencies.
	minio := e2edb.NewMinio(9000, flags["-alertmanager.storage.s3.buckets"])
	require.NoError(t, s.StartAndWaitReady(minio))

	client, err := s3.NewS3ObjectClient(s3.S3Config{
		Endpoint:         minio.HTTPEndpoint(),
		S3ForcePathStyle: true,
		Insecure:         true,
		BucketNames:      flags["-alertmanager.storage.s3.buckets"],
		AccessKeyID:      e2edb.MinioAccessKey,
		SecretAccessKey:  e2edb.MinioSecretKey,
	})
	require.NoError(t, err)

	// Create and upload an Alertmanager configuration.
	user := "user-1"
	desc := alertspb.AlertConfigDesc{RawConfig: simpleAlertmanagerConfig, User: user, Templates: []*alertspb.TemplateDesc{}}

	d, err := desc.Marshal()
	require.NoError(t, err)
	err = client.PutObject(context.Background(), fmt.Sprintf("/alerts/%s", user), bytes.NewReader(d))
	require.NoError(t, err)

	peers := strings.Join([]string{
		e2e.NetworkContainerHostPort(networkName, "alertmanager-1", e2ecortex.GossipPort),
		e2e.NetworkContainerHostPort(networkName, "alertmanager-2", e2ecortex.GossipPort),
	}, ",")
	flags = mergeFlags(flags, AlertmanagerClusterFlags(peers))

	// Wait for the Alertmanagers to start.
	alertmanager1 := e2ecortex.NewAlertmanager("alertmanager-1", flags, "")
	alertmanager2 := e2ecortex.NewAlertmanager("alertmanager-2", flags, "")

	alertmanagers := e2ecortex.NewCompositeCortexService(alertmanager1, alertmanager2)

	// Start Alertmanager instances.
	for _, am := range alertmanagers.Instances() {
		require.NoError(t, s.StartAndWaitReady(am))
	}

	for _, am := range alertmanagers.Instances() {
		require.NoError(t, am.WaitSumMetrics(e2e.Equals(float64(0)), "alertmanager_cluster_health_score")) // Lower means healthier, 0 being totally healthy.
		require.NoError(t, am.WaitSumMetrics(e2e.Equals(float64(0)), "alertmanager_cluster_failed_peers"))
		require.NoError(t, am.WaitSumMetrics(e2e.Equals(float64(2)), "alertmanager_cluster_members"))
	}
}

func TestAlertmanagerSharding(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(AlertmanagerFlags(), AlertmanagerS3Flags())

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-alertmanager.storage.s3.buckets"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	client, err := s3.NewS3ObjectClient(s3.S3Config{
		Endpoint:         minio.HTTPEndpoint(),
		S3ForcePathStyle: true,
		Insecure:         true,
		BucketNames:      flags["-alertmanager.storage.s3.buckets"],
		AccessKeyID:      e2edb.MinioAccessKey,
		SecretAccessKey:  e2edb.MinioSecretKey,
	})
	require.NoError(t, err)

	// Create and upload Alertmanager configurations.
	for i := 1; i <= 30; i++ {
		user := fmt.Sprintf("user-%d", i)
		desc := alertspb.AlertConfigDesc{
			RawConfig: simpleAlertmanagerConfig,
			User:      user,
			Templates: []*alertspb.TemplateDesc{},
		}

		d, err := desc.Marshal()
		require.NoError(t, err)
		err = client.PutObject(context.Background(), fmt.Sprintf("/alerts/%s", user), bytes.NewReader(d))
		require.NoError(t, err)
	}

	// 3 instances, 30 configurations and a replication factor of 2.
	flags = mergeFlags(flags, AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), 2))

	// Wait for the Alertmanagers to start.
	alertmanager1 := e2ecortex.NewAlertmanager("alertmanager-1", flags, "")
	alertmanager2 := e2ecortex.NewAlertmanager("alertmanager-2", flags, "")
	alertmanager3 := e2ecortex.NewAlertmanager("alertmanager-3", flags, "")

	alertmanagers := e2ecortex.NewCompositeCortexService(alertmanager1, alertmanager2, alertmanager3)

	// Start Alertmanager instances.
	for _, am := range alertmanagers.Instances() {
		require.NoError(t, s.StartAndWaitReady(am))
	}

	for _, am := range alertmanagers.Instances() {
		require.NoError(t, am.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "alertmanager"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"),
		)))

		// We expect every instance to discover every configuration but only own a subset of them.
		require.NoError(t, am.WaitSumMetrics(e2e.Equals(float64(30)), "cortex_alertmanager_tenants_discovered"))
		// We know that the ring has settled when every instance has some tenants and the total number of tokens have been assigned.
		require.NoError(t, am.WaitSumMetrics(e2e.Greater(float64(0)), "cortex_alertmanager_tenants_owned"))
		require.NoError(t, am.WaitSumMetrics(e2e.Equals(float64(384)), "cortex_ring_tokens_total"))
	}

	var totalTenants int
	for _, am := range alertmanagers.Instances() {
		values, err := am.SumMetrics([]string{"cortex_alertmanager_tenants_owned"})
		require.NoError(t, err)

		tenants := int(e2e.SumValues(values))
		totalTenants += tenants
	}

	// The total number of tenants across all instances is: total alertmanager configs * replication factor.
	// In this case: 30 * 2
	require.Equal(t, 60, totalTenants)
}
