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

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	s3 "github.com/cortexproject/cortex/pkg/chunk/aws"
)

const simpleAlertmanagerConfig = `route:
  receiver: dummy
  group_by: [group]
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
	tests := map[string]struct {
		legacyAlertStore bool
	}{
		"legacy alertstore": {legacyAlertStore: true},
		"bucket alertstore": {legacyAlertStore: false},
	}

	for testName, testCfg := range tests {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			flags := mergeFlags(AlertmanagerFlags(), AlertmanagerS3Flags(testCfg.legacyAlertStore))

			minio := e2edb.NewMinio(9000, alertsBucketName)
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
		})
	}
}

func TestAlertmanagerClustering(t *testing.T) {
	tests := map[string]struct {
		legacyAlertStore bool
	}{
		"legacy alertstore": {legacyAlertStore: true},
		"bucket alertstore": {legacyAlertStore: false},
	}

	for testName, testCfg := range tests {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			flags := mergeFlags(AlertmanagerFlags(), AlertmanagerS3Flags(testCfg.legacyAlertStore))

			// Start dependencies.
			minio := e2edb.NewMinio(9000, alertsBucketName)
			require.NoError(t, s.StartAndWaitReady(minio))

			client, err := s3.NewS3ObjectClient(s3.S3Config{
				Endpoint:         minio.HTTPEndpoint(),
				S3ForcePathStyle: true,
				Insecure:         true,
				BucketNames:      alertsBucketName,
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
		})
	}
}

func TestAlertmanagerSharding(t *testing.T) {
	tests := map[string]struct {
		legacyAlertStore  bool
		replicationFactor int
	}{
		"legacy alertstore, RF = 2": {legacyAlertStore: true, replicationFactor: 2},
		"bucket alertstore, RF = 2": {legacyAlertStore: false, replicationFactor: 2},
		"legacy alertstore, RF = 3": {legacyAlertStore: true, replicationFactor: 3},
		"bucket alertstore, RF = 3": {legacyAlertStore: false, replicationFactor: 3},
	}

	for testName, testCfg := range tests {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			flags := mergeFlags(AlertmanagerFlags(), AlertmanagerS3Flags(testCfg.legacyAlertStore))

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, alertsBucketName)
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			client, err := s3.NewS3ObjectClient(s3.S3Config{
				Endpoint:         minio.HTTPEndpoint(),
				S3ForcePathStyle: true,
				Insecure:         true,
				BucketNames:      alertsBucketName,
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

			// 3 instances, 30 configurations and a replication factor of 2 or 3.
			flags = mergeFlags(flags, AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), testCfg.replicationFactor))

			// Wait for the Alertmanagers to start.
			alertmanager1 := e2ecortex.NewAlertmanager("alertmanager-1", flags, "")
			alertmanager2 := e2ecortex.NewAlertmanager("alertmanager-2", flags, "")
			alertmanager3 := e2ecortex.NewAlertmanager("alertmanager-3", flags, "")

			alertmanagers := e2ecortex.NewCompositeCortexService(alertmanager1, alertmanager2, alertmanager3)

			// Start Alertmanager instances.
			for _, am := range alertmanagers.Instances() {
				require.NoError(t, s.StartAndWaitReady(am))
			}

			require.NoError(t, alertmanagers.WaitSumMetricsWithOptions(e2e.Equals(9), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "alertmanager"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"),
			)))

			// We expect every instance to discover every configuration but only own a subset of them.
			require.NoError(t, alertmanagers.WaitSumMetrics(e2e.Equals(90), "cortex_alertmanager_tenants_discovered"))
			// We know that the ring has settled when every instance has some tenants and the total number of tokens have been assigned.
			// The total number of tenants across all instances is: total alertmanager configs * replication factor.
			require.NoError(t, alertmanagers.WaitSumMetricsWithOptions(e2e.Equals(float64(30*testCfg.replicationFactor)), []string{"cortex_alertmanager_config_last_reload_successful"}, e2e.SkipMissingMetrics))
			require.NoError(t, alertmanagers.WaitSumMetrics(e2e.Equals(float64(1152)), "cortex_ring_tokens_total"))

			// Now, let's make sure state is replicated across instances.
			// 1. Let's select a random tenant
			userID := "user-5"

			// 2. Let's create a silence
			comment := func(i int) string {
				return fmt.Sprintf("Silence Comment #%d", i)
			}
			silence := func(i int) types.Silence {
				return types.Silence{
					Matchers: amlabels.Matchers{
						{Name: "instance", Value: "prometheus-one"},
					},
					Comment:  comment(i),
					StartsAt: time.Now(),
					EndsAt:   time.Now().Add(time.Hour),
				}
			}

			// 2b. For each tenant, with a replication factor of 2 and 3 instances,
			// the user will not be present in one of the instances.
			// However, the distributor should route us to a correct instance.
			c1, err := e2ecortex.NewClient("", "", alertmanager1.HTTPEndpoint(), "", userID)
			require.NoError(t, err)
			c2, err := e2ecortex.NewClient("", "", alertmanager2.HTTPEndpoint(), "", userID)
			require.NoError(t, err)
			c3, err := e2ecortex.NewClient("", "", alertmanager3.HTTPEndpoint(), "", userID)
			require.NoError(t, err)

			clients := []*e2ecortex.Client{c1, c2, c3}

			waitForSilences := func(state string, amount int) error {
				return alertmanagers.WaitSumMetricsWithOptions(
					e2e.Equals(float64(amount)),
					[]string{"cortex_alertmanager_silences"},
					e2e.WaitMissingMetrics,
					e2e.WithLabelMatchers(
						labels.MustNewMatcher(labels.MatchEqual, "state", state),
					),
				)
			}

			var id1, id2, id3 string

			// Endpoint: POST /silences
			{
				id1, err = c1.CreateSilence(context.Background(), silence(1))
				assert.NoError(t, err)
				id2, err = c2.CreateSilence(context.Background(), silence(2))
				assert.NoError(t, err)
				id3, err = c3.CreateSilence(context.Background(), silence(3))
				assert.NoError(t, err)

				// Reading silences do not currently read from all replicas. We have to wait for
				// the silence to be replicated asynchronously, before we can reliably read them.
				require.NoError(t, waitForSilences("active", 3*testCfg.replicationFactor))
			}

			assertSilences := func(list []types.Silence, s1, s2, s3 types.SilenceState) {
				assert.Equal(t, 3, len(list))

				ids := make(map[string]types.Silence, len(list))
				for _, s := range list {
					ids[s.ID] = s
				}

				require.Contains(t, ids, id1)
				assert.Equal(t, comment(1), ids[id1].Comment)
				assert.Equal(t, s1, ids[id1].Status.State)
				require.Contains(t, ids, id2)
				assert.Equal(t, comment(2), ids[id2].Comment)
				assert.Equal(t, s2, ids[id2].Status.State)
				require.Contains(t, ids, id3)
				assert.Equal(t, comment(3), ids[id3].Comment)
				assert.Equal(t, s3, ids[id3].Status.State)
			}

			// Endpoint: GET /silences
			{
				for _, c := range clients {
					list, err := c.GetSilences(context.Background())
					require.NoError(t, err)
					assertSilences(list, types.SilenceStateActive, types.SilenceStateActive, types.SilenceStateActive)
				}
			}

			// Endpoint: GET /silence/{id}
			{
				for _, c := range clients {
					sil1, err := c.GetSilence(context.Background(), id1)
					require.NoError(t, err)
					assert.Equal(t, comment(1), sil1.Comment)
					assert.Equal(t, types.SilenceStateActive, sil1.Status.State)

					sil2, err := c.GetSilence(context.Background(), id2)
					require.NoError(t, err)
					assert.Equal(t, comment(2), sil2.Comment)
					assert.Equal(t, types.SilenceStateActive, sil2.Status.State)

					sil3, err := c.GetSilence(context.Background(), id3)
					require.NoError(t, err)
					assert.Equal(t, comment(3), sil3.Comment)
					assert.Equal(t, types.SilenceStateActive, sil3.Status.State)
				}
			}

			// Endpoint: GET /receivers
			{
				for _, c := range clients {
					list, err := c.GetReceivers(context.Background())
					assert.NoError(t, err)
					assert.ElementsMatch(t, list, []string{"dummy"})
				}
			}

			// Endpoint: GET /status
			{
				for _, c := range clients {
					_, err := c.GetAlertmanagerConfig(context.Background())
					assert.NoError(t, err)
				}
			}

			// Endpoint: DELETE /silence/{id}
			{
				// Delete one silence via each instance. Listing the silences on
				// all other instances should yield the silence being expired.
				err = c1.DeleteSilence(context.Background(), id2)
				assert.NoError(t, err)

				// These waits are required as deletion replication is currently
				// asynchronous, and silence reading is not consistent. Once
				// merging is implemented on the read path, this is not needed.
				require.NoError(t, waitForSilences("expired", 1*testCfg.replicationFactor))

				for _, c := range clients {
					list, err := c.GetSilences(context.Background())
					require.NoError(t, err)
					assertSilences(list, types.SilenceStateActive, types.SilenceStateExpired, types.SilenceStateActive)
				}

				err = c2.DeleteSilence(context.Background(), id3)
				assert.NoError(t, err)
				require.NoError(t, waitForSilences("expired", 2*testCfg.replicationFactor))

				for _, c := range clients {
					list, err := c.GetSilences(context.Background())
					require.NoError(t, err)
					assertSilences(list, types.SilenceStateActive, types.SilenceStateExpired, types.SilenceStateExpired)
				}

				err = c3.DeleteSilence(context.Background(), id1)
				assert.NoError(t, err)
				require.NoError(t, waitForSilences("expired", 3*testCfg.replicationFactor))

				for _, c := range clients {
					list, err := c.GetSilences(context.Background())
					require.NoError(t, err)
					assertSilences(list, types.SilenceStateExpired, types.SilenceStateExpired, types.SilenceStateExpired)
				}
			}

			alert := func(i, g int) *model.Alert {
				return &model.Alert{
					Labels: model.LabelSet{
						"name":  model.LabelValue(fmt.Sprintf("alert_%d", i)),
						"group": model.LabelValue(fmt.Sprintf("group_%d", g)),
					},
					StartsAt: time.Now(),
					EndsAt:   time.Now().Add(time.Hour),
				}
			}

			alertNames := func(list []model.Alert) (r []string) {
				for _, a := range list {
					r = append(r, string(a.Labels["name"]))
				}
				return
			}

			// Endpoint: POST /alerts
			{
				err = c1.SendAlertToAlermanager(context.Background(), alert(1, 1))
				require.NoError(t, err)
				err = c2.SendAlertToAlermanager(context.Background(), alert(2, 1))
				require.NoError(t, err)
				err = c3.SendAlertToAlermanager(context.Background(), alert(3, 2))
				require.NoError(t, err)
			}

			// Endpoint: GET /v1/alerts
			{
				// Reads will query at least two replicas and merge the results.
				// Therefore, the alerts we posted should always be visible.

				for _, c := range clients {
					list, err := c.GetAlerts(context.Background())
					require.NoError(t, err)
					assert.ElementsMatch(t, []string{"alert_1", "alert_2", "alert_3"}, alertNames(list))
				}
			}

			// Endpoint: GET /alerts/groups
			{
				// Writes do not block for the write slowest replica, and reads do not
				// currently merge results from multiple replicas, so we have to wait.
				require.NoError(t, alertmanagers.WaitSumMetricsWithOptions(
					e2e.Equals(float64(3*testCfg.replicationFactor)),
					[]string{"cortex_alertmanager_alerts_received_total"},
					e2e.SkipMissingMetrics))

				for _, c := range clients {
					list, err := c.GetAlertGroups(context.Background())
					require.NoError(t, err)

					assert.Equal(t, 2, len(list))
					groups := make(map[string][]model.Alert)
					for _, g := range list {
						groups[string(g.Labels["group"])] = g.Alerts
					}

					require.Contains(t, groups, "group_1")
					assert.ElementsMatch(t, []string{"alert_1", "alert_2"}, alertNames(groups["group_1"]))
					require.Contains(t, groups, "group_2")
					assert.ElementsMatch(t, []string{"alert_3"}, alertNames(groups["group_2"]))
				}
			}
		})
	}
}
