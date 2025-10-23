//go:build requires_docker
// +build requires_docker

package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

const utf8AlertmanagerConfig = `route:
  receiver: dummy
  group_by: [group.test.🙂]
  routes:
    - matchers:
      - foo.🙂=bar.🙂
receivers:
  - name: dummy`

func Test_Alertmanager_UTF8(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(AlertmanagerFlags(), AlertmanagerS3Flags(),
		map[string]string{
			"-name-validation-scheme": "utf8",
		},
	)

	minio := e2edb.NewMinio(9000, alertsBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	alertmanager := e2ecortex.NewAlertmanager(
		"alertmanager",
		flags,
		"",
	)

	require.NoError(t, s.StartAndWaitReady(alertmanager))

	c, err := e2ecortex.NewClient("", "", alertmanager.HTTPEndpoint(), "", "user-1")
	require.NoError(t, err)

	ctx := context.Background()

	err = c.SetAlertmanagerConfig(ctx, utf8AlertmanagerConfig, map[string]string{})
	require.NoError(t, err)

	require.NoError(t, alertmanager.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_alertmanager_config_last_reload_successful"}, e2e.WaitMissingMetrics))
	require.NoError(t, alertmanager.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_alertmanager_config_hash"}, e2e.WaitMissingMetrics))

	silenceId, err := c.CreateSilence(ctx, types.Silence{
		Matchers: amlabels.Matchers{
			{Name: "silences.name.🙂", Value: "silences.value.🙂"},
		},
		Comment:  "test silences",
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(time.Minute),
	})
	require.NoError(t, err)
	require.NotEmpty(t, silenceId)
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Equals(1), "cortex_alertmanager_silences"))

	err = c.SendAlertToAlermanager(ctx, &model.Alert{
		Labels: model.LabelSet{
			"alert.name.🙂": "alert.value.🙂",
		},
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(time.Minute),
	})
	require.NoError(t, err)
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Equals(1), "cortex_alertmanager_alerts_received_total"))
}

func Test_Ruler_UTF8(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, rulestoreBucketName, bucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	runtimeConfigYamlFile := `
overrides:
  'user-1':
    ruler_external_labels:
      test.utf8.metric: 😄
`
	require.NoError(t, writeFileToSharedDir(s, runtimeConfigFile, []byte(runtimeConfigYamlFile)))
	filePath := filepath.Join(e2e.ContainerSharedDir, runtimeConfigFile)

	flags := mergeFlags(
		BlocksStorageFlags(),
		RulerFlags(),
		map[string]string{
			"-runtime-config.file":    filePath,
			"-runtime-config.backend": "filesystem",
			"-name-validation-scheme": "utf8",

			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// ingester
			"-blocks-storage.s3.access-key-id":     e2edb.MinioAccessKey,
			"-blocks-storage.s3.secret-access-key": e2edb.MinioSecretKey,
			"-blocks-storage.s3.bucket-name":       bucketName,
			"-blocks-storage.s3.endpoint":          fmt.Sprintf("%s-minio-9000:9000", networkName),
			"-blocks-storage.s3.insecure":          "true",
			// alert manager
			"-alertmanager.web.external-url":   "http://localhost/alertmanager",
			"-alertmanager-storage.backend":    "local",
			"-alertmanager-storage.local.path": filepath.Join(e2e.ContainerSharedDir, "alertmanager_configs"),
		},
	)
	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	// Start Cortex.
	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	groupLabels := map[string]string{
		"group_label": "group_value",
	}
	ruleLabels := map[string]string{
		"rule_label": "rule_value",
	}

	interval, _ := model.ParseDuration("1s")

	ruleGroup := rulefmt.RuleGroup{
		Name:     "rule",
		Interval: interval,
		Rules: []rulefmt.Rule{{
			Alert:  "alert_rule",
			Expr:   "up",
			Labels: ruleLabels,
		}, {
			Record: "record_rule",
			Expr:   "up",
			Labels: ruleLabels,
		}},
		Labels: groupLabels,
	}

	c, err := e2ecortex.NewClient("", "", "", cortex.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	// Set rule group to user-1
	err = c.SetRuleGroup(ruleGroup, "namespace")
	require.NoError(t, err)
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ruler_managers_total"}), e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1")))
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ruler_rule_groups_in_store"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1")),
		e2e.WaitMissingMetrics,
	))
}

func Test_PushQuery_UTF8(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Cortex components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks.yaml", cortexConfigFile))

	flags := map[string]string{
		"-auth.enabled":           "true",
		"-name-validation-scheme": "utf8",
		// ingester
		"-blocks-storage.s3.access-key-id":     e2edb.MinioAccessKey,
		"-blocks-storage.s3.secret-access-key": e2edb.MinioSecretKey,
		"-blocks-storage.s3.bucket-name":       bucketName,
		"-blocks-storage.s3.endpoint":          fmt.Sprintf("%s-minio-9000:9000", networkName),
		"-blocks-storage.s3.insecure":          "true",
		// alert manager
		"-alertmanager.web.external-url":   "http://localhost/alertmanager",
		"-alertmanager-storage.backend":    "local",
		"-alertmanager-storage.local.path": filepath.Join(e2e.ContainerSharedDir, "alertmanager_configs"),
	}
	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	cortex := e2ecortex.NewSingleBinaryWithConfigFile("cortex-1", cortexConfigFile, flags, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(cortex))

	c, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()

	utf8Series, _ := generateSeries("series.1", now, prompb.Label{Name: "test.utf8.metric", Value: "😄"})
	legacySeries, _ := generateSeries("series_2", now, prompb.Label{Name: "job", Value: "test"})

	metadata := []prompb.MetricMetadata{
		{
			MetricFamilyName: "metadata.name",
			Help:             "metadata.help",
			Unit:             "metadata.unit",
		},
	}

	res, err := c.Push(legacySeries, metadata...)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// utf8Series push should be success
	res, err = c.Push(utf8Series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// utf8 querying
	// c.f. https://prometheus.io/docs/guides/utf8/#querying
	query := `{"series.1", "test.utf8.metric"="😄"}`
	queryResult, err := c.Query(query, now)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	require.Equal(t, model.ValVector, queryResult.Type())
	vec := queryResult.(model.Vector)
	require.Equal(t, 1, len(vec))

	// label names
	start := now
	end := now.Add(time.Minute * 5)
	labelNames, err := c.LabelNames(start, end)
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "job", "test.utf8.metric"}, labelNames)

	// series
	series, err := c.Series([]string{`{"test.utf8.metric"="😄"}`}, start, end)
	require.NoError(t, err)
	require.Equal(t, 1, len(series))
	require.Equal(t, `{__name__="series.1", test.utf8.metric="😄"}`, series[0].String())

	// label values
	labelValues, err := c.LabelValues("test.utf8.metric", start, end, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(labelValues))
	require.Equal(t, model.LabelValue("😄"), labelValues[0])

	// metadata
	metadataResult, err := c.Metadata("metadata.name", "")
	require.NoError(t, err)
	require.Equal(t, 1, len(metadataResult))
}
