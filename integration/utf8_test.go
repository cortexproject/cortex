//go:build requires_docker

package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	open_api_models "github.com/prometheus/alertmanager/api/v2/models"
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

	silenceName := "silences.name.🙂"
	silenceValue := "silences.value.🙂"
	silenceComment := "test silences"
	silenceCreatedBy := "integration-test"
	isRegex := false
	isEqual := true
	startsAt := strfmt.DateTime(time.Now())
	endsAt := strfmt.DateTime(time.Now().Add(time.Minute))
	silenceId, err := c.CreateSilence(ctx, open_api_models.Silence{
		Matchers: open_api_models.Matchers{
			{Name: &silenceName, Value: &silenceValue, IsRegex: &isRegex, IsEqual: &isEqual},
		},
		Comment:   &silenceComment,
		CreatedBy: &silenceCreatedBy,
		StartsAt:  &startsAt,
		EndsAt:    &endsAt,
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
		"group.label.😄": "group.value",
	}
	ruleLabels := map[string]string{
		"rule.label.😄": "rule.value",
	}

	interval, _ := model.ParseDuration("1s")

	ruleGroup := rulefmt.RuleGroup{
		Name:     "rule.utf8.😄",
		Interval: interval,
		Rules: []rulefmt.Rule{{
			Alert:  "alert.rule.😄",
			Expr:   "up",
			Labels: ruleLabels,
		}, {
			Record: "record.rule.😄",
			Expr:   "up",
			Labels: ruleLabels,
		}},
		Labels: groupLabels,
	}

	c, err := e2ecortex.NewClient("", "", "", cortex.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	err = c.SetRuleGroup(ruleGroup, "namespace")
	require.NoError(t, err)
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(1), "cortex_ruler_managers_total"))
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
		"-ingester.max-exemplars":              "100",
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
	utf8Series[0].Exemplars = []prompb.Exemplar{{
		Labels:    []prompb.Label{{Name: "trace.id.😄", Value: "abc"}},
		Value:     1,
		Timestamp: e2e.TimeToMilliseconds(now),
	}}

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

	// range query
	rangeResult, err := c.QueryRange(query, now.Add(-time.Minute), now.Add(time.Minute), 15*time.Second)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, rangeResult.Type())
	require.Equal(t, 1, rangeResult.(model.Matrix).Len())

	// exemplars
	exemplars, err := c.QueryExemplars(`{"series.1"}`, now.Add(-time.Minute), now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, 1, len(exemplars))
	require.Equal(t, 1, len(exemplars[0].Exemplars))
	require.Equal(t, model.LabelValue("abc"), exemplars[0].Exemplars[0].Labels["trace.id.😄"])

	// label names
	start := now
	end := now.Add(time.Minute * 5)
	labelNames, err := c.LabelNames(start, end)
	require.NoError(t, err)
	require.Equal(t, model.LabelNames{"__name__", "job", "test.utf8.metric"}, labelNames)

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

// Test_StoreGateway_UTF8 queries UTF-8 series after they were shipped to the
// storage and removed from the ingester, so they are served by the
// store-gateway.
func Test_StoreGateway_UTF8(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-name-validation-scheme":                  "utf8",
		"-blocks-storage.tsdb.block-ranges-period": blockRangePeriod.String(),
		"-blocks-storage.tsdb.ship-interval":       "1s",
		"-blocks-storage.tsdb.retention-period":    ((blockRangePeriod * 2) - 1).String(),
	})

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	// The second series is far enough ahead to make the ingester cut and ship
	// a block with the first one, and then drop it from the head.
	series1Timestamp := time.Now()
	series2Timestamp := series1Timestamp.Add(blockRangePeriod * 2)
	series1, _ := generateSeries("series.1", series1Timestamp, prompb.Label{Name: "test.utf8.metric", Value: "😄"})
	series2, _ := generateSeries("series.2", series2Timestamp, prompb.Label{Name: "test.utf8.metric", Value: "😄"})

	res, err := c.Push(series1)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	res, err = c.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series_removed_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))

	// Start the compactor to create the bucket index.
	compactor := e2ecortex.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(compactor))

	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-blocks-storage.bucket-store.sync-interval": "1s",
	}), "")
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-blocks-storage.bucket-store.sync-interval": "1s",
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier, storeGateway))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512*2), "cortex_ring_tokens_total"))
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(1), "cortex_bucket_store_blocks_loaded"))

	// Make sure the consistency checker doesn't consider the block uploaded
	// too recently.
	time.Sleep(3 * time.Second)

	c, err = e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	query := `{"series.1", "test.utf8.metric"="😄"}`
	result, err := c.Query(query, series1Timestamp)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	require.Equal(t, 1, len(result.(model.Vector)))

	rangeResult, err := c.QueryRange(query, series1Timestamp.Add(-time.Minute), series1Timestamp, time.Second)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, rangeResult.Type())
	require.Equal(t, 1, rangeResult.(model.Matrix).Len())

	start := series1Timestamp.Add(-time.Minute)
	end := series1Timestamp.Add(time.Second)

	series, err := c.Series([]string{`{"series.1"}`}, start, end)
	require.NoError(t, err)
	require.Equal(t, 1, len(series))
	require.Equal(t, `{__name__="series.1", test.utf8.metric="😄"}`, series[0].String())

	labelValues, err := c.LabelValues("test.utf8.metric", start, end, []string{`{"series.1"}`})
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"😄"}, labelValues)

	// The block was queried through the store-gateway.
	require.NoError(t, storeGateway.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"thanos_store_index_cache_requests_total"}, e2e.WaitMissingMetrics))
}
