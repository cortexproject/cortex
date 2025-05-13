//go:build requires_docker
// +build requires_docker

package integration

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/s3"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestOTLP(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Cortex components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks.yaml", cortexConfigFile))

	// Start Cortex in single binary mode, reading the config from file and overwriting
	// the backend config to make it work with Minio.
	flags := map[string]string{
		"-blocks-storage.s3.access-key-id":              e2edb.MinioAccessKey,
		"-blocks-storage.s3.secret-access-key":          e2edb.MinioSecretKey,
		"-blocks-storage.s3.bucket-name":                bucketName,
		"-blocks-storage.s3.endpoint":                   fmt.Sprintf("%s-minio-9000:9000", networkName),
		"-blocks-storage.s3.insecure":                   "true",
		"-blocks-storage.tsdb.enable-native-histograms": "true",
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

	// Push some series to Cortex.
	now := time.Now()
	series, expectedVector := generateSeries("series_1_total", now, prompb.Label{Name: "foo", Value: "bar"})

	metadata := []prompb.MetricMetadata{
		{
			Help: "help",
			Unit: "total",
		},
	}

	res, err := c.OTLP(series, metadata)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Query the series.
	result, err := c.Query("series_1_total", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))

	labelValues, err := c.LabelValues("foo", time.Time{}, time.Time{}, nil)
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"bar"}, labelValues)

	labelNames, err := c.LabelNames(time.Time{}, time.Time{})
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "foo"}, labelNames)

	metadataResult, err := c.Metadata("series_1_total", "")
	require.NoError(t, err)
	require.Equal(t, 1, len(metadataResult))

	// Check that a range query does not return an error to sanity check the queryrange tripperware.
	_, err = c.QueryRange("series_1", now.Add(-15*time.Minute), now, 15*time.Second)
	require.NoError(t, err)

	i := rand.Uint32()
	histogramSeries := e2e.GenerateHistogramSeries("histogram_series", now, i, false, prompb.Label{Name: "job", Value: "test"})
	res, err = c.OTLP(histogramSeries, nil)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	result, err = c.Query(`histogram_series`, now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	v := result.(model.Vector)
	require.Equal(t, 1, v.Len())
	expectedHistogram := tsdbutil.GenerateTestHistogram(int64(i))
	require.NotNil(t, v[0].Histogram)
	require.Equal(t, float64(expectedHistogram.Count), float64(v[0].Histogram.Count))
	require.Equal(t, expectedHistogram.Sum, float64(v[0].Histogram.Sum))
}

func TestOTLPIngestExemplar(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Cortex components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks.yaml", cortexConfigFile))

	// Start Cortex in single binary mode, reading the config from file and overwriting
	// the backend config to make it work with Minio.
	flags := map[string]string{
		"-blocks-storage.s3.access-key-id":              e2edb.MinioAccessKey,
		"-blocks-storage.s3.secret-access-key":          e2edb.MinioSecretKey,
		"-blocks-storage.s3.bucket-name":                bucketName,
		"-blocks-storage.s3.endpoint":                   fmt.Sprintf("%s-minio-9000:9000", networkName),
		"-blocks-storage.s3.insecure":                   "true",
		"-blocks-storage.tsdb.enable-native-histograms": "true",
		"-ingester.max-exemplars":                       "100",
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

	res, err := c.OTLPPushExemplar("exemplar_1")
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	now := time.Now()
	exemplars, err := c.QueryExemplars("exemplar_1", now.Add(-time.Minute), now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, 1, len(exemplars))
}

func TestOTLPPromoteResourceAttributesPerTenant(t *testing.T) {
	configFileName := "runtime-config.yaml"

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-auth.enabled":                        "true",
		"-runtime-config.backend":              "s3",
		"-runtime-config.s3.access-key-id":     e2edb.MinioAccessKey,
		"-runtime-config.s3.secret-access-key": e2edb.MinioSecretKey,
		"-runtime-config.s3.bucket-name":       bucketName,
		"-runtime-config.s3.endpoint":          fmt.Sprintf("%s-minio-9000:9000", networkName),
		"-runtime-config.s3.insecure":          "true",
		"-runtime-config.file":                 configFileName,
		"-runtime-config.reload-period":        "1s",

		// Distributor
		"-distributor.otlp.convert-all-attributes": "false",
		"-distributor.promote-resource-attributes": "attr1,attr2,attr3",

		// alert manager
		"-alertmanager.web.external-url":   "http://localhost/alertmanager",
		"-alertmanager-storage.backend":    "local",
		"-alertmanager-storage.local.path": filepath.Join(e2e.ContainerSharedDir, "alertmanager_configs"),
	})

	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	client, err := s3.NewBucketWithConfig(nil, s3.Config{
		Endpoint:  minio.HTTPEndpoint(),
		Insecure:  true,
		Bucket:    bucketName,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
	}, "runtime-config-test", nil)

	require.NoError(t, err)

	// update runtime config
	newRuntimeConfig := []byte(`overrides:
  user-1:
    promote_resource_attributes: ["attr1"]
  user-2:
    promote_resource_attributes: ["attr1", "attr2"]
`)
	require.NoError(t, client.Upload(context.Background(), configFileName, bytes.NewReader(newRuntimeConfig)))
	time.Sleep(2 * time.Second)

	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks-local.yaml", cortexConfigFile))

	// start cortex and assert runtime-config is loaded correctly
	cortex := e2ecortex.NewSingleBinaryWithConfigFile("cortex", cortexConfigFile, flags, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(cortex))

	c1, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	c2, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-2")
	require.NoError(t, err)

	c3, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-3")
	require.NoError(t, err)

	// Push some series to Cortex.
	now := time.Now()

	labels := []prompb.Label{
		{Name: "service.name", Value: "test-service"},
		{Name: "attr1", Value: "value"},
		{Name: "attr2", Value: "value"},
		{Name: "attr3", Value: "value"},
	}

	res, err := c1.OTLPPushExemplar("series_1", labels...)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	res, err = c2.OTLPPushExemplar("series_1", labels...)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	res, err = c3.OTLPPushExemplar("series_1", labels...)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	labelSet1, err := c1.LabelNames(now.Add(-time.Minute*5), now, "series_1")
	require.NoError(t, err)
	require.Equal(t, labelSet1, []string{"__name__", "attr1", "instance", "job"})

	labelSet2, err := c2.LabelNames(now.Add(-time.Minute*5), now, "series_1")
	require.NoError(t, err)
	require.Equal(t, labelSet2, []string{"__name__", "attr1", "attr2", "instance", "job"})

	labelSet3, err := c3.LabelNames(now.Add(-time.Minute*5), now, "series_1")
	require.NoError(t, err)
	require.Equal(t, labelSet3, []string{"__name__", "attr1", "attr2", "attr3", "instance", "job"})
}
