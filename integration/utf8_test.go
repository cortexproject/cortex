//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func Test_RulerExternalLabels_UTF8Validation(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	runtimeConfigYamlFile := `
overrides:
  'user-2':
    name_validation_scheme: utf8
    ruler_external_labels:
      test.utf8.metric: 😄
`
	require.NoError(t, writeFileToSharedDir(s, runtimeConfigFile, []byte(runtimeConfigYamlFile)))
	filePath := filepath.Join(e2e.ContainerSharedDir, runtimeConfigFile)

	// Start Cortex components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks.yaml", cortexConfigFile))

	flags := map[string]string{
		"-auth.enabled":           "true",
		"-runtime-config.file":    filePath,
		"-runtime-config.backend": "filesystem",
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

	// The external labels validation should be success
	cortex := e2ecortex.NewSingleBinaryWithConfigFile("cortex-1", cortexConfigFile, flags, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(cortex))
}

func Test_Distributor_UTF8ValidationPerTenant(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	runtimeConfigYamlFile := `
overrides:
  'user-2':
    name_validation_scheme: utf8
`

	require.NoError(t, writeFileToSharedDir(s, runtimeConfigFile, []byte(runtimeConfigYamlFile)))
	filePath := filepath.Join(e2e.ContainerSharedDir, runtimeConfigFile)

	// Start Cortex components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks.yaml", cortexConfigFile))

	flags := map[string]string{
		"-auth.enabled":           "true",
		"-runtime-config.file":    filePath,
		"-runtime-config.backend": "filesystem",
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

	// user-1 uses legacy validation
	user1Client, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	// user-2 uses utf8 validation
	user2Client, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), "", "", "", "user-2")
	require.NoError(t, err)

	now := time.Now()

	utf8Series, _ := generateSeries("series_1", now, prompb.Label{Name: "test.utf8.metric", Value: "😄"})
	legacySeries, _ := generateSeries("series_2", now, prompb.Label{Name: "job", Value: "test"})

	res, err := user1Client.Push(legacySeries)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// utf8Series push should be fail for user-1
	res, err = user1Client.Push(utf8Series)
	require.NoError(t, err)
	require.Equal(t, 400, res.StatusCode)

	res, err = user2Client.Push(legacySeries)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// utf8Series push should be success for user-2
	res, err = user2Client.Push(utf8Series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
}
