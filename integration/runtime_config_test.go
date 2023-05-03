//go:build requires_docker
// +build requires_docker

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/s3"
	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/cortex"
)

func TestLoadRuntimeConfigFromStorageBackend(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/runtime-config.yaml", runtimeConfigFile))
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks-local.yaml", cortexConfigFile))

	filePath := filepath.Join(e2e.ContainerSharedDir, runtimeConfigFile)
	tests := []struct {
		name  string
		flags map[string]string
	}{
		{
			name: "no storage backend provided",
			flags: map[string]string{
				"-runtime-config.file": filePath,
			},
		},
		{
			name: "filesystem as storage backend",
			flags: map[string]string{
				"-runtime-config.file":    filePath,
				"-runtime-config.backend": "filesystem",
			},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cortexSvc := e2ecortex.NewSingleBinaryWithConfigFile(fmt.Sprintf("cortex-%d", i), cortexConfigFile, tt.flags, "", 9009, 9095)
			require.NoError(t, s.StartAndWaitReady(cortexSvc))

			assertRuntimeConfigLoadedCorrectly(t, cortexSvc)

			require.NoError(t, s.Stop(cortexSvc))
		})
	}
}

func TestLoadRuntimeConfigFromCloudStorage(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	configFileName := "runtime-config.yaml"
	bucketName := "cortex"
	flags := map[string]string{
		"-runtime-config.backend":              "s3",
		"-runtime-config.s3.access-key-id":     e2edb.MinioAccessKey,
		"-runtime-config.s3.secret-access-key": e2edb.MinioSecretKey,
		"-runtime-config.s3.bucket-name":       bucketName,
		"-runtime-config.s3.endpoint":          fmt.Sprintf("%s-minio-9000:9000", networkName),
		"-runtime-config.s3.insecure":          "true",
		"-runtime-config.file":                 configFileName,
		"-runtime-config.reload-period":        "2s",
	}
	// create s3 storage backend
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	client, err := s3.NewBucketWithConfig(nil, s3.Config{
		Endpoint:  minio.HTTPEndpoint(),
		Insecure:  true,
		Bucket:    bucketName,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
	}, "runtime-config-test")
	require.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(getCortexProjectDir(), "docs/configuration/runtime-config.yaml"))
	require.NoError(t, err)

	require.NoError(t, client.Upload(context.Background(), configFileName, bytes.NewReader(content)))
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks-local.yaml", cortexConfigFile))

	// start cortex and assert runtime-config is loaded correctly
	cortexSvc := e2ecortex.NewSingleBinaryWithConfigFile("cortex-svc", cortexConfigFile, flags, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(cortexSvc))
	assertRuntimeConfigLoadedCorrectly(t, cortexSvc)

	// update runtime config
	newRuntimeConfig := []byte(`overrides:
  tenant3:
    ingestion_rate: 30000
    max_exemplars: 3`)
	require.NoError(t, client.Upload(context.Background(), configFileName, bytes.NewReader(newRuntimeConfig)))
	time.Sleep(2 * time.Second)

	runtimeConfig := getRuntimeConfig(t, cortexSvc)
	require.Nil(t, runtimeConfig.TenantLimits["tenant1"])
	require.Nil(t, runtimeConfig.TenantLimits["tenant2"])
	require.NotNil(t, runtimeConfig.TenantLimits["tenant3"])
	require.Equal(t, float64(30000), (*runtimeConfig.TenantLimits["tenant3"]).IngestionRate)
	require.Equal(t, 3, (*runtimeConfig.TenantLimits["tenant3"]).MaxExemplars)

	require.NoError(t, s.Stop(cortexSvc))
}

func assertRuntimeConfigLoadedCorrectly(t *testing.T, cortexSvc *e2ecortex.CortexService) {
	runtimeConfig := getRuntimeConfig(t, cortexSvc)

	require.NotNil(t, runtimeConfig.TenantLimits["tenant1"])
	require.Equal(t, float64(10000), (*runtimeConfig.TenantLimits["tenant1"]).IngestionRate)
	require.Equal(t, 1, (*runtimeConfig.TenantLimits["tenant1"]).MaxExemplars)
	require.NotNil(t, runtimeConfig.TenantLimits["tenant2"])
	require.Equal(t, float64(10000), (*runtimeConfig.TenantLimits["tenant2"]).IngestionRate)
	require.Equal(t, 0, (*runtimeConfig.TenantLimits["tenant2"]).MaxExemplars)
	require.Equal(t, false, *runtimeConfig.Multi.Mirroring)
	require.Equal(t, "memberlist", runtimeConfig.Multi.PrimaryStore)
	require.Equal(t, float64(42000), (*runtimeConfig.IngesterLimits).MaxIngestionRate)
	require.Equal(t, int64(10000), (*runtimeConfig.IngesterLimits).MaxInflightPushRequests)
}

func getRuntimeConfig(t *testing.T, cortexSvc *e2ecortex.CortexService) cortex.RuntimeConfigValues {
	res, err := e2e.GetRequest("http://" + cortexSvc.HTTPEndpoint() + "/runtime_config")
	require.NoError(t, err)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	runtimeConfig := cortex.RuntimeConfigValues{}
	require.NoError(t, yaml.Unmarshal(body, &runtimeConfig))
	return runtimeConfig
}
