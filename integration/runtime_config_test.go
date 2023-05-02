//go:build requires_docker
// +build requires_docker

package integration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/s3"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cortex := e2ecortex.NewSingleBinaryWithConfigFile("cortex-1", cortexConfigFile, tt.flags, "", 9009, 9095)
			require.NoError(t, s.StartAndWaitReady(cortex))
			require.NoError(t, s.Stop(cortex))
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
	}
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

	err = client.Upload(context.Background(), configFileName, bytes.NewReader(content))
	require.NoError(t, err)
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks-local.yaml", cortexConfigFile))

	cortex := e2ecortex.NewSingleBinaryWithConfigFile("cortex-1", cortexConfigFile, flags, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(cortex))
	require.NoError(t, s.Stop(cortex))
}
