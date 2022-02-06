//go:build requires_docker
// +build requires_docker

package integration

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	s3 "github.com/cortexproject/cortex/pkg/chunk/aws"
	cortex_s3 "github.com/cortexproject/cortex/pkg/storage/bucket/s3"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func TestS3Client(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	// We use KES to emulate a Key Management Store for use with Minio
	kesDNSName := networkName + "-kes"
	require.NoError(t, writeCerts(s.SharedDir(), kesDNSName))
	// Start dependencies.
	kes := e2edb.NewKES(7373, serverKeyFile, serverCertFile, clientCertFile)
	require.NoError(t, s.Start(kes)) // TODO: wait for it to be ready, but currently there is no way to probe.
	minio := e2edb.NewMinioWithKES(9000, "https://"+kesDNSName+":7373", clientKeyFile, clientCertFile, caCertFile, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	tests := []struct {
		name string
		cfg  s3.S3Config
	}{
		{
			name: "expanded-config",
			cfg: s3.S3Config{
				Endpoint:         minio.HTTPEndpoint(),
				BucketNames:      bucketName,
				S3ForcePathStyle: true,
				Insecure:         true,
				AccessKeyID:      e2edb.MinioAccessKey,
				SecretAccessKey:  e2edb.MinioSecretKey,
			},
		},
		{
			name: "url-config",
			cfg: s3.S3Config{
				S3: flagext.URLValue{
					URL: urlMustParse("http://" + e2edb.MinioAccessKey + ":" + e2edb.MinioSecretKey + "@" + minio.HTTPEndpoint()),
				},
				BucketNames:      bucketName,
				S3ForcePathStyle: true,
			},
		},
		{
			name: "mixed-config",
			cfg: s3.S3Config{
				S3: flagext.URLValue{
					URL: urlMustParse("http://" + minio.HTTPEndpoint()),
				},
				BucketNames:      bucketName,
				S3ForcePathStyle: true,
				AccessKeyID:      e2edb.MinioAccessKey,
				SecretAccessKey:  e2edb.MinioSecretKey,
			},
		},
		{
			name: "config-with-deprecated-sse",
			cfg: s3.S3Config{
				Endpoint:         minio.HTTPEndpoint(),
				BucketNames:      bucketName,
				S3ForcePathStyle: true,
				Insecure:         true,
				AccessKeyID:      e2edb.MinioAccessKey,
				SecretAccessKey:  e2edb.MinioSecretKey,
				SSEEncryption:    true,
			},
		},
		{
			name: "config-with-sse-s3",
			cfg: s3.S3Config{
				Endpoint:         minio.HTTPEndpoint(),
				BucketNames:      bucketName,
				S3ForcePathStyle: true,
				Insecure:         true,
				AccessKeyID:      e2edb.MinioAccessKey,
				SecretAccessKey:  e2edb.MinioSecretKey,
				SSEConfig: cortex_s3.SSEConfig{
					Type: "SSE-S3",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := s3.NewS3ObjectClient(tt.cfg)

			require.NoError(t, err)

			ctx := context.Background()
			objectKey := "key-" + tt.name
			obj := []byte{0x01, 0x02, 0x03, 0x04}

			err = client.PutObject(ctx, objectKey, bytes.NewReader(obj))
			require.NoError(t, err)

			readCloser, err := client.GetObject(ctx, objectKey)
			require.NoError(t, err)

			read := make([]byte, 4)
			_, err = readCloser.Read(read)
			if err != io.EOF {
				require.NoError(t, err)
			}

			require.Equal(t, obj, read)
		})
	}
}

func urlMustParse(parse string) *url.URL {
	u, err := url.Parse(parse)
	if err != nil {
		panic(err)
	}

	return u
}
