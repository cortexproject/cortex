package aws

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"testing"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/stretchr/testify/require"
)

func TestS3Client(t *testing.T) {
	networkName := "s3-test"
	bucketName := "test"

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	tests := []struct {
		name string
		cfg  S3Config
	}{
		{
			name: "expanded-config",
			cfg: S3Config{
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
			cfg: S3Config{
				S3: flagext.URLValue{
					URL: urlMustParse("http://" + e2edb.MinioAccessKey + ":" + e2edb.MinioSecretKey + "@" + minio.HTTPEndpoint()),
				},
				BucketNames:      bucketName,
				S3ForcePathStyle: true,
			},
		},
		{
			name: "mixed-config",
			cfg: S3Config{
				S3: flagext.URLValue{
					URL: urlMustParse("http://" + minio.HTTPEndpoint()),
				},
				BucketNames:      bucketName,
				S3ForcePathStyle: true,
				AccessKeyID:      e2edb.MinioAccessKey,
				SecretAccessKey:  e2edb.MinioSecretKey,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewS3ObjectClient(tt.cfg, "/")

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
