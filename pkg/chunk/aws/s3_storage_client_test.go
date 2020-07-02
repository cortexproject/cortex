package aws

import (
	"bytes"
	"context"
	"testing"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
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

	client, err := NewS3ObjectClient(S3Config{
		Endpoint:         minio.HTTPEndpoint(),
		BucketNames:      bucketName,
		S3ForcePathStyle: true,
		Insecure:         true,
	}, "/")

	require.NoError(t, err)

	ctx := context.Background()
	objectKey := "key"
	obj := []byte{0x01}

	err = client.PutObject(ctx, objectKey, bytes.NewReader(obj))
	require.NoError(t, err)

	readCloser, err := client.GetObject(ctx, objectKey)
	require.NoError(t, err)

	read := make([]byte, 1)
	_, err = readCloser.Read(read)
	require.NoError(t, err)

	require.Equal(t, obj, read)
}
