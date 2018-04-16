package aws

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaveworks/cortex/pkg/chunk/testutils"
)

const (
	tableName = "table"
)

func TestChunksPartialError(t *testing.T) {
	fixture := dynamoDBFixture(0, 10, 20)
	defer fixture.Teardown()
	client, err := testutils.Setup(fixture, tableName)
	require.NoError(t, err)

	// We use some carefully-chosen numbers:
	// Start with 150 chunks; DynamoDB writes batches in 25s so 6 batches.
	// We tell the client to error after 7 operations so all writes succeed
	// and then the 2nd read fails, so we read back only 100 chunks
	if ep, ok := client.(*storageClient); ok {
		ep.SetErrorParameters(22, 7)
	} else {
		t.Error("DynamoDB test client has unexpected type")
		return
	}
	ctx := context.Background()
	_, chunks, err := testutils.CreateChunks(0, 150)
	require.NoError(t, err)
	err = client.PutChunks(ctx, chunks)
	require.NoError(t, err)

	chunksWeGot, err := client.GetChunks(ctx, chunks)
	require.Error(t, err)
	require.Equal(t, 100, len(chunksWeGot))
}
