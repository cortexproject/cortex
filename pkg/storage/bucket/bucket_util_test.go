package bucket

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestDeletePrefix(t *testing.T) {
	mem := objstore.NewInMemBucket()

	require.NoError(t, mem.Upload(context.Background(), "obj", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "prefix/1", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "prefix/2", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "prefix/sub1/3", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "prefix/sub2/4", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "outside/obj", strings.NewReader("hello")))

	del, err := DeletePrefix(context.Background(), mem, "prefix", log.NewNopLogger())
	require.NoError(t, err)
	assert.Equal(t, 4, del)
	assert.Equal(t, 2, len(mem.Objects()))
}

func TestDeletePrefixConcurrent(t *testing.T) {
	mem := objstore.NewInMemBucket()

	require.NoError(t, mem.Upload(context.Background(), "obj", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "prefix/1", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "prefix/2", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "prefix/sub1/3", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "prefix/sub2/4", strings.NewReader("hello")))
	require.NoError(t, mem.Upload(context.Background(), "outside/obj", strings.NewReader("hello")))
	n := 10000
	for i := 0; i < n; i++ {
		require.NoError(t, mem.Upload(context.Background(), fmt.Sprintf("prefix/sub/%d", i), strings.NewReader(fmt.Sprintf("hello%d", i))))
	}

	del, err := DeletePrefixConcurrent(context.Background(), mem, "prefix", log.NewNopLogger(), 100)
	require.NoError(t, err)
	assert.Equal(t, 4+n, del)
	assert.Equal(t, 2, len(mem.Objects()))
}
