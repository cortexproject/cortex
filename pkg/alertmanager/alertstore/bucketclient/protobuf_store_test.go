package bucketclient

import (
	"context"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"
)

// For testing, use one of the generic built-in protobuf types (Struct) to
// avoid having to generate a dedicated message just for the sake of this test.

func makeTestMessage(content string) *types.Struct {
	return &types.Struct{Fields: map[string]*types.Value{content: nil}}
}

func TestProtobufStore(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	store := NewProtobufStore("prefix", bucket, nil, log.NewNopLogger())
	ctx := context.Background()

	msg1 := makeTestMessage("one")
	msg2 := makeTestMessage("two")

	// The storage is empty.
	{
		users, err := store.ListAllUsers(ctx)
		require.NoError(t, err)
		assert.Empty(t, users)

		res := &types.Struct{}
		err = store.Get(ctx, "user-1", res)
		assert.True(t, store.IsNotFoundErr(err))
	}

	// The storage contains users.
	{
		require.NoError(t, store.Set(ctx, "user-1", makeTestMessage("one")))
		require.NoError(t, store.Set(ctx, "user-2", makeTestMessage("two")))

		users, err := store.ListAllUsers(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"user-1", "user-2"}, users)

		res1 := &types.Struct{}
		require.NoError(t, store.Get(ctx, "user-1", res1))
		assert.Equal(t, msg1, res1)

		res2 := &types.Struct{}
		require.NoError(t, store.Get(ctx, "user-2", res2))
		assert.Equal(t, msg2, res2)

		// Ensure the config is stored at the expected location. Without this check
		// we have no guarantee that the objects are stored at the expected location.
		exists, err := bucket.Exists(ctx, "prefix/user-1")
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = bucket.Exists(ctx, "prefix/user-2")
		require.NoError(t, err)
		assert.True(t, exists)
	}

	// The storage has had user-1 deleted.
	{
		require.NoError(t, store.Delete(ctx, "user-1"))

		// Ensure the correct entry has been deleted.
		res1 := &types.Struct{}
		err := store.Get(ctx, "user-1", res1)
		assert.True(t, store.IsNotFoundErr(err))

		res2 := &types.Struct{}
		require.NoError(t, store.Get(ctx, "user-2", res2))
		assert.Equal(t, msg2, res2)

		// Delete again (should be idempotent).
		require.NoError(t, store.Delete(ctx, "user-1"))
	}
}
