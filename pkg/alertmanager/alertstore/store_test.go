package alertstore

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/bucketclient"
)

func TestAlertStore_ListAllUsers(t *testing.T) {
	runForEachAlertStore(t, func(t *testing.T, store AlertStore, client interface{}) {
		ctx := context.Background()
		user1Cfg := alertspb.AlertConfigDesc{User: "user-1", RawConfig: "content-1"}
		user2Cfg := alertspb.AlertConfigDesc{User: "user-2", RawConfig: "content-2"}

		// The storage is empty.
		{
			users, err := store.ListAllUsers(ctx)
			require.NoError(t, err)
			assert.Empty(t, users)
		}

		// The storage contains users.
		{
			require.NoError(t, store.SetAlertConfig(ctx, user1Cfg))
			require.NoError(t, store.SetAlertConfig(ctx, user2Cfg))

			users, err := store.ListAllUsers(ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"user-1", "user-2"}, users)
		}
	})
}

func TestAlertStore_SetAndGetAlertConfig(t *testing.T) {
	runForEachAlertStore(t, func(t *testing.T, store AlertStore, client interface{}) {
		ctx := context.Background()
		user1Cfg := alertspb.AlertConfigDesc{User: "user-1", RawConfig: "content-1"}
		user2Cfg := alertspb.AlertConfigDesc{User: "user-2", RawConfig: "content-2"}

		// The user has no config.
		{
			_, err := store.GetAlertConfig(ctx, "user-1")
			assert.Equal(t, alertspb.ErrNotFound, err)
		}

		// The user has a config
		{
			require.NoError(t, store.SetAlertConfig(ctx, user1Cfg))
			require.NoError(t, store.SetAlertConfig(ctx, user2Cfg))

			config, err := store.GetAlertConfig(ctx, "user-1")
			require.NoError(t, err)
			assert.Equal(t, user1Cfg, config)

			config, err = store.GetAlertConfig(ctx, "user-2")
			require.NoError(t, err)
			assert.Equal(t, user2Cfg, config)

			// Ensure the config is stored at the expected location. Without this check
			// we have no guarantee that the objects are stored at the expected location.
			exists, err := objectExists(client, "alerts/user-1")
			require.NoError(t, err)
			assert.True(t, exists)

			exists, err = objectExists(client, "alerts/user-2")
			require.NoError(t, err)
			assert.True(t, exists)
		}
	})
}

func TestStore_GetAlertConfigs(t *testing.T) {
	runForEachAlertStore(t, func(t *testing.T, store AlertStore, client interface{}) {
		ctx := context.Background()
		user1Cfg := alertspb.AlertConfigDesc{User: "user-1", RawConfig: "content-1"}
		user2Cfg := alertspb.AlertConfigDesc{User: "user-2", RawConfig: "content-2"}

		// The storage is empty.
		{
			configs, err := store.GetAlertConfigs(ctx, []string{"user-1", "user-2"})
			require.NoError(t, err)
			assert.Empty(t, configs)
		}

		// The storage contains some configs.
		{
			require.NoError(t, store.SetAlertConfig(ctx, user1Cfg))

			configs, err := store.GetAlertConfigs(ctx, []string{"user-1", "user-2"})
			require.NoError(t, err)
			assert.Contains(t, configs, "user-1")
			assert.NotContains(t, configs, "user-2")
			assert.Equal(t, user1Cfg, configs["user-1"])

			// Add another user config.
			require.NoError(t, store.SetAlertConfig(ctx, user2Cfg))

			configs, err = store.GetAlertConfigs(ctx, []string{"user-1", "user-2"})
			require.NoError(t, err)
			assert.Contains(t, configs, "user-1")
			assert.Contains(t, configs, "user-2")
			assert.Equal(t, user1Cfg, configs["user-1"])
			assert.Equal(t, user2Cfg, configs["user-2"])
		}
	})
}

func TestAlertStore_DeleteAlertConfig(t *testing.T) {
	runForEachAlertStore(t, func(t *testing.T, store AlertStore, client interface{}) {
		ctx := context.Background()
		user1Cfg := alertspb.AlertConfigDesc{User: "user-1", RawConfig: "content-1"}
		user2Cfg := alertspb.AlertConfigDesc{User: "user-2", RawConfig: "content-2"}

		// Upload the config for 2 users.
		require.NoError(t, store.SetAlertConfig(ctx, user1Cfg))
		require.NoError(t, store.SetAlertConfig(ctx, user2Cfg))

		// Ensure the config has been correctly uploaded.
		config, err := store.GetAlertConfig(ctx, "user-1")
		require.NoError(t, err)
		assert.Equal(t, user1Cfg, config)

		config, err = store.GetAlertConfig(ctx, "user-2")
		require.NoError(t, err)
		assert.Equal(t, user2Cfg, config)

		// Delete the config for user-1.
		require.NoError(t, store.DeleteAlertConfig(ctx, "user-1"))

		// Ensure the correct config has been deleted.
		_, err = store.GetAlertConfig(ctx, "user-1")
		assert.Equal(t, alertspb.ErrNotFound, err)

		config, err = store.GetAlertConfig(ctx, "user-2")
		require.NoError(t, err)
		assert.Equal(t, user2Cfg, config)

		// Delete again (should be idempotent).
		require.NoError(t, store.DeleteAlertConfig(ctx, "user-1"))
	})
}

func runForEachAlertStore(t *testing.T, testFn func(t *testing.T, store AlertStore, client interface{})) {
	bucketClient := objstore.NewInMemBucket()
	bucketStore := bucketclient.NewBucketAlertStore(bucketClient, nil, log.NewNopLogger())

	stores := map[string]struct {
		store  AlertStore
		client interface{}
	}{
		"bucket": {store: bucketStore, client: bucketClient},
	}

	for name, data := range stores {
		t.Run(name, func(t *testing.T) {
			testFn(t, data.store, data.client)
		})
	}
}

func objectExists(bucketClient interface{}, key string) (bool, error) {
	if typed, ok := bucketClient.(*objstore.InMemBucket); ok {
		return typed.Exists(context.Background(), key)
	}

	panic("unexpected bucket client")
}

func makeTestFullState(content string) alertspb.FullStateDesc {
	return alertspb.FullStateDesc{
		State: &clusterpb.FullState{
			Parts: []clusterpb.Part{
				{
					Key:  "key",
					Data: []byte(content),
				},
			},
		},
	}
}

func TestBucketAlertStore_GetSetDeleteFullState(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	store := bucketclient.NewBucketAlertStore(bucket, nil, log.NewNopLogger())
	ctx := context.Background()

	state1 := makeTestFullState("one")
	state2 := makeTestFullState("two")

	// The storage is empty.
	{
		_, err := store.GetFullState(ctx, "user-1")
		assert.Equal(t, alertspb.ErrNotFound, err)

		_, err = store.GetFullState(ctx, "user-2")
		assert.Equal(t, alertspb.ErrNotFound, err)

		users, err := store.ListUsersWithFullState(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{}, users)
	}

	// The storage contains users.
	{
		require.NoError(t, store.SetFullState(ctx, "user-1", state1))
		require.NoError(t, store.SetFullState(ctx, "user-2", state2))

		res, err := store.GetFullState(ctx, "user-1")
		require.NoError(t, err)
		assert.Equal(t, state1, res)

		res, err = store.GetFullState(ctx, "user-2")
		require.NoError(t, err)
		assert.Equal(t, state2, res)

		// Ensure the config is stored at the expected location. Without this check
		// we have no guarantee that the objects are stored at the expected location.
		exists, err := bucket.Exists(ctx, "alertmanager/user-1/fullstate")
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = bucket.Exists(ctx, "alertmanager/user-2/fullstate")
		require.NoError(t, err)
		assert.True(t, exists)

		users, err := store.ListUsersWithFullState(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"user-1", "user-2"}, users)
	}

	// The storage has had user-1 deleted.
	{
		require.NoError(t, store.DeleteFullState(ctx, "user-1"))

		// Ensure the correct entry has been deleted.
		_, err := store.GetFullState(ctx, "user-1")
		assert.Equal(t, alertspb.ErrNotFound, err)

		res, err := store.GetFullState(ctx, "user-2")
		require.NoError(t, err)
		assert.Equal(t, state2, res)

		users, err := store.ListUsersWithFullState(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"user-2"}, users)

		// Delete again (should be idempotent).
		require.NoError(t, store.DeleteFullState(ctx, "user-1"))
	}
}
