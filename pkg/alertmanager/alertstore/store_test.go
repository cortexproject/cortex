package alertstore

import (
	"context"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/bucketclient"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/objectclient"
	"github.com/cortexproject/cortex/pkg/chunk"
)

func TestAlertStore_ListAllUsers(t *testing.T) {
	runForEachAlertStore(t, func(t *testing.T, store AlertStore) {
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
	runForEachAlertStore(t, func(t *testing.T, store AlertStore) {
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
		}
	})
}

func TestStore_GetAlertConfigs(t *testing.T) {
	runForEachAlertStore(t, func(t *testing.T, store AlertStore) {
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
	runForEachAlertStore(t, func(t *testing.T, store AlertStore) {
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

func runForEachAlertStore(t *testing.T, testFn func(t *testing.T, store AlertStore)) {
	legacyClient := chunk.NewMockStorage()
	legacyStore := objectclient.NewAlertStore(legacyClient, log.NewNopLogger())

	bucketClient := objstore.NewInMemBucket()
	bucketStore := bucketclient.NewBucketAlertStore(bucketClient, nil, log.NewNopLogger())

	stores := map[string]AlertStore{
		"legacy": legacyStore,
		"bucket": bucketStore,
	}

	for name, store := range stores {
		t.Run(name, func(t *testing.T) {
			testFn(t, store)
		})
	}
}
