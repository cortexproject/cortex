package local

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
)

func TestStore_ListAllUsers(t *testing.T) {
	ctx := context.Background()
	store, storeDir := prepareLocalStore(t)

	// The storage is empty.
	{
		users, err := store.ListAllUsers(ctx)
		require.NoError(t, err)
		assert.Empty(t, users)
	}

	// The storage contains some users.
	{
		user1Cfg := prepareAlertmanagerConfig("user-1")
		user2Cfg := prepareAlertmanagerConfig("user-2")
		require.NoError(t, ioutil.WriteFile(filepath.Join(storeDir, "user-1.yaml"), []byte(user1Cfg), os.ModePerm))
		require.NoError(t, ioutil.WriteFile(filepath.Join(storeDir, "user-2.yaml"), []byte(user2Cfg), os.ModePerm))

		// The following file is expected to be skipped.
		require.NoError(t, ioutil.WriteFile(filepath.Join(storeDir, "user-3.unsupported-extension"), []byte{}, os.ModePerm))

		users, err := store.ListAllUsers(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"user-1", "user-2"}, users)
	}
}

func TestStore_GetAlertConfig(t *testing.T) {
	ctx := context.Background()
	store, storeDir := prepareLocalStore(t)

	// The user has no config.
	{
		_, err := store.GetAlertConfig(ctx, "user-1")
		assert.Equal(t, alertspb.ErrNotFound, err)
	}

	// The user has a config
	{
		user1Cfg := prepareAlertmanagerConfig("user-1")
		user2Cfg := prepareAlertmanagerConfig("user-2")
		require.NoError(t, ioutil.WriteFile(filepath.Join(storeDir, "user-1.yaml"), []byte(user1Cfg), os.ModePerm))
		require.NoError(t, ioutil.WriteFile(filepath.Join(storeDir, "user-2.yaml"), []byte(user2Cfg), os.ModePerm))

		config, err := store.GetAlertConfig(ctx, "user-1")
		require.NoError(t, err)
		assert.Equal(t, user1Cfg, config.RawConfig)

		config, err = store.GetAlertConfig(ctx, "user-2")
		require.NoError(t, err)
		assert.Equal(t, user2Cfg, config.RawConfig)
	}
}

func TestStore_GetAlertConfigs(t *testing.T) {
	ctx := context.Background()
	store, storeDir := prepareLocalStore(t)

	// The storage is empty.
	{
		configs, err := store.GetAlertConfigs(ctx, []string{"user-1", "user-2"})
		require.NoError(t, err)
		assert.Empty(t, configs)
	}

	// The storage contains some configs.
	{
		user1Cfg := prepareAlertmanagerConfig("user-1")
		require.NoError(t, ioutil.WriteFile(filepath.Join(storeDir, "user-1.yaml"), []byte(user1Cfg), os.ModePerm))

		configs, err := store.GetAlertConfigs(ctx, []string{"user-1", "user-2"})
		require.NoError(t, err)
		assert.Contains(t, configs, "user-1")
		assert.NotContains(t, configs, "user-2")
		assert.Equal(t, user1Cfg, configs["user-1"].RawConfig)

		// Add another user config.
		user2Cfg := prepareAlertmanagerConfig("user-2")
		require.NoError(t, ioutil.WriteFile(filepath.Join(storeDir, "user-2.yaml"), []byte(user2Cfg), os.ModePerm))

		configs, err = store.GetAlertConfigs(ctx, []string{"user-1", "user-2"})
		require.NoError(t, err)
		assert.Contains(t, configs, "user-1")
		assert.Contains(t, configs, "user-2")
		assert.Equal(t, user1Cfg, configs["user-1"].RawConfig)
		assert.Equal(t, user2Cfg, configs["user-2"].RawConfig)
	}
}

func prepareLocalStore(t *testing.T) (store *Store, storeDir string) {
	var err error

	// Create a temporarily directory for the storage.
	storeDir, err = ioutil.TempDir(os.TempDir(), "local")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(storeDir))
	})

	store, err = NewStore(StoreConfig{Path: storeDir})
	require.NoError(t, err)
	return
}

func prepareAlertmanagerConfig(userID string) string {
	return fmt.Sprintf(`
global:
  smtp_smarthost: 'localhost:25'
  smtp_from: 'alertmanager@example.org'
  smtp_auth_username: 'alertmanager'
  smtp_auth_password: 'password'

route:
  receiver: send-email

receivers:
  - name: send-email
    email_configs:
      - to: '%s@localhost'
`, userID)
}
