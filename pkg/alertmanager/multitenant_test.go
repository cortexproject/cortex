package alertmanager

import (
	"context"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/alertmanager/alerts"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

var (
	simpleConfigOne = `route:
  receiver: dummy

receivers:
  - name: dummy`

	simpleConfigTwo = `route:
  receiver: dummy

receivers:
  - name: dummy`
)

// basic easily configurable mock
type mockAlertStore struct {
	configs map[string]alerts.AlertConfigDesc
}

func (m *mockAlertStore) ListAlertConfigs(ctx context.Context) (map[string]alerts.AlertConfigDesc, error) {
	return m.configs, nil
}

func TestMultitenantAlertmanager_loadAllConfigs(t *testing.T) {
	mockStore := &mockAlertStore{
		configs: map[string]alerts.AlertConfigDesc{
			"user1": {
				User:      "user1",
				RawConfig: simpleConfigOne,
				Templates: []*alerts.TemplateDesc{},
			},
			"user2": {
				User:      "user2",
				RawConfig: simpleConfigOne,
				Templates: []*alerts.TemplateDesc{},
			},
		},
	}

	externalURL := flagext.URLValue{}
	err := externalURL.Set("http://localhost/api/prom")
	require.NoError(t, err)

	tempDir, err := ioutil.TempDir(os.TempDir(), "alertmanager")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	am := &MultitenantAlertmanager{
		cfg: &MultitenantAlertmanagerConfig{
			ExternalURL: externalURL,
			DataDir:     tempDir,
		},
		store:            mockStore,
		cfgs:             map[string]alerts.AlertConfigDesc{},
		alertmanagersMtx: sync.Mutex{},
		alertmanagers:    map[string]*Alertmanager{},
		stop:             make(chan struct{}),
		done:             make(chan struct{}),
	}

	// Ensure the configs are synced correctly
	err = am.updateConfigs()
	require.NoError(t, err)
	require.Len(t, am.alertmanagers, 2)

	currentConfig, exists := am.cfgs["user1"]
	require.True(t, exists)
	require.Equal(t, simpleConfigOne, currentConfig.RawConfig)

	// Ensure when a 3rd config is added, it is synced correctly
	mockStore.configs["user3"] = alerts.AlertConfigDesc{
		User:      "user3",
		RawConfig: simpleConfigOne,
		Templates: []*alerts.TemplateDesc{},
	}
	err = am.updateConfigs()
	require.NoError(t, err)
	require.Len(t, am.alertmanagers, 3)

	// Ensure the config is updated
	mockStore.configs["user1"] = alerts.AlertConfigDesc{
		User:      "user1",
		RawConfig: simpleConfigTwo,
		Templates: []*alerts.TemplateDesc{},
	}
	err = am.updateConfigs()
	require.NoError(t, err)

	currentConfig, exists = am.cfgs["user1"]
	require.True(t, exists)
	require.Equal(t, simpleConfigTwo, currentConfig.RawConfig)

	// Test deletion of user config
	delete(mockStore.configs, "user1")
	err = am.updateConfigs()
	require.NoError(t, err)
	require.Len(t, am.alertmanagers, 2)
}
