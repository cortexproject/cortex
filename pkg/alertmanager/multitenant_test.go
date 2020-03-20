// +build !race

package alertmanager

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
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

// TestLoadAllConfigs ensures the multitenant alertmanager can properly load configs from a local backend store.
// It is excluded from the race detector due to a vendored race issue https://github.com/prometheus/alertmanager/issues/2182
func TestLoadAllConfigs(t *testing.T) {
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

	reg := prometheus.NewPedanticRegistry()
	am := createMultitenantAlertmanager(&MultitenantAlertmanagerConfig{
		ExternalURL: externalURL,
		DataDir:     tempDir,
	}, nil, nil, mockStore, log.NewNopLogger(), reg)

	// Ensure the configs are synced correctly
	require.NoError(t, am.updateConfigs())
	require.Len(t, am.alertmanagers, 2)

	currentConfig, exists := am.cfgs["user1"]
	require.True(t, exists)
	require.Equal(t, simpleConfigOne, currentConfig.RawConfig)

	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_configs How many configs the multitenant alertmanager knows about.
		# TYPE cortex_alertmanager_configs gauge
		cortex_alertmanager_configs{status="valid"} 2
		cortex_alertmanager_configs{status="invalid"} 0
	`), "cortex_alertmanager_configs"))

	// Ensure when a 3rd config is added, it is synced correctly
	mockStore.configs["user3"] = alerts.AlertConfigDesc{
		User:      "user3",
		RawConfig: simpleConfigOne,
		Templates: []*alerts.TemplateDesc{},
	}

	require.NoError(t, am.updateConfigs())
	require.Len(t, am.alertmanagers, 3)

	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_configs How many configs the multitenant alertmanager knows about.
		# TYPE cortex_alertmanager_configs gauge
		cortex_alertmanager_configs{status="valid"} 3
		cortex_alertmanager_configs{status="invalid"} 0
	`), "cortex_alertmanager_configs"))

	// Ensure the config is updated
	mockStore.configs["user1"] = alerts.AlertConfigDesc{
		User:      "user1",
		RawConfig: simpleConfigTwo,
		Templates: []*alerts.TemplateDesc{},
	}

	require.NoError(t, am.updateConfigs())

	currentConfig, exists = am.cfgs["user1"]
	require.True(t, exists)
	require.Equal(t, simpleConfigTwo, currentConfig.RawConfig)

	// Test Delete User, ensure config is remove but alertmananger
	// exists and is set to inactive
	delete(mockStore.configs, "user3")
	require.NoError(t, am.updateConfigs())
	currentConfig, exists = am.cfgs["user3"]
	require.False(t, exists)
	require.Equal(t, "", currentConfig.RawConfig)

	userAM, exists := am.alertmanagers["user3"]
	require.True(t, exists)
	require.False(t, userAM.IsActive())

	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_configs How many configs the multitenant alertmanager knows about.
		# TYPE cortex_alertmanager_configs gauge
		cortex_alertmanager_configs{status="valid"} 2
		cortex_alertmanager_configs{status="invalid"} 0
	`), "cortex_alertmanager_configs"))

	// Ensure when a 3rd config is re-added, it is synced correctly
	mockStore.configs["user3"] = alerts.AlertConfigDesc{
		User:      "user3",
		RawConfig: simpleConfigOne,
		Templates: []*alerts.TemplateDesc{},
	}

	require.NoError(t, am.updateConfigs())

	currentConfig, exists = am.cfgs["user3"]
	require.True(t, exists)
	require.Equal(t, simpleConfigOne, currentConfig.RawConfig)

	userAM, exists = am.alertmanagers["user3"]
	require.True(t, exists)
	require.True(t, userAM.IsActive())

	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_configs How many configs the multitenant alertmanager knows about.
		# TYPE cortex_alertmanager_configs gauge
		cortex_alertmanager_configs{status="valid"} 3
		cortex_alertmanager_configs{status="invalid"} 0
	`), "cortex_alertmanager_configs"))
}
