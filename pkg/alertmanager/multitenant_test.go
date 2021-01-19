package alertmanager

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/http/pprof"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/alertmanager/alerts"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
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

func (m *mockAlertStore) GetAlertConfig(ctx context.Context, user string) (alerts.AlertConfigDesc, error) {
	return alerts.AlertConfigDesc{}, fmt.Errorf("not implemented")
}

func (m *mockAlertStore) SetAlertConfig(ctx context.Context, cfg alerts.AlertConfigDesc) error {
	m.configs[cfg.User] = cfg
	return nil
}

func (m *mockAlertStore) DeleteAlertConfig(ctx context.Context, user string) error {
	return fmt.Errorf("not implemented")
}

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
		# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
		# TYPE cortex_alertmanager_config_last_reload_successful gauge
		cortex_alertmanager_config_last_reload_successful{user="user1"} 1
		cortex_alertmanager_config_last_reload_successful{user="user2"} 1
	`), "cortex_alertmanager_config_last_reload_successful"))

	// Ensure when a 3rd config is added, it is synced correctly
	mockStore.configs["user3"] = alerts.AlertConfigDesc{
		User:      "user3",
		RawConfig: simpleConfigOne,
		Templates: []*alerts.TemplateDesc{},
	}

	require.NoError(t, am.updateConfigs())
	require.Len(t, am.alertmanagers, 3)

	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
		# TYPE cortex_alertmanager_config_last_reload_successful gauge
		cortex_alertmanager_config_last_reload_successful{user="user1"} 1
		cortex_alertmanager_config_last_reload_successful{user="user2"} 1
		cortex_alertmanager_config_last_reload_successful{user="user3"} 1
	`), "cortex_alertmanager_config_last_reload_successful"))

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
		# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
		# TYPE cortex_alertmanager_config_last_reload_successful gauge
		cortex_alertmanager_config_last_reload_successful{user="user1"} 1
		cortex_alertmanager_config_last_reload_successful{user="user2"} 1
	`), "cortex_alertmanager_config_last_reload_successful"))

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
		# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
		# TYPE cortex_alertmanager_config_last_reload_successful gauge
		cortex_alertmanager_config_last_reload_successful{user="user1"} 1
		cortex_alertmanager_config_last_reload_successful{user="user2"} 1
		cortex_alertmanager_config_last_reload_successful{user="user3"} 1
	`), "cortex_alertmanager_config_last_reload_successful"))
}

func TestAlertmanager_NoExternalURL(t *testing.T) {
	tempDir, err := ioutil.TempDir(os.TempDir(), "alertmanager")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create the Multitenant Alertmanager.
	reg := prometheus.NewPedanticRegistry()
	_, err = NewMultitenantAlertmanager(&MultitenantAlertmanagerConfig{
		DataDir: tempDir,
	}, log.NewNopLogger(), reg)

	require.EqualError(t, err, "unable to create Alertmanager because the external URL has not been configured")
}

func TestAlertmanager_ServeHTTP(t *testing.T) {
	mockStore := &mockAlertStore{
		configs: map[string]alerts.AlertConfigDesc{},
	}

	externalURL := flagext.URLValue{}
	err := externalURL.Set("http://localhost:8080/alertmanager")
	require.NoError(t, err)

	tempDir, err := ioutil.TempDir(os.TempDir(), "alertmanager")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create the Multitenant Alertmanager.
	reg := prometheus.NewPedanticRegistry()
	am := createMultitenantAlertmanager(&MultitenantAlertmanagerConfig{
		ExternalURL:  externalURL,
		DataDir:      tempDir,
		PollInterval: time.Minute,
	}, nil, nil, mockStore, log.NewNopLogger(), reg)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), am))
	defer services.StopAndAwaitTerminated(context.Background(), am) //nolint:errcheck

	// Request when no user configuration is present.
	req := httptest.NewRequest("GET", externalURL.String(), nil)
	ctx := user.InjectOrgID(req.Context(), "user1")

	{
		w := httptest.NewRecorder()
		am.ServeHTTP(w, req.WithContext(ctx))

		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)
		require.Equal(t, 404, w.Code)
		require.Equal(t, "the Alertmanager is not configured\n", string(body))
	}

	// Create a configuration for the user in storage.
	mockStore.configs["user1"] = alerts.AlertConfigDesc{
		User:      "user1",
		RawConfig: simpleConfigTwo,
		Templates: []*alerts.TemplateDesc{},
	}

	// Make the alertmanager pick it up, then pause it.
	err = am.updateConfigs()
	require.NoError(t, err)

	// Request when AM is active.
	{
		w := httptest.NewRecorder()
		am.ServeHTTP(w, req.WithContext(ctx))

		require.Equal(t, 301, w.Code) // redirect to UI
	}

	// Verify that GET /metrics returns 404 even when AM is active.
	{
		metricURL := externalURL.String() + "/metrics"
		require.Equal(t, "http://localhost:8080/alertmanager/metrics", metricURL)
		verify404(ctx, t, am, "GET", metricURL)
	}

	// Verify that POST /-/reload returns 404 even when AM is active.
	{
		metricURL := externalURL.String() + "/-/reload"
		require.Equal(t, "http://localhost:8080/alertmanager/-/reload", metricURL)
		verify404(ctx, t, am, "POST", metricURL)
	}

	// Verify that GET /debug/index returns 404 even when AM is active.
	{
		// Register pprof Index (under non-standard path, but this path is exposed by AM using default MUX!)
		http.HandleFunc("/alertmanager/debug/index", pprof.Index)

		metricURL := externalURL.String() + "/debug/index"
		require.Equal(t, "http://localhost:8080/alertmanager/debug/index", metricURL)
		verify404(ctx, t, am, "GET", metricURL)
	}

	// Pause alert manager.
	am.alertmanagers["user1"].Pause()

	{
		// Request when user configuration is paused.
		w := httptest.NewRecorder()
		am.ServeHTTP(w, req.WithContext(ctx))

		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)
		require.Equal(t, 404, w.Code)
		require.Equal(t, "the Alertmanager is not configured\n", string(body))
	}
}

func verify404(ctx context.Context, t *testing.T, am *MultitenantAlertmanager, method string, url string) {
	metricsReq := httptest.NewRequest(method, url, strings.NewReader("Hello")) // Body for POST Request.
	w := httptest.NewRecorder()
	am.ServeHTTP(w, metricsReq.WithContext(ctx))

	require.Equal(t, 404, w.Code)
}

func TestAlertmanager_ServeHTTPWithFallbackConfig(t *testing.T) {
	mockStore := &mockAlertStore{
		configs: map[string]alerts.AlertConfigDesc{},
	}

	externalURL := flagext.URLValue{}
	err := externalURL.Set("http://localhost:8080/alertmanager")
	require.NoError(t, err)

	tempDir, err := ioutil.TempDir(os.TempDir(), "alertmanager")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	fallbackCfg := `
global:
  smtp_smarthost: 'localhost:25'
  smtp_from: 'youraddress@example.org'
route:
  receiver: example-email
receivers:
  - name: example-email
    email_configs:
    - to: 'youraddress@example.org'
`

	// Create the Multitenant Alertmanager.
	am := createMultitenantAlertmanager(&MultitenantAlertmanagerConfig{
		ExternalURL:  externalURL,
		DataDir:      tempDir,
		PollInterval: time.Minute,
	}, nil, nil, mockStore, log.NewNopLogger(), nil)
	am.fallbackConfig = fallbackCfg

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), am))
	defer services.StopAndAwaitTerminated(context.Background(), am) //nolint:errcheck

	// Request when no user configuration is present.
	req := httptest.NewRequest("GET", externalURL.String()+"/api/v1/status", nil)
	ctx := user.InjectOrgID(req.Context(), "user1")
	w := httptest.NewRecorder()

	am.ServeHTTP(w, req.WithContext(ctx))

	resp := w.Result()

	// It succeeds and the Alertmanager is started
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, am.alertmanagers, 1)
	require.True(t, am.alertmanagers["user1"].IsActive())

	// Even after a poll it does not pause your Alertmanager
	err = am.updateConfigs()
	require.NoError(t, err)

	require.True(t, am.alertmanagers["user1"].IsActive())
	require.Len(t, am.alertmanagers, 1)

	// Pause the alertmanager
	am.alertmanagers["user1"].Pause()

	// Request when user configuration is paused.
	w = httptest.NewRecorder()
	am.ServeHTTP(w, req.WithContext(ctx))

	resp = w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	require.Equal(t, "the Alertmanager is not configured\n", string(body))
}
