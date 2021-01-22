package alertmanager

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/alertmanager/alerts"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

func TestAMConfigValidationAPI(t *testing.T) {
	testCases := []struct {
		name     string
		cfg      string
		response string
		err      error
	}{
		{
			name: "It is not a valid payload without receivers",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
`,
			err: fmt.Errorf("error validating Alertmanager config: undefined receiver \"default-receiver\" used in route"),
		},
		{
			name: "It is valid",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
`,
		},
		{
			name: "It is not valid with paths in the template",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
template_files:
  "good.tpl": "good-templ"
  "not/very/good.tpl": "bad-template"
`,
			err: fmt.Errorf("error validating Alertmanager config: unable to create template file 'not/very/good.tpl'"),
		},
		{
			name: "It is not valid with .",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
template_files:
  "good.tpl": "good-templ"
  ".": "bad-template"
`,
			err: fmt.Errorf("error validating Alertmanager config: unable to create template file '.'"),
		},
		{
			name: "It is not valid if the config is empty due to wrong indendatation",
			cfg: `
alertmanager_config: |
route:
  receiver: 'default-receiver'
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  group_by: [cluster, alertname]
receivers:
  - name: default-receiver
template_files:
  "good.tpl": "good-templ"
  "not/very/good.tpl": "bad-template"
`,
			err: fmt.Errorf("error validating Alertmanager config: configuration provided is empty, if you'd like to remove your configuration please use the delete configuration endpoint"),
		},
		{
			name: "It is not valid if the config is empty due to wrong key",
			cfg: `
XWRONGalertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
template_files:
  "good.tpl": "good-templ"
  "not/very/good.tpl": "bad-template"
`,
			err: fmt.Errorf("error validating Alertmanager config: configuration provided is empty, if you'd like to remove your configuration please use the delete configuration endpoint"),
		},
	}

	am := &MultitenantAlertmanager{
		store:  noopAlertStore{},
		logger: util.Logger,
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://alertmanager/api/v1/alerts", bytes.NewReader([]byte(tc.cfg)))
			ctx := user.InjectOrgID(req.Context(), "testing")
			w := httptest.NewRecorder()
			am.SetUserConfig(w, req.WithContext(ctx))
			resp := w.Result()

			body, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			if tc.err == nil {
				require.Equal(t, http.StatusCreated, resp.StatusCode)
				require.Equal(t, "", string(body))
			} else {
				require.Equal(t, http.StatusBadRequest, resp.StatusCode)
				require.Equal(t, tc.err.Error()+"\n", string(body))
			}

		})
	}
}

type noopAlertStore struct{}

func (noopAlertStore) ListAlertConfigs(ctx context.Context) (map[string]alerts.AlertConfigDesc, error) {
	return nil, nil
}
func (noopAlertStore) GetAlertConfig(ctx context.Context, user string) (alerts.AlertConfigDesc, error) {
	return alerts.AlertConfigDesc{}, nil
}
func (noopAlertStore) SetAlertConfig(ctx context.Context, cfg alerts.AlertConfigDesc) error {
	return nil
}
func (noopAlertStore) DeleteAlertConfig(ctx context.Context, user string) error {
	return nil
}

func TestAMConfigListUserConfig(t *testing.T) {
	testCases := map[string]*UserConfig{
		"user1": {
			AlertmanagerConfig: `
global:
  resolve_timeout: 5m
route:
  receiver: route1
  group_by:
  - '...'
  continue: false
receivers:
- name: route1
  webhook_configs:
  - send_resolved: true
    http_config: {}
    url: http://alertmanager/api/notifications?orgId=1&rrid=7
    max_alerts: 0
`,
		},
		"user2": {
			AlertmanagerConfig: `
global:
  resolve_timeout: 5m
route:
  receiver: route1
  group_by:
  - '...'
  continue: false
receivers:
- name: route1
  webhook_configs:
  - send_resolved: true
    http_config: {}
    url: http://alertmanager/api/notifications?orgId=2&rrid=7
    max_alerts: 0
`,
		},
	}

	mockStore := &mockAlertStore{
		configs: map[string]alerts.AlertConfigDesc{},
	}

	for u, cfg := range testCases {
		err := mockStore.SetAlertConfig(context.Background(), alerts.AlertConfigDesc{
			User:      u,
			RawConfig: cfg.AlertmanagerConfig,
		})
		require.NoError(t, err)
	}

	externalURL := flagext.URLValue{}
	err := externalURL.Set("http://localhost:8080/alertmanager")
	require.NoError(t, err)

	tempDir, err := ioutil.TempDir(os.TempDir(), "alertmanager")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create the Multitenant Alertmanager.
	reg := prometheus.NewPedanticRegistry()
	cfg := mockAlertmanagerConfig(t)
	am, err := createMultitenantAlertmanager(cfg, nil, nil, mockStore, nil, log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), am))
	defer services.StopAndAwaitTerminated(context.Background(), am) //nolint:errcheck

	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)

	router := mux.NewRouter()
	router.Path("/multitenant_alertmanager/configs").Methods(http.MethodGet).HandlerFunc(am.ListUserConfig)
	// Request when no user configuration is present.
	req := httptest.NewRequest("GET", "https://localhost:8080/multitenant_alertmanager/configs", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/yaml", resp.Header.Get("Content-Type"))
	body, _ := ioutil.ReadAll(resp.Body)
	old, _ := yaml.Marshal(testCases)
	require.Equal(t, string(old), string(body))

	// It succeeds and the Alertmanager is started
	require.Len(t, am.alertmanagers, 2)
	require.True(t, am.alertmanagers["user1"].IsActive())
	require.True(t, am.alertmanagers["user2"].IsActive())
}
