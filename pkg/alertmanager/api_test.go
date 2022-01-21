package alertmanager

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/client_golang/prometheus"
	commoncfg "github.com/prometheus/common/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/bucketclient"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestAMConfigValidationAPI(t *testing.T) {
	testCases := []struct {
		name            string
		cfg             string
		maxConfigSize   int
		maxTemplates    int
		maxTemplateSize int

		response string
		err      error
	}{
		{
			name: "Should return error if the alertmanager config contains no receivers",
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
			name: "Should pass if the alertmanager config is valid",
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
			name: "Should return error if the config is empty due to wrong indentation",
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
			name: "Should return error if the alertmanager config is empty due to wrong key",
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
`,
			err: fmt.Errorf("error validating Alertmanager config: configuration provided is empty, if you'd like to remove your configuration please use the delete configuration endpoint"),
		},
		{
			name: "Should return error if the external template file name contains an absolute path",
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
  "/absolute/filepath": "a simple template"
`,
			err: fmt.Errorf(`error validating Alertmanager config: invalid template name "/absolute/filepath": the template name cannot contain any path`),
		},
		{
			name: "Should return error if the external template file name contains a relative path",
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
  "../filepath": "a simple template"
`,
			err: fmt.Errorf(`error validating Alertmanager config: invalid template name "../filepath": the template name cannot contain any path`),
		},
		{
			name: "Should return error if the external template file name is not a valid filename",
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
			err: fmt.Errorf("error validating Alertmanager config: unable to store template file '.'"),
		},
		{
			name: "Should return error if the referenced template contains the root /",
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
  templates:
    - "/"
`,
			err: fmt.Errorf(`error validating Alertmanager config: invalid template name "/": the template name cannot contain any path`),
		},
		{
			name: "Should return error if the referenced template contains the root with repeated separators ///",
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
  templates:
    - "///"
`,
			err: fmt.Errorf(`error validating Alertmanager config: invalid template name "///": the template name cannot contain any path`),
		},
		{
			name: "Should return error if the referenced template contains an absolute path",
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
  templates:
    - "/absolute/filepath"
`,
			err: fmt.Errorf(`error validating Alertmanager config: invalid template name "/absolute/filepath": the template name cannot contain any path`),
		},
		{
			name: "Should return error if the referenced template contains a relative path",
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
  templates:
    - "../filepath"
`,
			err: fmt.Errorf(`error validating Alertmanager config: invalid template name "../filepath": the template name cannot contain any path`),
		},
		{
			name: "Should pass if the referenced template is valid filename",
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
  templates:
    - "something.tmpl"
`,
		},
		{
			name: "Should return error if global HTTP password_file is set",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      basic_auth:
        password_file: /secrets

  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: errors.Wrap(errPasswordFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if global HTTP bearer_token_file is set",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      bearer_token_file: /secrets

  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: errors.Wrap(errPasswordFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if global HTTP credentials_file is set",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      authorization:
        credentials_file: /secrets

  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: errors.Wrap(errPasswordFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if global OAuth2 client_secret_file is set",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      oauth2:
        client_id: test
        token_url: http://example.com
        client_secret_file: /secrets

  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: errors.Wrap(errOAuth2SecretFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if receiver's HTTP password_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost
          http_config:
            basic_auth:
              password_file: /secrets

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errPasswordFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if receiver's HTTP bearer_token_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost
          http_config:
            bearer_token_file: /secrets

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errPasswordFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if receiver's HTTP credentials_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost
          http_config:
            authorization:
              credentials_file: /secrets

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errPasswordFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if receiver's OAuth2 client_secret_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost
          http_config:
            oauth2:
              client_id: test
              token_url: http://example.com
              client_secret_file: /secrets

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errOAuth2SecretFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if receiver's HTTP proxy_url is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost
          http_config:
            proxy_url: http://localhost

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errProxyURLNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if global slack_api_url_file is set",
			cfg: `
alertmanager_config: |
  global:
    slack_api_url_file: /secrets

  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errSlackAPIURLFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if Slack api_url_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      slack_configs:
        - api_url_file: /secrets

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errSlackAPIURLFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if VictorOps api_key_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      victorops_configs:
        - api_key_file: /secrets
          api_key: my-key
          routing_key: test

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errVictorOpsAPIKeyFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "should return error if template is wrong",
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
  templates:
    - "*.tmpl"
template_files:
  "test.tmpl": "{{ invalid Go template }}"
`,
			err: fmt.Errorf(`error validating Alertmanager config: template: test.tmpl:1: function "invalid" not defined`),
		},
		{
			name: "config too big",
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
			maxConfigSize: 10,
			err:           fmt.Errorf(errConfigurationTooBig, 10),
		},
		{
			name: "config size OK",
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
			maxConfigSize: 1000,
			err:           nil,
		},
		{
			name: "templates limit reached",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
template_files:
  "t1.tmpl": "Some template"
  "t2.tmpl": "Some template"
  "t3.tmpl": "Some template"
  "t4.tmpl": "Some template"
  "t5.tmpl": "Some template"
`,
			maxTemplates: 3,
			err:          errors.Wrap(fmt.Errorf(errTooManyTemplates, 5, 3), "error validating Alertmanager config"),
		},
		{
			name: "templates limit not reached",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
template_files:
  "t1.tmpl": "Some template"
  "t2.tmpl": "Some template"
  "t3.tmpl": "Some template"
  "t4.tmpl": "Some template"
  "t5.tmpl": "Some template"
`,
			maxTemplates: 10,
			err:          nil,
		},
		{
			name: "template size limit reached",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
template_files:
  "t1.tmpl": "Very big template"
`,
			maxTemplateSize: 5,
			err:             errors.Wrap(fmt.Errorf(errTemplateTooBig, "t1.tmpl", 17, 5), "error validating Alertmanager config"),
		},
		{
			name: "template size limit ok",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
template_files:
  "t1.tmpl": "Very big template"
`,
			maxTemplateSize: 20,
			err:             nil,
		},
	}

	limits := &mockAlertManagerLimits{}
	am := &MultitenantAlertmanager{
		store:  prepareInMemoryAlertStore(),
		logger: util_log.Logger,
		limits: limits,
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			limits.maxConfigSize = tc.maxConfigSize
			limits.maxTemplatesCount = tc.maxTemplates
			limits.maxSizeOfTemplate = tc.maxTemplateSize

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

func TestMultitenantAlertmanager_DeleteUserConfig(t *testing.T) {
	storage := objstore.NewInMemBucket()
	alertStore := bucketclient.NewBucketAlertStore(storage, nil, log.NewNopLogger())

	am := &MultitenantAlertmanager{
		store:  alertStore,
		logger: util_log.Logger,
	}

	require.NoError(t, alertStore.SetAlertConfig(context.Background(), alertspb.AlertConfigDesc{
		User:      "test_user",
		RawConfig: "config",
	}))

	require.Equal(t, 1, len(storage.Objects()))

	req := httptest.NewRequest("POST", "/multitenant_alertmanager/delete_tenant_config", nil)
	// Missing user returns error 401. (DeleteUserConfig does this, but in practice, authentication middleware will do it first)
	{
		rec := httptest.NewRecorder()
		am.DeleteUserConfig(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Equal(t, 1, len(storage.Objects()))
	}

	// With user in the context.
	ctx := user.InjectOrgID(context.Background(), "test_user")
	req = req.WithContext(ctx)
	{
		rec := httptest.NewRecorder()
		am.DeleteUserConfig(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, 0, len(storage.Objects()))
	}

	// Repeating the request still reports 200
	{
		rec := httptest.NewRecorder()
		am.DeleteUserConfig(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, 0, len(storage.Objects()))
	}
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

	storage := objstore.NewInMemBucket()
	alertStore := bucketclient.NewBucketAlertStore(storage, nil, log.NewNopLogger())

	for u, cfg := range testCases {
		err := alertStore.SetAlertConfig(context.Background(), alertspb.AlertConfigDesc{
			User:      u,
			RawConfig: cfg.AlertmanagerConfig,
		})
		require.NoError(t, err)
	}

	externalURL := flagext.URLValue{}
	err := externalURL.Set("http://localhost:8080/alertmanager")
	require.NoError(t, err)

	// Create the Multitenant Alertmanager.
	reg := prometheus.NewPedanticRegistry()
	cfg := mockAlertmanagerConfig(t)
	am, err := createMultitenantAlertmanager(cfg, nil, nil, alertStore, nil, nil, log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), am))
	defer services.StopAndAwaitTerminated(context.Background(), am) //nolint:errcheck

	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)
	require.Len(t, am.alertmanagers, 2)

	router := mux.NewRouter()
	router.Path("/multitenant_alertmanager/configs").Methods(http.MethodGet).HandlerFunc(am.ListAllConfigs)
	req := httptest.NewRequest("GET", "https://localhost:8080/multitenant_alertmanager/configs", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/yaml", resp.Header.Get("Content-Type"))
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	old, err := yaml.Marshal(testCases)
	require.NoError(t, err)
	require.YAMLEq(t, string(old), string(body))
}

func TestValidateAlertmanagerConfig(t *testing.T) {
	tests := map[string]struct {
		input    interface{}
		expected error
	}{
		"*HTTPClientConfig": {
			input: &commoncfg.HTTPClientConfig{
				BasicAuth: &commoncfg.BasicAuth{
					PasswordFile: "/secrets",
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"HTTPClientConfig": {
			input: commoncfg.HTTPClientConfig{
				BasicAuth: &commoncfg.BasicAuth{
					PasswordFile: "/secrets",
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"*TLSConfig": {
			input: &commoncfg.TLSConfig{
				CertFile: "/cert",
			},
			expected: errTLSFileNotAllowed,
		},
		"TLSConfig": {
			input: commoncfg.TLSConfig{
				CertFile: "/cert",
			},
			expected: errTLSFileNotAllowed,
		},
		"struct containing *HTTPClientConfig as direct child": {
			input: config.GlobalConfig{
				HTTPConfig: &commoncfg.HTTPClientConfig{
					BasicAuth: &commoncfg.BasicAuth{
						PasswordFile: "/secrets",
					},
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"struct containing *HTTPClientConfig as nested child": {
			input: config.Config{
				Global: &config.GlobalConfig{
					HTTPConfig: &commoncfg.HTTPClientConfig{
						BasicAuth: &commoncfg.BasicAuth{
							PasswordFile: "/secrets",
						},
					},
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"struct containing *HTTPClientConfig as nested child within a slice": {
			input: config.Config{
				Receivers: []*config.Receiver{{
					Name: "test",
					WebhookConfigs: []*config.WebhookConfig{{
						HTTPConfig: &commoncfg.HTTPClientConfig{
							BasicAuth: &commoncfg.BasicAuth{
								PasswordFile: "/secrets",
							},
						},
					}}},
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"map containing *HTTPClientConfig": {
			input: map[string]*commoncfg.HTTPClientConfig{
				"test": {
					BasicAuth: &commoncfg.BasicAuth{
						PasswordFile: "/secrets",
					},
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"map containing TLSConfig as nested child": {
			input: map[string][]config.EmailConfig{
				"test": {{
					TLSConfig: commoncfg.TLSConfig{
						CAFile: "/file",
					},
				}},
			},
			expected: errTLSFileNotAllowed,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			err := validateAlertmanagerConfig(testData.input)
			assert.ErrorIs(t, err, testData.expected)
		})
	}
}
