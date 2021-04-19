package alertmanager

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/bucketclient"
	util_log "github.com/cortexproject/cortex/pkg/util/log"

	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/config"
	commoncfg "github.com/prometheus/common/config"
	"github.com/stretchr/testify/assert"
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
	}

	am := &MultitenantAlertmanager{
		store:  prepareInMemoryAlertStore(),
		logger: util_log.Logger,
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
			assert.True(t, errors.Is(err, testData.expected))
		})
	}
}
