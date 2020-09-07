package alertmanager

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cortexproject/cortex/pkg/alertmanager/alerts"
	"github.com/cortexproject/cortex/pkg/util"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v2"
)

func TestAMConfigValidationAPI(t *testing.T) {
	testCases := []struct {
		cfg    UserConfig
		hasErr bool
	}{
		{
			cfg: UserConfig{
				AlertmanagerConfig: `
route:
  receiver: 'default-receiver'
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  group_by: [cluster, alertname]
`,
			},
			hasErr: true,
		},
		{
			cfg: UserConfig{
				AlertmanagerConfig: `
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
			hasErr: false,
		},
		{
			cfg: UserConfig{
				AlertmanagerConfig: `
route:
  receiver: 'default-receiver'
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  group_by: [cluster, alertname]
receivers:
  - name: default-receiver
`,
				TemplateFiles: map[string]string{
					"good.tpl":          "good-template",
					"not/very/good.tpl": "bad-template", // Paths are not allowed in templates.
				},
			},
			hasErr: true,
		},
	}

	am := &MultitenantAlertmanager{
		store:  noopAlertStore{},
		logger: util.Logger,
	}
	for _, tc := range testCases {
		payload, err := yaml.Marshal(&tc.cfg)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "http://alertmanager/api/v1/alerts", bytes.NewReader(payload))
		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, "testing")
		require.NoError(t, user.InjectOrgIDIntoHTTPRequest(ctx, req))
		w := httptest.NewRecorder()
		am.SetUserConfig(w, req)

		resp := w.Result()
		if tc.hasErr {
			require.Equal(t, 400, resp.StatusCode)
			respBody, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)
			require.True(t, strings.Contains(string(respBody), "error validating Alertmanager config"))
		} else {
			require.Equal(t, 201, resp.StatusCode)
		}
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
