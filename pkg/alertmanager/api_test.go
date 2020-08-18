package alertmanager

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/alertmanager/alerts"

	"github.com/stretchr/testify/require"
)

func TestAMConfigValidation(t *testing.T) {
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

	for _, tc := range testCases {
		cfgDesc := alerts.ToProto(tc.cfg.AlertmanagerConfig, tc.cfg.TemplateFiles, "fake")
		err := validateUserConfig(cfgDesc)
		if tc.hasErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}
