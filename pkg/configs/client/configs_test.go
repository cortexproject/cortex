package client

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/cortex/pkg/configs"
)

func TestJSONDecoding(t *testing.T) {
	observed, err := configsFromJSON(strings.NewReader(`
{
  "configs": {
    "2": {
      "id": 1,
      "config": {
        "rules_files": {
          "recording.rules": "groups:\n- name: demo-service-alerts\n  interval: 15s\n  rules:\n  - alert: SomethingIsUp\n    expr: up == 1\n"
				},
				"rule_format_version": "2"
      }
    }
  }
}
`))
	assert.Nil(t, err)
	expected := ConfigsResponse{Configs: map[string]configs.View{
		"2": {
			ID: 1,
			Config: configs.Config{
				RulesConfig: configs.RulesConfig{
					Files: map[string]string{
						"recording.rules": "groups:\n- name: demo-service-alerts\n  interval: 15s\n  rules:\n  - alert: SomethingIsUp\n    expr: up == 1\n",
					},
					FormatVersion: configs.RuleFormatV2,
				},
			},
		},
	}}
	assert.Equal(t, &expected, observed)
}
