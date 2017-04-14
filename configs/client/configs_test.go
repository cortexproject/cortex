package client

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/cortex/configs"
)

func TestJSONDecoding(t *testing.T) {
	observed, err := configsFromJSON(strings.NewReader(`
{
  "configs": {
    "2": {
      "id": 1,
      "config": {
        "rules_files": {
          "recording.rules": ":scope_authfe_request_duration_seconds:99quantile = histogram_quantile(0.99, sum(rate(scope_request_duration_seconds_bucket{ws=\"false\",job=\"authfe\",route!~\"(admin|metrics).*\"}[5m])) by (le))\n"
        }
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
				RulesFiles: map[string]string{
					"recording.rules": ":scope_authfe_request_duration_seconds:99quantile = histogram_quantile(0.99, sum(rate(scope_request_duration_seconds_bucket{ws=\"false\",job=\"authfe\",route!~\"(admin|metrics).*\"}[5m])) by (le))\n",
				},
			},
		},
	}}
	assert.Equal(t, &expected, observed)
}
