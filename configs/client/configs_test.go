package client

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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
	expected := CortexConfigsResponse{Configs: map[string]CortexConfigView{
		"2": {
			ConfigID: 1,
			Config: CortexConfig{
				RulesFiles: map[string]string{
					"recording.rules": ":scope_authfe_request_duration_seconds:99quantile = histogram_quantile(0.99, sum(rate(scope_request_duration_seconds_bucket{ws=\"false\",job=\"authfe\",route!~\"(admin|metrics).*\"}[5m])) by (le))\n",
				},
			},
		},
	}}
	assert.Equal(t, &expected, observed)
}
