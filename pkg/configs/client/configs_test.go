package client

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/stretchr/testify/assert"
)

var response = `{
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
`

func TestDoRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(response))
		require.NoError(t, err)
	}))
	defer server.Close()

	resp, err := doRequest(server.URL, 1*time.Second, 0, "TestDoRequest")
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
	assert.Equal(t, &expected, resp)
}
