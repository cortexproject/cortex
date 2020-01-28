package ruler

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

func TestRuler_rules(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "ruler-tests")
	defer os.RemoveAll(dir)

	require.NoError(t, err)
	cfg := defaultRulerConfig()
	cfg.RulePath = dir

	r := newTestRuler(t, cfg)
	defer r.Stop()

	req := httptest.NewRequest("GET", "https://localhost:8080/api/prom/api/v1/rules", nil)
	req.Header.Add(user.OrgIDHeaderName, "user1")
	w := httptest.NewRecorder()
	r.rules(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	responseJSON := response{}
	json.Unmarshal(body, &responseJSON)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, responseJSON.Status, "success")

	expectedResponse, _ := json.Marshal(response{
		Status: "success",
		Data: &RuleDiscovery{
			RuleGroups: []*RuleGroup{
				&RuleGroup{
					Name: "group1",
					File: "namespace1",
					Rules: []rule{
						&recordingRule{
							Name:   "UP_RULE",
							Query:  "up",
							Health: "unknown",
							Type:   "recording",
						},
						&alertingRule{
							Name:   "UP_ALERT",
							Query:  "up < 1",
							State:  "inactive",
							Health: "unknown",
							Type:   "alerting",
							Alerts: []*Alert{},
						},
					},
					Interval: 60,
				},
			},
		},
	})

	require.Equal(t, string(expectedResponse), string(body))
}

func TestRuler_alerts(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "ruler-tests")
	defer os.RemoveAll(dir)

	require.NoError(t, err)
	cfg := defaultRulerConfig()
	cfg.RulePath = dir

	r := newTestRuler(t, cfg)
	defer r.Stop()

	req := httptest.NewRequest("GET", "https://localhost:8080/api/prom/api/v1/alerts", nil)
	req.Header.Add(user.OrgIDHeaderName, "user1")
	w := httptest.NewRecorder()
	r.alerts(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	responseJSON := response{}
	json.Unmarshal(body, &responseJSON)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, responseJSON.Status, "success")

	expectedResponse, _ := json.Marshal(response{
		Status: "success",
		Data: &AlertDiscovery{
			Alerts: []*Alert{},
		},
	})

	require.Equal(t, string(expectedResponse), string(body))
}
