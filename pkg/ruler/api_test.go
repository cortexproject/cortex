package ruler

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestRuler_rules(t *testing.T) {
	cfg, cleanup := defaultRulerConfig(newMockRuleStore(mockRules))
	defer cleanup()

	r, rcleanup := newTestRuler(t, cfg)
	defer rcleanup()
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	req := httptest.NewRequest("GET", "https://localhost:8080/api/prom/api/v1/rules", nil)
	req.Header.Add(user.OrgIDHeaderName, "user1")
	w := httptest.NewRecorder()
	r.rules(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	// Check status code and status response
	responseJSON := response{}
	err := json.Unmarshal(body, &responseJSON)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, responseJSON.Status, "success")

	// Testing the running rules for user1 in the mock store
	expectedResponse, _ := json.Marshal(response{
		Status: "success",
		Data: &RuleDiscovery{
			RuleGroups: []*RuleGroup{
				{
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
	cfg, cleanup := defaultRulerConfig(newMockRuleStore(mockRules))
	defer cleanup()

	r, rcleanup := newTestRuler(t, cfg)
	defer rcleanup()
	defer r.StopAsync()

	req := httptest.NewRequest("GET", "https://localhost:8080/api/prom/api/v1/alerts", nil)
	req.Header.Add(user.OrgIDHeaderName, "user1")
	w := httptest.NewRecorder()
	r.alerts(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	// Check status code and status response
	responseJSON := response{}
	err := json.Unmarshal(body, &responseJSON)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, responseJSON.Status, "success")

	// Currently there is not an easy way to mock firing alerts. The empty
	// response case is tested instead.
	expectedResponse, _ := json.Marshal(response{
		Status: "success",
		Data: &AlertDiscovery{
			Alerts: []*Alert{},
		},
	})

	require.Equal(t, string(expectedResponse), string(body))
}
