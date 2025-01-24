package ruler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	util_api "github.com/cortexproject/cortex/pkg/util/api"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestAPIResponseSerialization(t *testing.T) {
	lastEvalTime := time.Now()
	responseTime := lastEvalTime.Format(time.RFC3339Nano)
	testCases := map[string]struct {
		rules        RuleDiscovery
		expectedJSON string
	}{
		"No rules": {
			rules: RuleDiscovery{
				RuleGroups: make([]*RuleGroup, 0),
			},
			expectedJSON: `{
				"groups":[]
			}`,
		},
		"Rules with no next token": {
			rules: RuleDiscovery{
				RuleGroups: []*RuleGroup{
					{
						Name:           "Test",
						File:           "/rules/Test",
						Rules:          make([]rule, 0),
						Interval:       60,
						LastEvaluation: lastEvalTime,
						EvaluationTime: 10,
						Limit:          0,
					},
				},
			},
			expectedJSON: fmt.Sprintf(`{
				"groups": [
					{
						"evaluationTime": 10,
						"limit": 0,
						"name": "Test",
						"file": "/rules/Test",
						"interval": 60,
						"rules": [],
						"lastEvaluation": "%s"
					}
				]
			}`, responseTime),
		},
		"Rules with next token": {
			rules: RuleDiscovery{
				RuleGroups: []*RuleGroup{
					{
						Name:           "Test",
						File:           "/rules/Test",
						Rules:          make([]rule, 0),
						Interval:       60,
						LastEvaluation: lastEvalTime,
						EvaluationTime: 10,
						Limit:          0,
					},
				},
				GroupNextToken: "abcdef",
			},
			expectedJSON: fmt.Sprintf(`{
				"groups": [
					{
						"evaluationTime": 10,
						"limit": 0,
						"name": "Test",
						"file": "/rules/Test",
						"interval": 60,
						"rules": [],
						"lastEvaluation": "%s"
					}
				],
				"groupNextToken": "abcdef"
			}`, responseTime),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			data, err := json.Marshal(&tc.rules)
			require.NoError(t, err)
			require.JSONEq(t, tc.expectedJSON, string(data))
		})
	}
}

func Test_stripEvaluationFields(t *testing.T) {
	input := `
{
    "data": {
        "groups": [
            {
                "evaluationTime": 0.00119525,
                "file": ")(_+?/|namespace1+/?",
                "interval": 10,
                "lastEvaluation": "2025-01-24T12:04:26.440399-08:00",
                "limit": 0,
                "name": ")(_+?/|group1+/?",
                "rules": [
                    {
                        "evaluationTime": 0.000976083,
                        "health": "ok",
                        "labels": {},
                        "lastError": "",
                        "lastEvaluation": "2025-01-24T12:04:26.440437-08:00",
                        "name": "UP_RULE",
                        "query": "up",
                        "type": "recording"
                    },
                    {
                        "alerts": [],
                        "annotations": {},
                        "duration": 0,
                        "evaluationTime": 0.000172375,
                        "health": "ok",
                        "keepFiringFor": 0,
                        "labels": {},
                        "lastError": "",
                        "lastEvaluation": "2025-01-24T12:04:26.441418-08:00",
                        "name": "UP_ALERT",
                        "query": "up < 1",
                        "state": "inactive",
                        "type": "alerting"
                    }
                ]
            }
        ]
    },
    "status": "success"
}`
	inputResponse := util_api.Response{}
	err := json.Unmarshal([]byte(input), &inputResponse)
	require.NoError(t, err)
	stripEvaluationFields(t, inputResponse)
	output, err := json.Marshal(inputResponse)
	require.NoError(t, err)

	expected := `
{
    "data": {
        "groups": [
            {
                "evaluationTime": 0,
                "file": ")(_+?/|namespace1+/?",
                "interval": 10,
                "lastEvaluation": "0001-01-01T00:00:00Z",
                "limit": 0,
                "name": ")(_+?/|group1+/?",
                "rules": [
                    {
                        "evaluationTime": 0,
                        "health": "unknown",
                        "labels": {},
                        "lastError": "",
                        "lastEvaluation": "0001-01-01T00:00:00Z",
                        "name": "UP_RULE",
                        "query": "up",
                        "type": "recording"
                    },
                    {
                        "alerts": [],
                        "annotations": {},
                        "duration": 0,
                        "evaluationTime": 0,
                        "health": "unknown",
                        "keepFiringFor": 0,
                        "labels": {},
                        "lastError": "",
                        "lastEvaluation": "0001-01-01T00:00:00Z",
                        "name": "UP_ALERT",
                        "query": "up < 1",
                        "state": "inactive",
                        "type": "alerting"
                    }
                ]
            }
        ]
    },
    "status": "success"
}`

	require.JSONEq(t, expected, string(output))
}

// stripEvaluationFields sets evaluation-related fields of a rules API response to zero values.
func stripEvaluationFields(t *testing.T, r util_api.Response) {
	dataMap, ok := r.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map[string]interface{} got %T", r.Data)
	}

	groups, ok := dataMap["groups"].([]interface{})
	if !ok {
		t.Fatalf("expected []interface{} got %T", dataMap["groups"])
	}

	for i := range groups {
		group, ok := groups[i].(map[string]interface{})
		if !ok {
			t.Fatalf("expected map[string]interface{} got %T", groups[i])
		}
		group["evaluationTime"] = 0
		group["lastEvaluation"] = "0001-01-01T00:00:00Z"

		rules, ok := group["rules"].([]interface{})
		if !ok {
			t.Fatalf("expected []interface{} got %T", group["rules"])
		}

		for i := range rules {
			rule, ok := rules[i].(map[string]interface{})
			if !ok {
				t.Fatalf("expected map[string]interface{} got %T", rules[i])
			}
			rule["health"] = "unknown"
			rule["evaluationTime"] = 0
			rule["lastEvaluation"] = "0001-01-01T00:00:00Z"
			rules[i] = rule
		}
		group["rules"] = rules
		groups[i] = group
	}

	dataMap["groups"] = groups
	r.Data = dataMap
}

func TestRuler_rules(t *testing.T) {
	store := newMockRuleStore(mockRules, nil)
	cfg := defaultRulerConfig(t)

	r := newTestRuler(t, cfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	a := NewAPI(r, r.store, log.NewNopLogger())

	req := requestFor(t, "GET", "https://localhost:8080/api/prom/api/v1/rules", nil, "user1")
	w := httptest.NewRecorder()
	a.PrometheusRules(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	// Check status code and status response
	responseJSON := util_api.Response{}
	err := json.Unmarshal(body, &responseJSON)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, responseJSON.Status, "success")
	stripEvaluationFields(t, responseJSON)
	actual, err := json.Marshal(responseJSON)
	require.NoError(t, err)

	// Testing the running rules for user1 in the mock store
	expectedResponse, _ := json.Marshal(util_api.Response{
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
					Interval: 10,
				},
			},
		},
	})

	require.JSONEq(t, string(expectedResponse), string(actual))
}

func TestRuler_rules_special_characters(t *testing.T) {
	store := newMockRuleStore(mockSpecialCharRules, nil)
	cfg := defaultRulerConfig(t)

	r := newTestRuler(t, cfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	a := NewAPI(r, r.store, log.NewNopLogger())

	req := requestFor(t, http.MethodGet, "https://localhost:8080/api/prom/api/v1/rules", nil, "user1")
	w := httptest.NewRecorder()
	a.PrometheusRules(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	// Check status code and status response
	responseJSON := util_api.Response{}
	err := json.Unmarshal(body, &responseJSON)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, responseJSON.Status, "success")
	stripEvaluationFields(t, responseJSON)
	actual, err := json.Marshal(responseJSON)
	require.NoError(t, err)

	// Testing the running rules for user1 in the mock store
	expectedResponse, _ := json.Marshal(util_api.Response{
		Status: "success",
		Data: &RuleDiscovery{
			RuleGroups: []*RuleGroup{
				{
					Name: ")(_+?/|group1+/?",
					File: ")(_+?/|namespace1+/?",
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
					Interval: 10,
				},
			},
		},
	})
	require.JSONEq(t, string(expectedResponse), string(actual))
}

func TestRuler_rules_limit(t *testing.T) {
	store := newMockRuleStore(mockRulesLimit, nil)
	cfg := defaultRulerConfig(t)

	r := newTestRuler(t, cfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	a := NewAPI(r, r.store, log.NewNopLogger())

	req := requestFor(t, http.MethodGet, "https://localhost:8080/api/prom/api/v1/rules", nil, "user1")
	w := httptest.NewRecorder()
	a.PrometheusRules(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)
	// Check status code and status response
	responseJSON := util_api.Response{}
	err := json.Unmarshal(body, &responseJSON)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, responseJSON.Status, "success")
	stripEvaluationFields(t, responseJSON)
	actual, err := json.Marshal(responseJSON)
	require.NoError(t, err)

	// Testing the running rules for user1 in the mock store
	expectedResponse, _ := json.Marshal(util_api.Response{
		Status: "success",
		Data: &RuleDiscovery{
			RuleGroups: []*RuleGroup{
				{
					Name:  "group1",
					File:  "namespace1",
					Limit: 5,
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
					Interval: 10,
				},
			},
		},
	})
	require.JSONEq(t, string(expectedResponse), string(actual))
}

func TestRuler_alerts(t *testing.T) {
	store := newMockRuleStore(mockRules, nil)
	cfg := defaultRulerConfig(t)

	r := newTestRuler(t, cfg, store, nil)
	defer r.StopAsync()

	a := NewAPI(r, r.store, log.NewNopLogger())

	req := requestFor(t, http.MethodGet, "https://localhost:8080/api/prom/api/v1/alerts", nil, "user1")
	w := httptest.NewRecorder()
	a.PrometheusAlerts(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	// Check status code and status response
	responseJSON := util_api.Response{}
	err := json.Unmarshal(body, &responseJSON)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, responseJSON.Status, "success")

	// Currently there is not an easy way to mock firing alerts. The empty
	// response case is tested instead.
	expectedResponse, _ := json.Marshal(util_api.Response{
		Status: "success",
		Data: &AlertDiscovery{
			Alerts: []*Alert{},
		},
	})

	require.Equal(t, string(expectedResponse), string(body))
}

func TestRuler_Create(t *testing.T) {
	store := newMockRuleStore(make(map[string]rulespb.RuleGroupList), nil)
	cfg := defaultRulerConfig(t)

	r := newTestRuler(t, cfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	a := NewAPI(r, r.store, log.NewNopLogger())

	tc := []struct {
		name   string
		input  string
		output string
		err    error
		status int
	}{
		{
			name:   "with an empty payload",
			input:  "",
			status: 400,
			err:    errors.New("invalid rules config: rule group name must not be empty"),
		},
		{
			name: "with no rule group name",
			input: `
interval: 15s
rules:
- record: up_rule
  expr: up
`,
			status: 400,
			err:    errors.New("invalid rules config: rule group name must not be empty"),
		},
		{
			name: "with no rules",
			input: `
name: rg_name
interval: 15s
`,
			status: 400,
			err:    errors.New("invalid rules config: rule group 'rg_name' has no rules"),
		},
		{
			name:   "with a valid rules file",
			status: 202,
			input: `
name: test
interval: 15s
rules:
- record: up_rule
  expr: up{}
- alert: up_alert
  expr: sum(up{}) > 1
  for: 30s
  annotations:
    test: test
  labels:
    test: test
`,
			output: "name: test\ninterval: 15s\nrules:\n    - record: up_rule\n      expr: up{}\n    - alert: up_alert\n      expr: sum(up{}) > 1\n      for: 30s\n      labels:\n        test: test\n      annotations:\n        test: test\n",
		},
		{
			name:   "with a valid rule query offset",
			status: 202,
			input: `
name: test
interval: 15s
query_offset: 2m
rules:
- record: up_rule
  expr: up{}
`,
			output: "name: test\ninterval: 15s\nquery_offset: 2m\nrules:\n    - record: up_rule\n      expr: up{}\n",
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			router := mux.NewRouter()
			router.Path("/api/v1/rules/{namespace}").Methods("POST").HandlerFunc(a.CreateRuleGroup)
			router.Path("/api/v1/rules/{namespace}/{groupName}").Methods("GET").HandlerFunc(a.GetRuleGroup)
			// POST
			req := requestFor(t, http.MethodPost, "https://localhost:8080/api/v1/rules/namespace", strings.NewReader(tt.input), "user1")
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)
			require.Equal(t, tt.status, w.Code)

			if tt.err == nil {
				// GET
				req = requestFor(t, http.MethodGet, "https://localhost:8080/api/v1/rules/namespace/test", nil, "user1")
				w = httptest.NewRecorder()

				router.ServeHTTP(w, req)
				require.Equal(t, 200, w.Code)
				require.Equal(t, tt.output, w.Body.String())
			} else {
				require.Equal(t, tt.err.Error()+"\n", w.Body.String())
			}
		})
	}
}

func TestRuler_DeleteNamespace(t *testing.T) {
	store := newMockRuleStore(mockRulesNamespaces, nil)
	cfg := defaultRulerConfig(t)

	r := newTestRuler(t, cfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	a := NewAPI(r, r.store, log.NewNopLogger())

	router := mux.NewRouter()
	router.Path("/api/v1/rules/{namespace}").Methods(http.MethodDelete).HandlerFunc(a.DeleteNamespace)
	router.Path("/api/v1/rules/{namespace}/{groupName}").Methods(http.MethodGet).HandlerFunc(a.GetRuleGroup)

	// Verify namespace1 rules are there.
	req := requestFor(t, http.MethodGet, "https://localhost:8080/api/v1/rules/namespace1/group1", nil, "user1")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "name: group1\ninterval: 10s\nrules:\n    - record: UP_RULE\n      expr: up\n    - alert: UP_ALERT\n      expr: up < 1\n", w.Body.String())

	// Delete namespace1
	req = requestFor(t, http.MethodDelete, "https://localhost:8080/api/v1/rules/namespace1", nil, "user1")
	w = httptest.NewRecorder()

	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, "{\"status\":\"success\"}", w.Body.String())

	// On Partial failures
	req = requestFor(t, http.MethodDelete, "https://localhost:8080/api/v1/rules/namespace2", nil, "user1")
	w = httptest.NewRecorder()

	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Equal(t, "{\"status\":\"error\",\"errorType\":\"server_error\",\"error\":\"unable to delete rg\"}", w.Body.String())
}

func TestRuler_LimitsPerGroup(t *testing.T) {
	store := newMockRuleStore(make(map[string]rulespb.RuleGroupList), nil)
	cfg := defaultRulerConfig(t)

	r := newTestRuler(t, cfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	r.limits = &ruleLimits{maxRuleGroups: 1, maxRulesPerRuleGroup: 1}

	a := NewAPI(r, r.store, log.NewNopLogger())

	tc := []struct {
		name   string
		input  string
		output string
		err    error
		status int
	}{
		{
			name:   "when exceeding the rules per rule group limit",
			status: 400,
			input: `
name: test
interval: 15s
rules:
- record: up_rule
  expr: up{}
- alert: up_alert
  expr: sum(up{}) > 1
  for: 30s
  annotations:
    test: test
  labels:
    test: test
`,
			output: "per-user rules per rule group limit (limit: 1 actual: 2) exceeded\n",
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			router := mux.NewRouter()
			router.Path("/api/v1/rules/{namespace}").Methods("POST").HandlerFunc(a.CreateRuleGroup)
			// POST
			req := requestFor(t, http.MethodPost, "https://localhost:8080/api/v1/rules/namespace", strings.NewReader(tt.input), "user1")
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)
			require.Equal(t, tt.status, w.Code)
			require.Equal(t, tt.output, w.Body.String())
		})
	}
}

func TestRuler_RulerGroupLimits(t *testing.T) {
	store := newMockRuleStore(make(map[string]rulespb.RuleGroupList), nil)
	cfg := defaultRulerConfig(t)

	r := newTestRuler(t, cfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	r.limits = &ruleLimits{maxRuleGroups: 1, maxRulesPerRuleGroup: 1}

	a := NewAPI(r, r.store, log.NewNopLogger())

	tc := []struct {
		name   string
		input  string
		output string
		err    error
		status int
	}{
		{
			name:   "when pushing the first group within bounds of the limit",
			status: 202,
			input: `
name: test_first_group_will_succeed
interval: 15s
rules:
- record: up_rule
  expr: up{}
`,
			output: "{\"status\":\"success\"}",
		},
		{
			name:   "when exceeding the rule group limit after sending the first group",
			status: 400,
			input: `
name: test_second_group_will_fail
interval: 15s
rules:
- record: up_rule
  expr: up{}
`,
			output: "per-user rule groups limit (limit: 1 actual: 2) exceeded\n",
		},
	}

	// define once so the requests build on each other so the number of rules can be tested
	router := mux.NewRouter()
	router.Path("/api/v1/rules/{namespace}").Methods("POST").HandlerFunc(a.CreateRuleGroup)

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			// POST
			req := requestFor(t, http.MethodPost, "https://localhost:8080/api/v1/rules/namespace", strings.NewReader(tt.input), "user1")
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)
			require.Equal(t, tt.status, w.Code)
			require.Equal(t, tt.output, w.Body.String())
		})
	}
}

func TestRuler_ProtoToRuleGroupYamlConvertion(t *testing.T) {
	store := newMockRuleStore(make(map[string]rulespb.RuleGroupList), nil)
	cfg := defaultRulerConfig(t)

	r := newTestRuler(t, cfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	a := NewAPI(r, r.store, log.NewNopLogger())

	tc := []struct {
		name   string
		input  string
		output string
		err    error
		status int
	}{
		{
			name:   "when pushing group that can be safely converted from RuleGroupDesc to RuleGroup yaml",
			status: 202,
			input: `
name: test_first_group_will_succeed
interval: 15s
rules:
- record: up_rule
  expr: |2+
    up{}
`,
			output: "{\"status\":\"success\"}",
		},
		{
			name:   "when pushing group that CANNOT be safely converted from RuleGroupDesc to RuleGroup yaml",
			status: 400,
			input: `
name: test_first_group_will_succeed
interval: 15s
rules:
- record: up_rule
  expr: |2+

    up{}
`,
			output: "unable to decoded rule group\n",
		},
	}

	router := mux.NewRouter()
	router.Path("/api/v1/rules/{namespace}").Methods("POST").HandlerFunc(a.CreateRuleGroup)

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			// POST
			req := requestFor(t, http.MethodPost, "https://localhost:8080/api/v1/rules/namespace", strings.NewReader(tt.input), "user1")
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)
			require.Equal(t, tt.status, w.Code)
			require.Equal(t, tt.output, w.Body.String())
		})
	}
}

func requestFor(t *testing.T, method string, url string, body io.Reader, userID string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(method, url, body)
	ctx := user.InjectOrgID(req.Context(), userID)

	return req.WithContext(ctx)
}
