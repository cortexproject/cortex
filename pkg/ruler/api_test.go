package ruler

import (
	"context"
	"encoding/json"
	"errors"
	io "io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/gorilla/mux"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore/objectclient"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestRuler_rules(t *testing.T) {
	cfg, cleanup := defaultRulerConfig(newMockRuleStore(mockRules))
	defer cleanup()

	r, rcleanup := newTestRuler(t, cfg)
	defer rcleanup()
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	a := NewAPI(r, r.store, log.NewNopLogger())

	req := requestFor(t, "GET", "https://localhost:8080/api/prom/api/v1/rules", nil, "user1")
	w := httptest.NewRecorder()
	a.PrometheusRules(w, req)

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

func TestRuler_rules_special_characters(t *testing.T) {
	cfg, cleanup := defaultRulerConfig(newMockRuleStore(mockSpecialCharRules))
	defer cleanup()

	r, rcleanup := newTestRuler(t, cfg)
	defer rcleanup()
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	a := NewAPI(r, r.store, log.NewNopLogger())

	req := requestFor(t, http.MethodGet, "https://localhost:8080/api/prom/api/v1/rules", nil, "user1")
	w := httptest.NewRecorder()
	a.PrometheusRules(w, req)

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

	a := NewAPI(r, r.store, log.NewNopLogger())

	req := requestFor(t, http.MethodGet, "https://localhost:8080/api/prom/api/v1/alerts", nil, "user1")
	w := httptest.NewRecorder()
	a.PrometheusAlerts(w, req)

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

func TestRuler_Create(t *testing.T) {
	cfg, cleanup := defaultRulerConfig(newMockRuleStore(make(map[string]rulestore.RuleGroupList)))
	defer cleanup()

	r, rcleanup := newTestRuler(t, cfg)
	defer rcleanup()
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
			name:   "with a a valid rules file",
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
	cfg, cleanup := defaultRulerConfig(newMockRuleStore(mockRulesNamespaces))
	defer cleanup()

	r, rcleanup := newTestRuler(t, cfg)
	defer rcleanup()
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
	require.Equal(t, "name: group1\ninterval: 1m\nrules:\n    - record: UP_RULE\n      expr: up\n    - alert: UP_ALERT\n      expr: up < 1\n", w.Body.String())

	// Delete namespace1
	req = requestFor(t, http.MethodDelete, "https://localhost:8080/api/v1/rules/namespace1", nil, "user1")
	w = httptest.NewRecorder()

	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, "{\"status\":\"success\",\"data\":null,\"errorType\":\"\",\"error\":\"\"}", w.Body.String())

	// On Partial failures
	req = requestFor(t, http.MethodDelete, "https://localhost:8080/api/v1/rules/namespace2", nil, "user1")
	w = httptest.NewRecorder()

	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Equal(t, "{\"status\":\"error\",\"data\":null,\"errorType\":\"server_error\",\"error\":\"unable to delete rg\"}", w.Body.String())
}

func TestRuler_Limits(t *testing.T) {
	cfg, cleanup := defaultRulerConfig(newMockRuleStore(make(map[string]rulestore.RuleGroupList)))
	defer cleanup()

	r, rcleanup := newTestRuler(t, cfg)
	defer rcleanup()
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
		{
			name:   "when exceeding the rule group limit",
			status: 400,
			input: `
name: test
interval: 15s
rules:
- record: up_rule
  expr: up{}
`,
			output: "per-user rules per rule group limit (limit: 1 actual: 1) exceeded\n",
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

func requestFor(t *testing.T, method string, url string, body io.Reader, userID string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(method, url, body)
	ctx := user.InjectOrgID(req.Context(), userID)

	return req.WithContext(ctx)
}

func TestDeleteTenantRuleGroups(t *testing.T) {
	ruleGroups := []ruleGroupKey{
		{user: "userA", namespace: "namespace", group: "group"},
		{user: "userB", namespace: "namespace1", group: "group"},
		{user: "userB", namespace: "namespace2", group: "group"},
	}

	obj, rs := setupRuleGroupsStore(t, ruleGroups)
	require.Equal(t, 3, obj.GetObjectCount())

	api := NewAPI(nil, rs, log.NewNopLogger())

	{
		callDeleteTenantAPI(t, api, "user-with-no-rule-groups")
		require.Equal(t, 3, obj.GetObjectCount())

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", false)
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", false)
	}

	{
		callDeleteTenantAPI(t, api, "userA")
		require.Equal(t, 2, obj.GetObjectCount())

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", true)                    // Just deleted.
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", false)
	}

	{
		callDeleteTenantAPI(t, api, "userB")
		require.Equal(t, 0, obj.GetObjectCount())

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", true)                    // Deleted previously
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", true)                    // Just deleted
	}
}

func callDeleteTenantAPI(t *testing.T, api *API, userID string) {
	ctx := user.InjectOrgID(context.Background(), userID)

	req := &http.Request{}
	resp := httptest.NewRecorder()
	api.DeleteTenantConfiguration(resp, req.WithContext(ctx))

	require.Equal(t, http.StatusOK, resp.Code)
}

func verifyExpectedDeletedRuleGroupsForUser(t *testing.T, api *API, userID string, expectedDeleted bool) {
	ctx := user.InjectOrgID(context.Background(), userID)

	req := &http.Request{}
	var err error
	req.URL, err = url.ParseRequestURI("/api/v1/rules")
	require.NoError(t, err)
	resp := httptest.NewRecorder()
	api.ListRules(resp, req.WithContext(ctx))

	if expectedDeleted {
		// If no rules are found, 404 is returned.
		require.Equal(t, http.StatusNotFound, resp.Code)
	} else {
		require.Equal(t, http.StatusOK, resp.Code)
	}
}

func setupRuleGroupsStore(t *testing.T, ruleGroups []ruleGroupKey) (*chunk.MockStorage, rulestore.RuleStore) {
	obj := chunk.NewMockStorage()
	rs := objectclient.NewRuleStore(obj, 5, log.NewNopLogger())

	// "upload" rule groups
	for _, key := range ruleGroups {
		desc := rulespb.ToProto(key.user, key.namespace, rulefmt.RuleGroup{Name: key.group})
		require.NoError(t, rs.SetRuleGroup(context.Background(), key.user, key.namespace, desc))
	}

	return obj, rs
}

type ruleGroupKey struct {
	user, namespace, group string
}
