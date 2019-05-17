package ruler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/api"
	"github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/configs/db"
	"github.com/cortexproject/cortex/pkg/configs/db/dbtest"
	"github.com/weaveworks/common/user"
)

const (
	endpoint = "/api/prom/rules"
)

var (
	app        *API
	database   db.DB
	counter    int
	privateAPI client.Client
)

// setup sets up the environment for the tests.
func setup(t *testing.T) {
	database = dbtest.Setup(t)
	app = NewAPI(database)
	counter = 0
	var err error
	privateAPI, err = client.New(client.Config{
		DBConfig: db.Config{
			URI:  "mock", // trigger client.NewConfigClient to use the mock DB.
			Mock: database,
		},
	})
	require.NoError(t, err)
}

// cleanup cleans up the environment after a test.
func cleanup(t *testing.T) {
	dbtest.Cleanup(t, database)
}

// request makes a request to the configs API.
func request(t *testing.T, handler http.Handler, method, urlStr string, body io.Reader) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r, err := http.NewRequest(method, urlStr, body)
	require.NoError(t, err)
	handler.ServeHTTP(w, r)
	return w
}

// requestAsUser makes a request to the configs API as the given user.
func requestAsUser(t *testing.T, handler http.Handler, userID string, method, urlStr string, body io.Reader) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r, err := http.NewRequest(method, urlStr, body)
	require.NoError(t, err)
	r = r.WithContext(user.InjectOrgID(r.Context(), userID))
	user.InjectOrgIDIntoHTTPRequest(r.Context(), r)
	handler.ServeHTTP(w, r)
	return w
}

// makeString makes a string, guaranteed to be unique within a test.
func makeString(pattern string) string {
	counter++
	return fmt.Sprintf(pattern, counter)
}

// makeUserID makes an arbitrary user ID. Guaranteed to be unique within a test.
func makeUserID() string {
	return makeString("user%d")
}

// makeRulerConfig makes an arbitrary ruler config
func makeRulerConfig(rfv configs.RuleFormatVersion) configs.RulesConfig {
	switch rfv {
	case configs.RuleFormatV1:
		return configs.RulesConfig{
			Files: map[string]string{
				"filename.rules": makeString(`
# Config no. %d.
ALERT ScrapeFailed
  IF          up != 1
  FOR         10m
  LABELS      { severity="warning" }
  ANNOTATIONS {
    summary = "Scrape of {{$labels.job}} (pod: {{$labels.instance}}) failed.",
    description = "Prometheus cannot reach the /metrics page on the {{$labels.instance}} pod.",
    impact = "We have no monitoring data for {{$labels.job}} - {{$labels.instance}}. At worst, it's completely down. At best, we cannot reliably respond to operational issues.",
    dashboardURL = "$${base_url}/admin/prometheus/targets",
  }
			  `),
			},
			FormatVersion: configs.RuleFormatV1,
		}
	case configs.RuleFormatV2:
		return configs.RulesConfig{
			Files: map[string]string{
				"filename.rules": makeString(`
# Config no. %d.
groups:
- name: example
  rules:
  - alert: ScrapeFailed
    expr: 'up != 1'
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Scrape of {{$labels.job}} (pod: {{$labels.instance}}) failed."
      description: "Prometheus cannot reach the /metrics page on the {{$labels.instance}} pod."
      impact: "We have no monitoring data for {{$labels.job}} - {{$labels.instance}}. At worst, it's completely down. At best, we cannot reliably respond to operational issues."
      dashboardURL: "$${base_url}/admin/prometheus/targets"
        `),
			},
			FormatVersion: configs.RuleFormatV2,
		}
	default:
		panic("unknown rule format")
	}
}

// parseVersionedRulesConfig parses a configs.VersionedRulesConfig from JSON.
func parseVersionedRulesConfig(t *testing.T, b []byte) configs.VersionedRulesConfig {
	var x configs.VersionedRulesConfig
	err := json.Unmarshal(b, &x)
	require.NoError(t, err, "Could not unmarshal JSON: %v", string(b))
	return x
}

// post a config
func post(t *testing.T, userID string, oldConfig configs.RulesConfig, newConfig configs.RulesConfig) configs.VersionedRulesConfig {
	updateRequest := configUpdateRequest{
		OldConfig: oldConfig,
		NewConfig: newConfig,
	}
	b, err := json.Marshal(updateRequest)
	require.NoError(t, err)
	reader := bytes.NewReader(b)
	w := requestAsUser(t, app, userID, "POST", endpoint, reader)
	require.Equal(t, http.StatusNoContent, w.Code)
	return get(t, userID)
}

// get a config
func get(t *testing.T, userID string) configs.VersionedRulesConfig {
	w := requestAsUser(t, app, userID, "GET", endpoint, nil)
	return parseVersionedRulesConfig(t, w.Body.Bytes())
}

// configs returns 404 if no config has been created yet.
func Test_GetConfig_NotFound(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()
	w := requestAsUser(t, app, userID, "GET", endpoint, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// configs returns 401 to requests without authentication.
func Test_PostConfig_Anonymous(t *testing.T) {
	setup(t)
	defer cleanup(t)

	w := request(t, app, "POST", endpoint, nil)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// Posting to a configuration sets it so that you can get it again.
func Test_PostConfig_CreatesConfig(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()
	config := makeRulerConfig(configs.RuleFormatV2)
	result := post(t, userID, configs.RulesConfig{}, config)
	assert.Equal(t, config, result.Config)
}

// Posting an invalid config when there's none set returns an error and leaves the config unset.
func Test_PostConfig_InvalidNewConfig(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()
	invalidConfig := configs.RulesConfig{
		Files: map[string]string{
			"some.rules": "invalid config",
		},
		FormatVersion: configs.RuleFormatV2,
	}
	updateRequest := configUpdateRequest{
		OldConfig: configs.RulesConfig{},
		NewConfig: invalidConfig,
	}
	b, err := json.Marshal(updateRequest)
	require.NoError(t, err)
	reader := bytes.NewReader(b)
	{
		w := requestAsUser(t, app, userID, "POST", endpoint, reader)
		require.Equal(t, http.StatusBadRequest, w.Code)
	}
	{
		w := requestAsUser(t, app, userID, "GET", endpoint, nil)
		require.Equal(t, http.StatusNotFound, w.Code)
	}
}

// Posting a v1 rule format configuration sets it so that you can get it again.
func Test_PostConfig_UpdatesConfig_V1RuleFormat(t *testing.T) {
	setup(t)
	app = NewAPI(database)
	defer cleanup(t)

	userID := makeUserID()
	config1 := makeRulerConfig(configs.RuleFormatV1)
	view1 := post(t, userID, configs.RulesConfig{}, config1)
	config2 := makeRulerConfig(configs.RuleFormatV1)
	view2 := post(t, userID, config1, config2)
	assert.True(t, view2.ID > view1.ID, "%v > %v", view2.ID, view1.ID)
	assert.Equal(t, config2, view2.Config)
}

// Posting an invalid v1 rule format config when there's one already set returns an error and leaves the config as is.
func Test_PostConfig_InvalidChangedConfig_V1RuleFormat(t *testing.T) {
	setup(t)
	app = NewAPI(database)
	defer cleanup(t)

	userID := makeUserID()
	config := makeRulerConfig(configs.RuleFormatV1)
	post(t, userID, configs.RulesConfig{}, config)
	invalidConfig := configs.RulesConfig{
		Files: map[string]string{
			"some.rules": "invalid config",
		},
		FormatVersion: configs.RuleFormatV1,
	}
	updateRequest := configUpdateRequest{
		OldConfig: configs.RulesConfig{},
		NewConfig: invalidConfig,
	}
	b, err := json.Marshal(updateRequest)
	require.NoError(t, err)
	reader := bytes.NewReader(b)
	{
		w := requestAsUser(t, app, userID, "POST", endpoint, reader)
		require.Equal(t, http.StatusBadRequest, w.Code)
	}
	result := get(t, userID)
	assert.Equal(t, config, result.Config)
}

// Posting a v2 rule format configuration sets it so that you can get it again.
func Test_PostConfig_UpdatesConfig_V2RuleFormat(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()
	config1 := makeRulerConfig(configs.RuleFormatV2)
	view1 := post(t, userID, configs.RulesConfig{}, config1)
	config2 := makeRulerConfig(configs.RuleFormatV2)
	view2 := post(t, userID, config1, config2)
	assert.True(t, view2.ID > view1.ID, "%v > %v", view2.ID, view1.ID)
	assert.Equal(t, config2, view2.Config)
}

// Posting an invalid v2 rule format config when there's one already set returns an error and leaves the config as is.
func Test_PostConfig_InvalidChangedConfig_V2RuleFormat(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()
	config := makeRulerConfig(configs.RuleFormatV2)
	post(t, userID, configs.RulesConfig{}, config)
	invalidConfig := configs.RulesConfig{
		Files: map[string]string{
			"some.rules": "invalid config",
		},
	}
	updateRequest := configUpdateRequest{
		OldConfig: configs.RulesConfig{},
		NewConfig: invalidConfig,
	}
	b, err := json.Marshal(updateRequest)
	require.NoError(t, err)
	reader := bytes.NewReader(b)
	{
		w := requestAsUser(t, app, userID, "POST", endpoint, reader)
		require.Equal(t, http.StatusBadRequest, w.Code)
	}
	result := get(t, userID)
	assert.Equal(t, config, result.Config)
}

// Posting a config with an invalid rule format version returns an error and leaves the config as is.
func Test_PostConfig_InvalidChangedConfig_InvalidRuleFormat(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()
	config := makeRulerConfig(configs.RuleFormatV2)
	post(t, userID, configs.RulesConfig{}, config)

	// We have to provide the marshaled JSON manually here because json.Marshal() would error
	// on a bad rule format version.
	reader := strings.NewReader(`{"old_config":{"format_version":"1","files":null},"new_config":{"format_version":"<unknown>","files":{"filename.rules":"# Empty."}}}`)
	{
		w := requestAsUser(t, app, userID, "POST", endpoint, reader)
		require.Equal(t, http.StatusBadRequest, w.Code)
	}
	result := get(t, userID)
	assert.Equal(t, config, result.Config)
}

// Different users can have different configurations.
func Test_PostConfig_MultipleUsers(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID1 := makeUserID()
	userID2 := makeUserID()
	config1 := post(t, userID1, configs.RulesConfig{}, makeRulerConfig(configs.RuleFormatV2))
	config2 := post(t, userID2, configs.RulesConfig{}, makeRulerConfig(configs.RuleFormatV2))
	foundConfig1 := get(t, userID1)
	assert.Equal(t, config1, foundConfig1)
	foundConfig2 := get(t, userID2)
	assert.Equal(t, config2, foundConfig2)
	assert.True(t, config2.ID > config1.ID, "%v > %v", config2.ID, config1.ID)
}

// GetAllConfigs returns an empty list of configs if there aren't any.
func Test_GetAllConfigs_Empty(t *testing.T) {
	setup(t)
	defer cleanup(t)

	configs, err := privateAPI.GetRules(0)
	assert.NoError(t, err, "error getting configs")
	assert.Equal(t, 0, len(configs))
}

// GetAllConfigs returns all created configs.
func Test_GetAllConfigs(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()
	config := makeRulerConfig(configs.RuleFormatV2)
	view := post(t, userID, configs.RulesConfig{}, config)

	found, err := privateAPI.GetRules(0)
	assert.NoError(t, err, "error getting configs")
	assert.Equal(t, map[string]configs.VersionedRulesConfig{
		userID: view,
	}, found)
}

// GetAllConfigs returns the *newest* versions of all created configs.
func Test_GetAllConfigs_Newest(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()

	config1 := post(t, userID, configs.RulesConfig{}, makeRulerConfig(configs.RuleFormatV2))
	config2 := post(t, userID, config1.Config, makeRulerConfig(configs.RuleFormatV2))
	lastCreated := post(t, userID, config2.Config, makeRulerConfig(configs.RuleFormatV2))

	found, err := privateAPI.GetRules(0)
	assert.NoError(t, err, "error getting configs")
	assert.Equal(t, map[string]configs.VersionedRulesConfig{
		userID: lastCreated,
	}, found)
}

func Test_GetConfigs_IncludesNewerConfigsAndExcludesOlder(t *testing.T) {
	setup(t)
	defer cleanup(t)

	post(t, makeUserID(), configs.RulesConfig{}, makeRulerConfig(configs.RuleFormatV2))
	config2 := post(t, makeUserID(), configs.RulesConfig{}, makeRulerConfig(configs.RuleFormatV2))
	userID3 := makeUserID()
	config3 := post(t, userID3, configs.RulesConfig{}, makeRulerConfig(configs.RuleFormatV2))

	found, err := privateAPI.GetRules(config2.ID)
	assert.NoError(t, err, "error getting configs")
	assert.Equal(t, map[string]configs.VersionedRulesConfig{
		userID3: config3,
	}, found)
}

// postAlertmanagerConfig posts an alertmanager config to the alertmanager configs API.
func postAlertmanagerConfig(t *testing.T, userID, configFile string) {
	config := configs.Config{
		AlertmanagerConfig: configFile,
		RulesConfig:        configs.RulesConfig{},
	}
	b, err := json.Marshal(config)
	require.NoError(t, err)
	reader := bytes.NewReader(b)
	configsAPI := api.New(database)
	w := requestAsUser(t, configsAPI, userID, "POST", "/api/prom/configs/alertmanager", reader)
	require.Equal(t, http.StatusNoContent, w.Code)
}

// getAlertmanagerConfig posts an alertmanager config to the alertmanager configs API.
func getAlertmanagerConfig(t *testing.T, userID string) string {
	w := requestAsUser(t, api.New(database), userID, "GET", "/api/prom/configs/alertmanager", nil)
	var x configs.View
	b := w.Body.Bytes()
	err := json.Unmarshal(b, &x)
	require.NoError(t, err, "Could not unmarshal JSON: %v", string(b))
	return x.Config.AlertmanagerConfig
}

// If a user has only got alertmanager config set, then we learn nothing about them via GetConfigs.
func Test_AlertmanagerConfig_NotInAllConfigs(t *testing.T) {
	setup(t)
	defer cleanup(t)

	config := makeString(`
            # Config no. %d.
            route:
              receiver: noop

            receivers:
            - name: noop`)
	postAlertmanagerConfig(t, makeUserID(), config)

	found, err := privateAPI.GetRules(0)
	assert.NoError(t, err, "error getting configs")
	assert.Equal(t, map[string]configs.VersionedRulesConfig{}, found)
}

// Setting a ruler config doesn't change alertmanager config.
func Test_AlertmanagerConfig_RulerConfigDoesntChangeIt(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()
	alertmanagerConfig := makeString(`
            # Config no. %d.
            route:
              receiver: noop

            receivers:
            - name: noop`)
	postAlertmanagerConfig(t, userID, alertmanagerConfig)

	rulerConfig := makeRulerConfig(configs.RuleFormatV2)
	post(t, userID, configs.RulesConfig{}, rulerConfig)

	newAlertmanagerConfig := getAlertmanagerConfig(t, userID)
	assert.Equal(t, alertmanagerConfig, newAlertmanagerConfig)
}
