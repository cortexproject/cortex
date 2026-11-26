package alertmanager

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestAlertmanagerAPIReportsAlertStatus verifies that the Alertmanager API
// reports correct alert states (active vs suppressed) rather than falling back
// to the default "unprocessed" state.
func TestAlertmanagerAPIReportsAlertStatus(t *testing.T) {
	const user = "test"

	am, err := New(&Config{
		UserID:          user,
		Logger:          log.NewNopLogger(),
		Limits:          &mockAlertManagerLimits{},
		TenantDataDir:   t.TempDir(),
		ExternalURL:     &url.URL{Path: "/am"},
		ShardingEnabled: false,
		GCInterval:      30 * time.Minute,
	}, prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	defer am.StopAndWait()

	cfgRaw := `receivers:
- name: 'prod'

route:
  group_by: ['alertname']
  group_wait: 10ms
  group_interval: 10ms
  receiver: 'prod'`
	cfg, err := config.Load(cfgRaw)
	require.NoError(t, err)
	require.NoError(t, am.ApplyConfig(user, cfg, cfgRaw))

	now := time.Now().UTC()
	postJSON := func(path string, payload any) *httptest.ResponseRecorder {
		b, err := json.Marshal(payload)
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "http://alertmanager"+path, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		am.mux.ServeHTTP(rec, req)
		return rec
	}

	// 1. Post alerts via HTTP API
	alertsPayload := []map[string]any{
		{
			"labels":   map[string]string{"alertname": "ActiveAlert", "severity": "critical"},
			"startsAt": now.Add(-time.Minute).Format(time.RFC3339),
			"endsAt":   now.Add(time.Hour).Format(time.RFC3339),
		},
		{
			"labels":   map[string]string{"alertname": "SilencedAlert", "severity": "warning"},
			"startsAt": now.Add(-time.Minute).Format(time.RFC3339),
			"endsAt":   now.Add(time.Hour).Format(time.RFC3339),
		},
	}
	recAlerts := postJSON("/am/api/v2/alerts", alertsPayload)
	require.Equal(t, http.StatusOK, recAlerts.Code, recAlerts.Body.String())

	// 2. Post a silence for SilencedAlert
	silencePayload := map[string]any{
		"matchers": []map[string]any{
			{"name": "alertname", "value": "SilencedAlert", "isRegex": false},
		},
		"startsAt":  now.Add(-time.Minute).Format(time.RFC3339),
		"endsAt":    now.Add(time.Hour).Format(time.RFC3339),
		"createdBy": "test-author",
		"comment":   "test silence",
	}
	recSilence := postJSON("/am/api/v2/silences", silencePayload)
	require.Equal(t, http.StatusOK, recSilence.Code, recSilence.Body.String())

	var silResp struct {
		SilenceID string `json:"silenceId"`
	}
	require.NoError(t, json.NewDecoder(recSilence.Body).Decode(&silResp))
	require.NotEmpty(t, silResp.SilenceID)

	type apiAlertResponse struct {
		Labels map[string]string `json:"labels"`
		Status struct {
			State      string   `json:"state"`
			SilencedBy []string `json:"silencedBy"`
		} `json:"status"`
	}

	getAlerts := func(queryPath string) []apiAlertResponse {
		req := httptest.NewRequest(http.MethodGet, "http://alertmanager"+queryPath, nil)
		rec := httptest.NewRecorder()
		am.mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return nil
		}
		var alerts []apiAlertResponse
		_ = json.NewDecoder(rec.Body).Decode(&alerts)
		return alerts
	}

	// 3. Verify /api/v2/alerts reports active and suppressed (never unprocessed)
	require.Eventually(t, func() bool {
		alerts := getAlerts("/am/api/v2/alerts")
		if len(alerts) != 2 {
			return false
		}
		activeOK := false
		silencedOK := false
		for _, a := range alerts {
			switch a.Labels["alertname"] {
			case "ActiveAlert":
				if a.Status.State == "active" && len(a.Status.SilencedBy) == 0 {
					activeOK = true
				}
			case "SilencedAlert":
				if a.Status.State == "suppressed" && len(a.Status.SilencedBy) == 1 && a.Status.SilencedBy[0] == silResp.SilenceID {
					silencedOK = true
				}
			}
		}
		return activeOK && silencedOK
	}, 3*time.Second, 20*time.Millisecond, "alerts should settle into active and suppressed states")

	// 4. Verify /api/v2/alerts/groups also does not report unprocessed
	type apiAlertGroupResponse struct {
		Alerts []apiAlertResponse `json:"alerts"`
	}
	require.Eventually(t, func() bool {
		reqGroup := httptest.NewRequest(http.MethodGet, "http://alertmanager/am/api/v2/alerts/groups", nil)
		recGroup := httptest.NewRecorder()
		am.mux.ServeHTTP(recGroup, reqGroup)
		if recGroup.Code != http.StatusOK {
			return false
		}
		var groups []apiAlertGroupResponse
		if err := json.NewDecoder(recGroup.Body).Decode(&groups); err != nil || len(groups) == 0 {
			return false
		}
		for _, g := range groups {
			for _, a := range g.Alerts {
				if a.Status.State == "unprocessed" {
					return false
				}
			}
		}
		return true
	}, 3*time.Second, 20*time.Millisecond, "group alert state must not be unprocessed")

	// 5. Verify filtering by silenced=false excludes the suppressed alert
	require.Eventually(t, func() bool {
		unfilteredAlerts := getAlerts("/am/api/v2/alerts?silenced=false")
		return len(unfilteredAlerts) == 1 && unfilteredAlerts[0].Labels["alertname"] == "ActiveAlert"
	}, 3*time.Second, 20*time.Millisecond, "silenced=false should exclude suppressed alerts")
}
