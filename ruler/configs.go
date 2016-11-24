package ruler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
)

type cortexConfig struct {
	RulesFiles map[string]string `json:"rules_files"`
}

// getOrgConfig gets the organization's cortex config from a configs api server.
func getOrgConfig(configsAPIURL *url.URL, userID string) (*cortexConfig, error) {
	// TODO: Extract configs client logic into go client library (ala users)
	// TODO: Fix configs server so that we not need org ID in the URL to get authenticated org
	url := fmt.Sprintf("%s/api/configs/org/%s/cortex", configsAPIURL.String(), userID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("X-Scope-OrgID", userID)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Invalid response from configs server: %v", res.StatusCode)
	}
	var config cortexConfig
	if err := json.NewDecoder(res.Body).Decode(&config); err != nil {
		return nil, err
	}
	return &config, nil
}

// loadRules loads rules.
//
// Strongly inspired by `loadGroups` in Prometheus.
func loadRules(files map[string]string) ([]rules.Rule, error) {
	result := []rules.Rule{}
	for fn, content := range files {
		stmts, err := promql.ParseStmts(string(content))
		if err != nil {
			return nil, fmt.Errorf("error parsing %s: %s", fn, err)
		}

		for _, stmt := range stmts {
			var rule rules.Rule

			switch r := stmt.(type) {
			case *promql.AlertStmt:
				rule = rules.NewAlertingRule(r.Name, r.Expr, r.Duration, r.Labels, r.Annotations)

			case *promql.RecordStmt:
				rule = rules.NewRecordingRule(r.Name, r.Expr, r.Labels)

			default:
				panic("ruler.loadRules: unknown statement type")
			}
			result = append(result, rule)
		}
	}
	return result, nil
}
