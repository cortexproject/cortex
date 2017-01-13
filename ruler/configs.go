package ruler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
)

// TODO: Extract configs client logic into go client library (ala users)

type configID int

type cortexConfig struct {
	ConfigID   configID
	RulesFiles map[string]string `json:"rules_files"`
}

// cortexConfigsResponse is a response from server for getOrgConfigs
type cortexConfigsResponse struct {
	// Configs maps organization ID to their latest cortexConfig.
	Configs map[string]cortexConfig `json:"configs"`
}

// getLatestConfigID returns the last config ID from a set of configs.
func getLatestConfigID(configs map[string]cortexConfig) configID {
	latest := configID(0)
	for _, config := range configs {
		if config.ConfigID > latest {
			latest = config.ConfigID
		}
	}
	return latest
}

// Get the rules from the cortex configuration.
//
// Strongly inspired by `loadGroups` in Prometheus.
func (c cortexConfig) GetRules() ([]rules.Rule, error) {
	result := []rules.Rule{}
	for fn, content := range c.RulesFiles {
		stmts, err := promql.ParseStmts(content)
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
				return nil, fmt.Errorf("ruler.GetRules: unknown statement type")
			}
			result = append(result, rule)
		}
	}
	return result, nil
}

type configsAPI struct {
	url *url.URL
}

// getOrgConfigs returns all Cortex configurations from a configs api server
// that have been updated since the given time.
func (c *configsAPI) getOrgConfigs(since configID) (map[string]cortexConfig, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	url := fmt.Sprintf("%s/private/api/configs/org/cortex%s", c.url.String(), suffix)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{}
	res, err := client.Do(req)
	defer res.Body.Close()
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Invalid response from configs server: %v", res.StatusCode)
	}
	var configs cortexConfigsResponse
	if err := json.NewDecoder(res.Body).Decode(&configs); err != nil {
		return nil, err
	}
	return configs.Configs, nil
}

// getOrgConfig gets the organization's cortex config from a configs api server.
func (c *configsAPI) getOrgConfig(userID string) (*cortexConfig, error) {
	url := fmt.Sprintf("%s/api/configs/org/%s/cortex", c.url.String(), userID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("X-Scope-OrgID", userID)
	client := &http.Client{}
	res, err := client.Do(req)
	defer res.Body.Close()
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
