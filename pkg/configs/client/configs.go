package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/common/log"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/weaveworks/cortex/pkg/configs"
)

// TODO: Extract configs client logic into go client library (ala users)

// ConfigsResponse is a response from server for GetConfigs.
type ConfigsResponse struct {
	// The version since which these configs were changed
	since configs.ID
	// Configs maps user ID to their latest configs.View.
	Configs map[string]configs.View `json:"configs"`
}

func configsFromJSON(body io.Reader) (*ConfigsResponse, error) {
	var configs ConfigsResponse
	if err := json.NewDecoder(body).Decode(&configs); err != nil {
		log.Errorf("configs: couldn't decode JSON body: %v", err)
		return nil, err
	}
	return &configs, nil
}

// GetLatestConfigID returns the last config ID from a set of configs.
func (c ConfigsResponse) GetLatestConfigID() configs.ID {
	latest := c.since
	for _, config := range c.Configs {
		if config.ID > latest {
			latest = config.ID
		}
	}
	return latest
}

// RulesFromConfig gets the rules from the Cortex configuration.
//
// Strongly inspired by `loadGroups` in Prometheus.
func RulesFromConfig(c configs.Config) ([]rules.Rule, error) {
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

// AlertmanagerConfigFromConfig returns the Alertmanager config from the Cortex configuration.
func AlertmanagerConfigFromConfig(c configs.Config) (*config.Config, error) {
	cfg, err := config.Load(c.AlertmanagerConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing Alertmanager config: %s", err)
	}
	return cfg, nil
}

func getConfigs(endpoint string, timeout time.Duration, since configs.ID) (*ConfigsResponse, error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: timeout}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Invalid response from configs server: %v", res.StatusCode)
	}
	resp, err := configsFromJSON(res.Body)
	if err == nil {
		resp.since = since
	}
	return resp, err
}

// AlertManagerConfigsAPI allows retrieving alert configs.
type AlertManagerConfigsAPI struct {
	URL     *url.URL
	Timeout time.Duration
}

// GetConfigs returns all Cortex configurations from a configs API server
// that have been updated after the given configs.ID was last updated.
func (c *AlertManagerConfigsAPI) GetConfigs(since configs.ID) (*ConfigsResponse, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	endpoint := fmt.Sprintf("%s/private/api/prom/configs/alertmanager%s", c.URL.String(), suffix)
	return getConfigs(endpoint, c.Timeout, since)
}

// RulesAPI allows retrieving recording and alerting rules.
type RulesAPI struct {
	URL     *url.URL
	Timeout time.Duration
}

// GetConfigs returns all Cortex configurations from a configs API server
// that have been updated after the given configs.ID was last updated.
func (c *RulesAPI) GetConfigs(since configs.ID) (*ConfigsResponse, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	endpoint := fmt.Sprintf("%s/private/api/prom/configs/rules%s", c.URL.String(), suffix)
	return getConfigs(endpoint, c.Timeout, since)
}
