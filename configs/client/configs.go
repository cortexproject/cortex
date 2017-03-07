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
)

// TODO: Extract configs client logic into go client library (ala users)

// A ConfigID is the ID of a single organization's Cortex configuration.
type ConfigID int

// A CortexConfig is a Cortex configuration for a single organization.
type CortexConfig struct {
	// RulesFiles maps from a rules filename to file contents.
	RulesFiles         map[string]string `json:"rules_files"`
	AlertmanagerConfig string            `json:"alertmanager_config"`
}

// CortexConfigView is what's returned from the Weave Cloud configs service
// when we ask for all Cortex configurations.
//
// The configs service is essentially a JSON blob store that gives each
// _version_ of a configuration a unique ID and guarantees that later versions
// have greater IDs.
type CortexConfigView struct {
	ConfigID ConfigID     `json:"id"`
	Config   CortexConfig `json:"config"`
}

// CortexConfigsResponse is a response from server for GetOrgConfigs.
type CortexConfigsResponse struct {
	// Configs maps organization ID to their latest CortexConfigView.
	Configs map[string]CortexConfigView `json:"configs"`
}

func configsFromJSON(body io.Reader) (*CortexConfigsResponse, error) {
	var configs CortexConfigsResponse
	if err := json.NewDecoder(body).Decode(&configs); err != nil {
		log.Errorf("configs: couldn't decode JSON body: %v", err)
		return nil, err
	}
	log.Debugf("configs: got response: %v", configs)
	return &configs, nil
}

// GetLatestConfigID returns the last config ID from a set of configs.
func (c CortexConfigsResponse) GetLatestConfigID() ConfigID {
	latest := ConfigID(0)
	for _, config := range c.Configs {
		if config.ConfigID > latest {
			latest = config.ConfigID
		}
	}
	return latest
}

// GetRules gets the rules from the Cortex configuration.
//
// Strongly inspired by `loadGroups` in Prometheus.
func (c CortexConfig) GetRules() ([]rules.Rule, error) {
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

// GetAlertmanagerConfig returns the Alertmanager config from the Cortex configuration.
func (c CortexConfig) GetAlertmanagerConfig() (*config.Config, error) {
	cfg, err := config.Load(c.AlertmanagerConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing Alertmanager config: %s", err)
	}
	return cfg, nil
}

// API allows retrieving Cortex configs.
type API struct {
	URL     *url.URL
	Timeout time.Duration
}

// GetOrgConfigs returns all Cortex configurations from a configs API server
// that have been updated after the given ConfigID was last updated.
func (c *API) GetOrgConfigs(since ConfigID) (*CortexConfigsResponse, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	url := fmt.Sprintf("%s/private/api/configs/org/cortex%s", c.URL.String(), suffix)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: c.Timeout}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Invalid response from configs server: %v", res.StatusCode)
	}
	return configsFromJSON(res.Body)
}
