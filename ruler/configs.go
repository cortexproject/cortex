package ruler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/prometheus/common/log"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
)

// TODO: Extract configs client logic into go client library (ala users)

type configID int

type cortexConfig struct {
	// RulesFiles maps from a rules filename to file contents.
	RulesFiles map[string]string `json:"rules_files"`
}

// cortexConfigView is what's returned from the Weave Cloud configs service
// when we ask for all Cortex configurations.
//
// The configs service is essentially a JSON blob store that gives each
// _version_ of a configuration a unique ID and guarantees that later versions
// have greater IDs.
type cortexConfigView struct {
	ConfigID configID     `json:"id"`
	Config   cortexConfig `json:"config"`
}

// cortexConfigsResponse is a response from server for getOrgConfigs
type cortexConfigsResponse struct {
	// Configs maps organization ID to their latest cortexConfigView.
	Configs map[string]cortexConfigView `json:"configs"`
}

func configsFromJSON(body io.Reader) (*cortexConfigsResponse, error) {
	var configs cortexConfigsResponse
	if err := json.NewDecoder(body).Decode(&configs); err != nil {
		log.Errorf("configs: couldn't decode JSON body: %v", err)
		return nil, err
	}
	log.Debugf("configs: got response: %v", configs)
	return &configs, nil
}

// getLatestConfigID returns the last config ID from a set of configs.
func (c cortexConfigsResponse) getLatestConfigID() configID {
	latest := configID(0)
	for _, config := range c.Configs {
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
	url     *url.URL
	timeout time.Duration
}

// getOrgConfigs returns all Cortex configurations from a configs api server
// that have been updated after the given configID was last updated.
func (c *configsAPI) getOrgConfigs(since configID) (*cortexConfigsResponse, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	url := fmt.Sprintf("%s/private/api/configs/org/cortex%s", c.url.String(), suffix)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: c.timeout}
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
