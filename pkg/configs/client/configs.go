package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	gklog "github.com/go-kit/kit/log"
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

// Prometheus alerting rules log warnings when they fail to expand notification
// templates. This requires a go-kit logger to be injected (see below). The
// warnLogWriter is an io.Writer that gets wrapped into a go-kit logger and
// outputs any log messages at warning level with the usual Cortex (Prometheus)
// logger we use. The output is a bit messy (because structured key/vals are nested
// within a single key of the outer logger), but doing a full conversion of fields
// between go-kit and our logger seems like overkill just for this one case.
//
// TODO: Eventually, template expansion errors should be displayed to the user
//       somehow, though part of that can be caught during configuration-saving time.
type warnLogWriter struct{}

func (l warnLogWriter) Write(p []byte) (int, error) {
	var buf bytes.Buffer
	n, err := buf.Write(p)
	if err != nil {
		return n, err
	}
	log.Warn(buf.String())
	return n, nil
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
				rule = rules.NewAlertingRule(r.Name, r.Expr, r.Duration, r.Labels, r.Annotations, gklog.NewLogfmtLogger(warnLogWriter{}))

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
