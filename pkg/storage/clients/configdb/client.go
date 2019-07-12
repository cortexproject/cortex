package configdb

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/ruler/group"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/go-kit/kit/log/level"
)

// Config says where we can find the ruler configs.
type Config struct {
	ConfigsAPIURL flagext.URLValue
	ClientTimeout time.Duration
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Var(&cfg.ConfigsAPIURL, prefix+".configs.url", "DEPRECATED. URL of configs API server.")
	f.DurationVar(&cfg.ClientTimeout, prefix+".client-timeout", 5*time.Second, "DEPRECATED. Timeout for requests to Weave Cloud configs service.")
}

// ConfigsClient allows retrieving recording and alerting rules from the configs server.
type ConfigsClient struct {
	URL     *url.URL
	Timeout time.Duration

	lastPoll configs.ID
}

// New creates a new ConfigClient.
func New(cfg Config) (*ConfigsClient, error) {
	return &ConfigsClient{
		URL:     cfg.ConfigsAPIURL.URL,
		Timeout: cfg.ClientTimeout,

		lastPoll: 0,
	}, nil
}

// GetRules implements ConfigClient.
func (c *ConfigsClient) GetRules(ctx context.Context, since configs.ID) (map[string]configs.VersionedRulesConfig, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	endpoint := fmt.Sprintf("%s/private/api/prom/configs/rules%s", c.URL.String(), suffix)
	response, err := doRequest(endpoint, c.Timeout, since)
	if err != nil {
		return nil, err
	}
	configs := map[string]configs.VersionedRulesConfig{}
	for id, view := range response.Configs {
		cfg := view.GetVersionedRulesConfig()
		if cfg != nil {
			configs[id] = *cfg
		}
	}
	return configs, nil
}

// GetAlerts implements ConfigClient.
func (c *ConfigsClient) GetAlerts(ctx context.Context, since configs.ID) (*ConfigsResponse, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	endpoint := fmt.Sprintf("%s/private/api/prom/configs/alertmanager%s", c.URL.String(), suffix)
	return doRequest(endpoint, c.Timeout, since)
}

func doRequest(endpoint string, timeout time.Duration, since configs.ID) (*ConfigsResponse, error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Invalid response from configs server: %v", resp.StatusCode)
	}

	var config ConfigsResponse
	if err := json.NewDecoder(resp.Body).Decode(&config); err != nil {
		level.Error(util.Logger).Log("msg", "configs: couldn't decode JSON body", "err", err)
		return nil, err
	}

	config.since = since
	return &config, nil
}

// ConfigsResponse is a response from server for GetConfigs.
type ConfigsResponse struct {
	// The version since which these configs were changed
	since configs.ID

	// Configs maps user ID to their latest configs.View.
	Configs map[string]configs.View `json:"configs"`
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

func (c *ConfigsClient) PollAlertConfigs(ctx context.Context) (map[string]alertmanager.AlertConfig, error) {
	resp, err := c.GetAlerts(ctx, c.lastPoll)
	if err != nil {
		return nil, err
	}

	newConfigs := map[string]alertmanager.AlertConfig{}
	for user, c := range resp.Configs {
		newConfigs[user] = alertmanager.AlertConfig{
			TemplateFiles:      c.Config.TemplateFiles,
			AlertmanagerConfig: c.Config.AlertmanagerConfig,
		}
	}

	c.lastPoll = resp.GetLatestConfigID()

	return newConfigs, nil
}

// PollRules polls the configdb server and returns the updated rule groups
func (c *ConfigsClient) PollRules(ctx context.Context) (map[string][]ruler.RuleGroup, error) {
	resp, err := c.GetAlerts(ctx, c.lastPoll)
	if err != nil {
		return nil, err
	}

	newRules := map[string][]ruler.RuleGroup{}

	for user, cfg := range resp.Configs {
		userRules := []ruler.RuleGroup{}
		rls := cfg.GetVersionedRulesConfig()
		rMap, err := rls.Config.Parse()
		if err != nil {
			return nil, err
		}
		for groupSlug, r := range rMap {
			name, file := decomposeGroupSlug(groupSlug)
			userRules = append(userRules, group.NewRuleGroup(name, file, user, r))
		}
		newRules[user] = userRules
	}

	if err != nil {
		return nil, err
	}

	c.lastPoll = resp.GetLatestConfigID()

	return newRules, nil
}

// AlertStore returns an AlertStore from the client
func (c *ConfigsClient) AlertStore() alertmanager.AlertStore {
	return nil
}

// RuleStore returns an RuleStore from the client
func (c *ConfigsClient) RuleStore() ruler.RuleStore {
	return nil
}

// decomposeGroupSlug breaks the group slug from Parse
// into it's group name and file name
func decomposeGroupSlug(slug string) (string, string) {
	components := strings.Split(slug, ";")
	return components[0], components[1]
}
