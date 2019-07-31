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

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/storage/rules"
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

// Stop stops rthe config client
func (c *ConfigsClient) Stop() {}

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

// PollRules polls the configdb server and returns the updated rule groups
func (c *ConfigsClient) PollRules(ctx context.Context) (map[string][]rules.RuleGroup, error) {
	resp, err := c.GetRules(ctx, c.lastPoll)
	if err != nil {
		return nil, err
	}

	newRules := map[string][]rules.RuleGroup{}

	var highestID configs.ID
	for user, cfg := range resp {
		if cfg.ID > highestID {
			highestID = cfg.ID
		}
		userRules := []rules.RuleGroup{}
		if cfg.IsDeleted() {
			newRules[user] = []rules.RuleGroup{}
		}
		rMap, err := cfg.Config.Parse()
		if err != nil {
			return nil, err
		}
		for groupSlug, r := range rMap {
			name, file := decomposeGroupSlug(groupSlug)
			userRules = append(userRules, rules.FormattedToRuleGroup(user, file, name, r))
		}
		newRules[user] = userRules
	}

	if err != nil {
		return nil, err
	}

	c.lastPoll = highestID

	return newRules, nil
}

// decomposeGroupSlug breaks the group slug from Parse
// into it's group name and file name
func decomposeGroupSlug(slug string) (string, string) {
	components := strings.Split(slug, ";")
	return components[0], components[1]
}
