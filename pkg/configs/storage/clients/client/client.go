package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/ruler/rulegroup"

	"github.com/cortexproject/cortex/pkg/configs"
	legacy_configs "github.com/cortexproject/cortex/pkg/configs/legacy_configs"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/pkg/rulefmt"
)

// New creates a new ConfigClient.
func New(cfg Config) (configs.ConfigStore, error) {
	return &configsClient{
		URL:     cfg.ConfigsAPIURL.URL,
		Timeout: cfg.ClientTimeout,

		lastPoll: 0,
	}, nil
}

// configsClient allows retrieving recording and alerting rules from the configs server.
type configsClient struct {
	URL     *url.URL
	Timeout time.Duration

	lastPoll legacy_configs.ID
}

// GetRules implements ConfigClient.
func (c *configsClient) GetRules(ctx context.Context, since legacy_configs.ID) (map[string]legacy_configs.VersionedRulesConfig, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	endpoint := fmt.Sprintf("%s/private/api/prom/configs/rules%s", c.URL.String(), suffix)
	response, err := doRequest(endpoint, c.Timeout, since)
	if err != nil {
		return nil, err
	}
	configs := map[string]legacy_configs.VersionedRulesConfig{}
	for id, view := range response.Configs {
		cfg := view.GetVersionedRulesConfig()
		if cfg != nil {
			configs[id] = *cfg
		}
	}
	return configs, nil
}

// GetAlerts implements ConfigClient.
func (c *configsClient) GetAlerts(ctx context.Context, since legacy_configs.ID) (*ConfigsResponse, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	endpoint := fmt.Sprintf("%s/private/api/prom/configs/alertmanager%s", c.URL.String(), suffix)
	return doRequest(endpoint, c.Timeout, since)
}

func doRequest(endpoint string, timeout time.Duration, since legacy_configs.ID) (*ConfigsResponse, error) {
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
	since legacy_configs.ID

	// Configs maps user ID to their latest configs.View.
	Configs map[string]legacy_configs.View `json:"configs"`
}

// GetLatestConfigID returns the last config ID from a set of configs.
func (c ConfigsResponse) GetLatestConfigID() legacy_configs.ID {
	latest := c.since
	for _, config := range c.Configs {
		if config.ID > latest {
			latest = config.ID
		}
	}
	return latest
}

func (c *configsClient) PollAlerts(ctx context.Context) (map[string]configs.AlertConfig, error) {
	resp, err := c.GetAlerts(ctx, c.lastPoll)
	if err != nil {
		return nil, err
	}

	newConfigs := map[string]configs.AlertConfig{}
	for user, c := range resp.Configs {
		newConfigs[user] = configs.AlertConfig{
			TemplateFiles:      c.Config.TemplateFiles,
			AlertmanagerConfig: c.Config.AlertmanagerConfig,
		}
	}

	c.lastPoll = resp.GetLatestConfigID()

	return newConfigs, nil
}

// PollRules polls the configdb server and returns the updated rule groups
func (c *configsClient) PollRules(ctx context.Context) (map[string][]configs.RuleGroup, error) {
	resp, err := c.GetAlerts(ctx, c.lastPoll)
	if err != nil {
		return nil, err
	}

	newRules := map[string][]configs.RuleGroup{}

	for user, cfg := range resp.Configs {
		userRules := []configs.RuleGroup{}
		rls := cfg.GetVersionedRulesConfig()
		rMap, err := rls.Config.Parse()
		if err != nil {
			return nil, err
		}
		for groupSlug, r := range rMap {
			name, file := decomposeGroupSlug(groupSlug)
			userRules = append(userRules, rulegroup.NewRuleGroup(name, file, user, r))
		}
		newRules[user] = userRules
	}

	if err != nil {
		return nil, err
	}

	c.lastPoll = resp.GetLatestConfigID()

	return newRules, nil
}

// decomposeGroupSlug breaks the group slug from Parse
// into it's group name and file name
func decomposeGroupSlug(slug string) (string, string) {
	components := strings.Split(slug, ";")
	return components[0], components[1]
}

func (c *configsClient) GetAlertConfig(ctx context.Context, userID string) (configs.AlertConfig, error) {
	return configs.AlertConfig{}, errors.New("remote configdb client does not implement GetAlertConfig")
}

func (c *configsClient) SetAlertConfig(ctx context.Context, userID string, config configs.AlertConfig) error {
	return errors.New("remote configdb client does not implement SetAlertConfig")
}

func (c *configsClient) DeleteAlertConfig(ctx context.Context, userID string) error {
	return errors.New("remote configdb client does not implement DeleteAlertConfig")
}

func (c *configsClient) ListRuleGroups(ctx context.Context, options configs.RuleStoreConditions) (map[string]configs.RuleNamespace, error) {
	return nil, errors.New("remote configdb client does not implement ListRule")
}

func (c *configsClient) GetRuleGroup(ctx context.Context, userID, namespace, group string) (rulefmt.RuleGroup, error) {
	return rulefmt.RuleGroup{}, errors.New("remote configdb client does not implement GetRuleGroup")
}

func (c *configsClient) SetRuleGroup(ctx context.Context, userID, namespace string, group rulefmt.RuleGroup) error {
	return errors.New("remote configdb client does not implement SetRuleGroup")
}

func (c *configsClient) DeleteRuleGroup(ctx context.Context, userID, namespace string, group string) error {
	return errors.New("remote configdb client does not implement DeleteRuleGroup")
}
