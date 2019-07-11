package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/db"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
)

// Client is what the ruler and altermanger needs from a config store to process rules.
type Client interface {
	// GetRules returns all Cortex configurations from a configs API server
	// that have been updated after the given configs.ID was last updated.
	GetRules(ctx context.Context, since configs.ID) (map[string]configs.VersionedRulesConfig, error)

	// GetAlerts fetches all the alerts that have changes since since.
	GetAlerts(ctx context.Context, since configs.ID) (*ConfigsResponse, error)
}

// New creates a new ConfigClient.
func New(cfg Config) (Client, error) {
	// All of this falderal is to allow for a smooth transition away from
	// using the configs server and toward directly connecting to the database.
	// See https://github.com/cortexproject/cortex/issues/619
	if cfg.ConfigsAPIURL.URL != nil {
		return instrumented{
			next: configsClient{
				URL:     cfg.ConfigsAPIURL.URL,
				Timeout: cfg.ClientTimeout,
			},
		}, nil
	}

	db, err := db.New(cfg.DBConfig)
	if err != nil {
		return nil, err
	}
	return instrumented{
		next: dbStore{
			db: db,
		},
	}, nil
}

// configsClient allows retrieving recording and alerting rules from the configs server.
type configsClient struct {
	URL     *url.URL
	Timeout time.Duration
}

// GetRules implements ConfigClient.
func (c configsClient) GetRules(ctx context.Context, since configs.ID) (map[string]configs.VersionedRulesConfig, error) {
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
func (c configsClient) GetAlerts(ctx context.Context, since configs.ID) (*ConfigsResponse, error) {
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

type dbStore struct {
	db db.DB
}

// GetRules implements ConfigClient.
func (d dbStore) GetRules(ctx context.Context, since configs.ID) (map[string]configs.VersionedRulesConfig, error) {
	if since == 0 {
		return d.db.GetAllRulesConfigs(ctx)
	}
	return d.db.GetRulesConfigs(ctx, since)
}

// GetAlerts implements ConfigClient.
func (d dbStore) GetAlerts(ctx context.Context, since configs.ID) (*ConfigsResponse, error) {
	var resp map[string]configs.View
	var err error
	if since == 0 {
		resp, err = d.db.GetAllConfigs(ctx)

	}
	resp, err = d.db.GetConfigs(ctx, since)
	if err != nil {
		return nil, err
	}

	return &ConfigsResponse{
		since:   since,
		Configs: resp,
	}, nil
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
