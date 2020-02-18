package client

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/instrument"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

// Config says where we can find the ruler configs.
type Config struct {
	ConfigsAPIURL flagext.URLValue
	ClientTimeout time.Duration // HTTP timeout duration for requests made to the Weave Cloud configs service.
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Var(&cfg.ConfigsAPIURL, prefix+"configs.url", "URL of configs API server.")
	f.DurationVar(&cfg.ClientTimeout, prefix+"configs.client-timeout", 5*time.Second, "Timeout for requests to Weave Cloud configs service.")
}

var configsRequestDuration = instrument.NewHistogramCollector(promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "cortex",
	Name:      "configs_request_duration_seconds",
	Help:      "Time spent requesting configs.",
	Buckets:   prometheus.DefBuckets,
}, []string{"operation", "status_code"}))

// Client is what the ruler and altermanger needs from a config store to process rules.
type Client interface {
	// GetRules returns all Cortex configurations from a configs API server
	// that have been updated after the given configs.ID was last updated.
	GetRules(ctx context.Context, since configs.ID) (map[string]configs.VersionedRulesConfig, error)

	// GetAlerts fetches all the alerts that have changes since since.
	GetAlerts(ctx context.Context, since configs.ID) (*ConfigsResponse, error)
}

// New creates a new ConfigClient.
func New(cfg Config) (*ConfigDBClient, error) {
	return &ConfigDBClient{
		URL:     cfg.ConfigsAPIURL.URL,
		Timeout: cfg.ClientTimeout,
	}, nil
}

// ConfigDBClient allows retrieving recording and alerting rules from the configs server.
type ConfigDBClient struct {
	URL     *url.URL
	Timeout time.Duration
}

// GetRules implements Client
func (c ConfigDBClient) GetRules(ctx context.Context, since configs.ID) (map[string]configs.VersionedRulesConfig, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	endpoint := fmt.Sprintf("%s/private/api/prom/configs/rules%s", c.URL.String(), suffix)
	var response *ConfigsResponse
	err := instrument.CollectedRequest(ctx, "GetRules", configsRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		response, err = doRequest(endpoint, c.Timeout, since)
		return err
	})
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

// GetAlerts implements Client.
func (c ConfigDBClient) GetAlerts(ctx context.Context, since configs.ID) (*ConfigsResponse, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	endpoint := fmt.Sprintf("%s/private/api/prom/configs/alertmanager%s", c.URL.String(), suffix)
	var response *ConfigsResponse
	err := instrument.CollectedRequest(ctx, "GetAlerts", configsRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		response, err = doRequest(endpoint, c.Timeout, since)
		return err
	})
	return response, err
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
