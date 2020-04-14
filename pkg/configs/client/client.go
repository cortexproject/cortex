package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cortexproject/cortex/pkg/configs/userconfig"
	"github.com/cortexproject/cortex/pkg/util/httpclient"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/instrument"

	"github.com/cortexproject/cortex/pkg/util"
)

var configsRequestDuration = instrument.NewHistogramCollector(promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "cortex",
	Name:      "configs_request_duration_seconds",
	Help:      "Time spent requesting userconfig.",
	Buckets:   prometheus.DefBuckets,
}, []string{"operation", "status_code"}))

// Client is what the ruler and altermanger needs from a config store to process rules.
type Client interface {
	// GetRules returns all Cortex configurations from a configs API server
	// that have been updated after the given userconfig.ID was last updated.
	GetRules(ctx context.Context, since userconfig.ID) (map[string]userconfig.VersionedRulesConfig, error)

	// GetAlerts fetches all the alerts that have changes since since.
	GetAlerts(ctx context.Context, since userconfig.ID) (*ConfigsResponse, error)
}

// New creates a new ConfigClient.
func New(cfg httpclient.Config) (*ConfigDBClient, error) {
	return &ConfigDBClient{
		Config: cfg,
	}, nil
}

// ConfigDBClient allows retrieving recording and alerting rules from the configs server.
type ConfigDBClient struct {
	Config httpclient.Config
}

// GetRules implements Client
func (c ConfigDBClient) GetRules(ctx context.Context, since userconfig.ID) (map[string]userconfig.VersionedRulesConfig, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	endpoint := fmt.Sprintf("%s/private/api/prom/configs/rules%s", c.Config.HTTPEndpoint.URL.String(), suffix)
	var response *ConfigsResponse
	err := instrument.CollectedRequest(ctx, "GetRules", configsRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		response, err = doRequest(endpoint, c.Config, since)
		return err
	})
	if err != nil {
		return nil, err
	}
	configs := map[string]userconfig.VersionedRulesConfig{}
	for id, view := range response.Configs {
		cfg := view.GetVersionedRulesConfig()
		if cfg != nil {
			configs[id] = *cfg
		}
	}
	return configs, nil
}

// GetAlerts implements Client.
func (c ConfigDBClient) GetAlerts(ctx context.Context, since userconfig.ID) (*ConfigsResponse, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	endpoint := fmt.Sprintf("%s/private/api/prom/configs/alertmanager%s", c.Config.HTTPEndpoint.URL.String(), suffix)
	var response *ConfigsResponse
	err := instrument.CollectedRequest(ctx, "GetAlerts", configsRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		response, err = doRequest(endpoint, c.Config, since)
		return err
	})
	return response, err
}

func doRequest(endpoint string, clientConfig httpclient.Config, since userconfig.ID) (*ConfigsResponse, error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: clientConfig.HTTPClientTimeout}
	if tlsConfig := clientConfig.GetTLSConfig(); tlsConfig != nil {
		client.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	}

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

// ConfigsResponse is a response from server for Getuserconfig.
type ConfigsResponse struct {
	// The version since which these configs were changed
	since userconfig.ID

	// Configs maps user ID to their latest userconfig.View.
	Configs map[string]userconfig.View `json:"configs"`
}

// GetLatestConfigID returns the last config ID from a set of userconfig.
func (c ConfigsResponse) GetLatestConfigID() userconfig.ID {
	latest := c.since
	for _, config := range c.Configs {
		if config.ID > latest {
			latest = config.ID
		}
	}
	return latest
}
