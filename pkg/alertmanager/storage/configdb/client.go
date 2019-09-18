package configdb

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/cortexproject/cortex/pkg/alertmanager/alerts"
	"github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/go-kit/kit/log/level"
)

// Config says where we can find the ruler configs.
type Config struct {
	ConfigsAPIURL flagext.URLValue
	ClientTimeout time.Duration
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	flag.Var(&cfg.ConfigsAPIURL, "alertmanager.configs.url", "URL of configs API server.")
	flag.DurationVar(&cfg.ClientTimeout, "alertmanager.configs.client-timeout", 5*time.Second, "Timeout for requests to the cortex configdb.")
}

// New creates a new ConfigClient.
func New(cfg Config) (*Client, error) {
	return &Client{
		URL:     cfg.ConfigsAPIURL.URL,
		Timeout: cfg.ClientTimeout,
	}, nil
}

// Client allows retrieving recording and alerting rules from the configs server.
type Client struct {
	URL     *url.URL
	Timeout time.Duration
}

func doRequest(endpoint string, timeout time.Duration) (*client.ConfigsResponse, error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	cli := &http.Client{Timeout: timeout}
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Invalid response from configs server: %v", resp.StatusCode)
	}

	var config client.ConfigsResponse
	if err := json.NewDecoder(resp.Body).Decode(&config); err != nil {
		level.Error(util.Logger).Log("msg", "configs: couldn't decode JSON body", "err", err)
		return nil, err
	}

	return &config, nil
}

// ListAlertConfigs returns a list of each users alertmanager config.
func (c *Client) ListAlertConfigs(ctx context.Context) ([]alerts.AlertConfigDesc, error) {
	var cfgs []alerts.AlertConfigDesc
	endpoint := c.URL.String() + "/private/api/prom/configs/alertmanager"
	resp, err := doRequest(endpoint, c.Timeout)
	if err != nil {
		return nil, err
	}

	for user, cfg := range resp.Configs {
		var templates []*alerts.TemplateDesc
		for fn, template := range cfg.Config.TemplateFiles {
			templates = append(templates, &alerts.TemplateDesc{
				Filename: fn,
				Body:     template,
			})
		}
		cfgs = append(cfgs, alerts.AlertConfigDesc{
			User:      user,
			RawConfig: cfg.Config.AlertmanagerConfig,
			Templates: templates,
		})
	}

	return cfgs, nil
}
