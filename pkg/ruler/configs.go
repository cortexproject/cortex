package ruler

import (
	"flag"
	"fmt"
	"net/url"
	"time"

	"github.com/weaveworks/cortex/pkg/configs"
	configs_client "github.com/weaveworks/cortex/pkg/configs/client"
	"github.com/weaveworks/cortex/pkg/configs/db"
	"github.com/weaveworks/cortex/pkg/util"
)

// ConfigStoreConfig says where we can find the ruler configs.
type ConfigStoreConfig struct {
	DBConfig db.Config

	// DEPRECATED
	ConfigsAPIURL util.URLValue

	// DEPRECATED. HTTP timeout duration for requests made to the Weave Cloud
	// configs service.
	ClientTimeout time.Duration
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *ConfigStoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.DBConfig.RegisterFlags(f)
	f.Var(&cfg.ConfigsAPIURL, "ruler.configs.url", "DEPRECATED. URL of configs API server.")
	f.DurationVar(&cfg.ClientTimeout, "ruler.client-timeout", 5*time.Second, "DEPRECATED. Timeout for requests to Weave Cloud configs service.")
}

// RulesAPI is what the ruler needs from a config store to process rules.
type RulesAPI interface {
	// GetConfigs returns all Cortex configurations from a configs API server
	// that have been updated after the given configs.ID was last updated.
	GetConfigs(since configs.ID) (map[string]configs.VersionedRulesConfig, error)
}

// NewRulesAPI creates a new RulesAPI.
func NewRulesAPI(cfg ConfigStoreConfig) (RulesAPI, error) {
	// All of this falderal is to allow for a smooth transition away from
	// using the configs server and toward directly connecting to the database.
	// See https://github.com/weaveworks/cortex/issues/619
	if cfg.DBConfig.URI != "" {
		return configsClient{
			URL:     cfg.ConfigsAPIURL.URL,
			Timeout: cfg.ClientTimeout,
		}, nil
	}
	db, err := db.NewRulesDB(cfg.DBConfig)
	if err != nil {
		return nil, err
	}
	return dbStore{db: db}, nil
}

// configsClient allows retrieving recording and alerting rules from the configs server.
type configsClient struct {
	URL     *url.URL
	Timeout time.Duration
}

// GetConfigs implements RulesAPI.
func (c configsClient) GetConfigs(since configs.ID) (map[string]configs.VersionedRulesConfig, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	endpoint := fmt.Sprintf("%s/private/api/prom/configs/rules%s", c.URL.String(), suffix)
	response, err := configs_client.GetConfigs(endpoint, c.Timeout, since)
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

type dbStore struct {
	db db.RulesDB
}

// GetConfigs implements RulesAPI.
func (d dbStore) GetConfigs(since configs.ID) (map[string]configs.VersionedRulesConfig, error) {
	if since == 0 {
		return d.db.GetAllRulesConfigs()
	}
	return d.db.GetRulesConfigs(since)
}

// getLatestConfigID gets the latest configs ID.
// max [latest, max (map getID cfgs)]
func getLatestConfigID(cfgs map[string]configs.VersionedRulesConfig, latest configs.ID) configs.ID {
	ret := latest
	for _, config := range cfgs {
		if config.ID > ret {
			ret = config.ID
		}
	}
	return ret
}
