package client

import (
	"flag"
	"time"

	"github.com/cortexproject/cortex/pkg/configs/db"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

// Config says where we can find the ruler configs.
type Config struct {
	DBConfig db.Config

	// DEPRECATED
	ConfigsAPIURL flagext.URLValue

	// DEPRECATED. HTTP timeout duration for requests made to the Weave Cloud
	// configs service.
	ClientTimeout time.Duration
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.DBConfig.RegisterFlags(f)
	f.Var(&cfg.ConfigsAPIURL, "ruler.configs.url", "DEPRECATED. URL of configs API server.")
	f.DurationVar(&cfg.ClientTimeout, "ruler.client-timeout", 5*time.Second, "DEPRECATED. Timeout for requests to Weave Cloud configs service.")
	flag.Var(&cfg.ConfigsAPIURL, "alertmanager.configs.url", "URL of configs API server.")
	flag.DurationVar(&cfg.ClientTimeout, "alertmanager.configs.client-timeout", 5*time.Second, "Timeout for requests to Weave Cloud configs service.")
}
