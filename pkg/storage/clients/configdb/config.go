package configdb

import (
	"flag"
	"time"

	"github.com/cortexproject/cortex/pkg/util/flagext"
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
