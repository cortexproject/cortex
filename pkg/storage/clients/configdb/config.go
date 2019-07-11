package configdb

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

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.DBConfig.RegisterFlags(f)
	f.Var(&cfg.ConfigsAPIURL, prefix+".configs.url", "DEPRECATED. URL of configs API server.")
	f.DurationVar(&cfg.ClientTimeout, prefix+".client-timeout", 5*time.Second, "DEPRECATED. Timeout for requests to Weave Cloud configs service.")
}
