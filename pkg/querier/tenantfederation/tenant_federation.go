package tenantfederation

import (
	"flag"
)

type Config struct {
	// Enabled switches on support for multi tenant query federation
	Enabled        bool `yaml:"enabled"`
	MaxConcurrency int  `yaml:"max-concurrency"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "tenant-federation.enabled", false, "If enabled on all Cortex services, queries can be federated across multiple tenants. The tenant IDs involved need to be specified separated by a `|` character in the `X-Scope-OrgID` header (experimental).")
	f.IntVar(&cfg.MaxConcurrency, "tenant-federation.max-concurrency", 16, "Maximum concurrent federated sub queries used when evaluating a federated query")
}
