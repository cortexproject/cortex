package tenantfederation

import (
	"flag"
)

type Config struct {
	// Enabled switches on support for multi tenant query federation
	Enabled bool `yaml:"enabled"`
	// MaxConcurrent The number of workers used for processing federated query.
	MaxConcurrent int `yaml:"max_concurrent"`
	// MaxTenant A maximum number of tenants to query at once.
	MaxTenant int `yaml:"max_tenant"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "tenant-federation.enabled", false, "If enabled on all Cortex services, queries can be federated across multiple tenants. The tenant IDs involved need to be specified separated by a `|` character in the `X-Scope-OrgID` header (experimental).")
	f.IntVar(&cfg.MaxConcurrent, "tenant-federation.max-concurrent", defaultMaxConcurrency, "The number of workers used to process each federated query.")
	f.IntVar(&cfg.MaxTenant, "tenant-federation.max-tenant", 0, "A maximum number of tenants to query at once. 0 means no limit.")
}
