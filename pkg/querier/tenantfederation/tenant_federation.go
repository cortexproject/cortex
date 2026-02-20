package tenantfederation

import (
	"flag"
	"time"
)

type Config struct {
	// Enabled switches on support for multi tenant query federation
	Enabled bool `yaml:"enabled"`
	// MaxConcurrent The number of workers used for processing federated query.
	MaxConcurrent int `yaml:"max_concurrent"`
	// MaxTenant A maximum number of tenants to query at once.
	MaxTenant int `yaml:"max_tenant"`
	// RegexMatcherEnabled If true, the `X-Scope-OrgID` header can accept a regex, matched tenantIDs are automatically involved.
	RegexMatcherEnabled bool `yaml:"regex_matcher_enabled"`
	// UserSyncInterval How frequently to scan users, scanned users are used to calculate matched tenantIDs if the regex matcher is enabled.
	UserSyncInterval time.Duration `yaml:"user_sync_interval"`
	// AllowPartialData If true, enables returning partial results.
	AllowPartialData bool `yaml:"allow_partial_data"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "tenant-federation.enabled", false, "If enabled on all Cortex services, queries can be federated across multiple tenants. The tenant IDs involved need to be specified separated by a `|` character in the `X-Scope-OrgID` header (experimental).")
	f.IntVar(&cfg.MaxConcurrent, "tenant-federation.max-concurrent", defaultMaxConcurrency, "The number of workers used to process each federated query.")
	f.IntVar(&cfg.MaxTenant, "tenant-federation.max-tenant", 0, "A maximum number of tenants to query at once. 0 means no limit.")
	f.BoolVar(&cfg.RegexMatcherEnabled, "tenant-federation.regex-matcher-enabled", false, "[Experimental] If enabled, the `X-Scope-OrgID` header value can accept a regex and the matched tenantIDs are automatically involved. The regex matching rule follows the Prometheus, see the detail: https://prometheus.io/docs/prometheus/latest/querying/basics/#regular-expressions. The user discovery is based on scanning block storage, so new users can get queries after uploading a block (generally 2h).")
	f.DurationVar(&cfg.UserSyncInterval, "tenant-federation.user-sync-interval", time.Minute*5, "[Experimental] If the regex matcher is enabled, it specifies how frequently to scan users. The scanned users are used to calculate matched tenantIDs. The scanning strategy depends on the `-blocks-storage.users-scanner.strategy`.")
	f.BoolVar(&cfg.AllowPartialData, "tenant-federation.allow-partial-data", false, "[Experimental] If enabled, query errors from individual tenants are treated as warnings, allowing partial results to be returned.")
}
