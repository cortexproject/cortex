package ingester

import (
	"flag"

	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

var (
	// We don't include values in the message to avoid leaking Cortex cluster configuration to users.
	errMaxSamplesPushRateLimitReached = errors.New("cannot push more samples: ingester's samples push rate limit reached")
	errMaxUsersLimitReached           = errors.New("cannot create TSDB: ingesters's max tenants limit reached")
	errMaxSeriesLimitReached          = errors.New("cannot add series: ingesters's max series limit reached")
	errTooManyInflightPushRequests    = errors.New("cannot push: too many inflight push requests in ingester")
	errTooManyInflightQueryRequests   = errors.New("cannot push: too many inflight query requests in ingester")
)

// InstanceLimits describes limits used by ingester. Reaching any of these will result in error response to the call.
type InstanceLimits struct {
	configs.InstanceLimits `yaml:",inline"`

	MaxIngestionRate         float64 `yaml:"max_ingestion_rate"`
	MaxInMemoryTenants       int64   `yaml:"max_tenants"`
	MaxInMemorySeries        int64   `yaml:"max_series"`
	MaxInflightPushRequests  int64   `yaml:"max_inflight_push_requests"`
	MaxInflightQueryRequests int64   `yaml:"max_inflight_query_requests"`
}

// Sets default limit values for unmarshalling.
var defaultInstanceLimits *InstanceLimits

func (cfg *InstanceLimits) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.Float64Var(&cfg.MaxIngestionRate, prefix+"instance-limits.max-ingestion-rate", 0, "Max ingestion rate (samples/sec) that ingester will accept. This limit is per-ingester, not per-tenant. Additional push requests will be rejected. Current ingestion rate is computed as exponentially weighted moving average, updated every second. This limit only works when using blocks engine. 0 = unlimited.")
	f.Int64Var(&cfg.MaxInMemoryTenants, prefix+"instance-limits.max-tenants", 0, "Max users that this ingester can hold. Requests from additional users will be rejected. This limit only works when using blocks engine. 0 = unlimited.")
	f.Int64Var(&cfg.MaxInMemorySeries, prefix+"instance-limits.max-series", 0, "Max series that this ingester can hold (across all tenants). Requests to create additional series will be rejected. This limit only works when using blocks engine. 0 = unlimited.")
	f.Int64Var(&cfg.MaxInflightPushRequests, prefix+"instance-limits.max-inflight-push-requests", 0, "Max inflight push requests that this ingester can handle (across all tenants). Additional requests will be rejected. 0 = unlimited.")
	f.Int64Var(&cfg.MaxInflightQueryRequests, prefix+"instance-limits.max-inflight-query-requests", 0, "Max inflight query requests that this ingester can handle (across all tenants). Additional requests will be rejected. 0 = unlimited.")
	cfg.InstanceLimits.RegisterFlagsWithPrefix(f, prefix)
}

func (cfg *InstanceLimits) Validate(monitoredResources flagext.StringSliceCSV) error {
	return cfg.InstanceLimits.Validate(monitoredResources)
}

// UnmarshalYAML implements the yaml.Unmarshaler interface. If give
func (l *InstanceLimits) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if defaultInstanceLimits != nil {
		*l = *defaultInstanceLimits
	}
	type plain InstanceLimits // type indirection to make sure we don't go into recursive loop
	return unmarshal((*plain)(l))
}
