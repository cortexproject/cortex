package ingester

import "fmt"

// InstanceLimits describes limits used by ingester. Reaching any of these will result in Push method to return
// (internal) error.
type InstanceLimits struct {
	MaxIngestionRate        float64 `yaml:"max_ingestion_rate"`
	MaxInMemoryTenants      int64   `yaml:"max_tenants"`
	MaxInMemorySeries       int64   `yaml:"max_series"`
	MaxInflightPushRequests int64   `yaml:"max_inflight_push_requests"`
}

// Sets default limit values for unmarshalling.
var defaultInstanceLimits *InstanceLimits = nil

// UnmarshalYAML implements the yaml.Unmarshaler interface. If give
func (l *InstanceLimits) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if defaultInstanceLimits != nil {
		*l = *defaultInstanceLimits
	}
	type plain InstanceLimits // type indirection to make sure we don't go into recursive loop
	return unmarshal((*plain)(l))
}

type errMaxSamplesPushRateLimitReached struct {
	rate  float64
	limit float64
}

func (e errMaxSamplesPushRateLimitReached) Error() string {
	return fmt.Sprintf("cannot push more samples: ingester's max samples push rate reached, rate=%g, limit=%g", e.rate, e.limit)
}

type errMaxUsersLimitReached struct {
	users int64
	limit int64
}

func (e errMaxUsersLimitReached) Error() string {
	return fmt.Sprintf("cannot create TSDB: ingesters's max users limit reached, users=%d, limit=%d", e.users, e.limit)
}

type errMaxSeriesLimitReached struct {
	series int64
	limit  int64
}

func (e errMaxSeriesLimitReached) Error() string {
	return fmt.Sprintf("cannot add series: ingesters's max series limit reached, series=%d, limit=%d", e.series, e.limit)
}

type errTooManyInflightPushRequests struct {
	requests int64
	limit    int64
}

func (e errTooManyInflightPushRequests) Error() string {
	return fmt.Sprintf("cannot push: too many inflight push requests, requests=%d, limit=%d", e.requests, e.limit)
}
