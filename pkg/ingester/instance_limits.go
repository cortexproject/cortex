package ingester

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
	// We don't include values in the message to avoid leaking Cortex cluster configuration to users.
	return "cannot push more samples: ingester's max samples push rate reached"
}

type errMaxUsersLimitReached struct {
	users int64
	limit int64
}

func (e errMaxUsersLimitReached) Error() string {
	// We don't include values in the message to avoid leaking Cortex cluster configuration to users.
	return "cannot create TSDB: ingesters's max tenants limit reached"
}

type errMaxSeriesLimitReached struct {
	series int64
	limit  int64
}

func (e errMaxSeriesLimitReached) Error() string {
	// We don't include values in the message to avoid leaking Cortex cluster configuration to users.
	return "cannot add series: ingesters's max series limit reached"
}

type errTooManyInflightPushRequests struct {
	requests int64
	limit    int64
}

func (e errTooManyInflightPushRequests) Error() string {
	// We don't include values in the message to avoid leaking Cortex cluster configuration to users.
	return "cannot push: too many inflight push requests"
}
