package querier

import (
	"flag"
	"time"

	"github.com/prometheus/prometheus/promql"
)

// Config contains the configuration require to create a querier
type Config struct {
	MaxConcurrent int
	Timeout       time.Duration
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	flag.IntVar(&cfg.MaxConcurrent, "querier.max-concurrent", 20, "The maximum number of concurrent queries.")
	flag.DurationVar(&cfg.Timeout, "querier.timeout", 2*time.Minute, "The timeout for a query.")
	if flag.Lookup("promql.lookback-delta") == nil {
		flag.DurationVar(&promql.LookbackDelta, "promql.lookback-delta", promql.LookbackDelta, "Time since the last sample after which a time series is considered stale and ignored by expression evaluations.")
	}
}
