package querier

import (
	"flag"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/cortex/pkg/util"
)

// Config contains the configuration require to create a querier
type Config struct {
	MaxConcurrent int
	Timeout       time.Duration
	Iterators     bool
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxConcurrent, "querier.max-concurrent", 20, "The maximum number of concurrent queries.")
	f.DurationVar(&cfg.Timeout, "querier.timeout", 2*time.Minute, "The timeout for a query.")
	if f.Lookup("promql.lookback-delta") == nil {
		f.DurationVar(&promql.LookbackDelta, "promql.lookback-delta", promql.LookbackDelta, "Time since the last sample after which a time series is considered stale and ignored by expression evaluations.")
	}
	f.BoolVar(&cfg.Iterators, "querier.iterators", false, "Use iterators to execute query, as opposed to fully materialising the series in memory.")
}

// Make builds a queryable and promql engine.
func Make(cfg Config, distributor Distributor, chunkStore ChunkStore) (storage.Queryable, *promql.Engine) {
	queryable := NewQueryable(distributor, chunkStore, cfg.Iterators)
	engine := promql.NewEngine(util.Logger, nil, cfg.MaxConcurrent, cfg.Timeout)
	return queryable, engine
}
