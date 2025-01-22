package queryrange

import (
	"flag"
	"time"
)

type DynamicQuerySplitsConfig struct {
	MaxShardsPerQuery                           int           `yaml:"max_shards_per_query"`
	MaxDurationOfDataFetchedFromStoragePerQuery time.Duration `yaml:"max_duration_of_data_fetched_from_storage_per_query"`
}

// RegisterFlags registers flags foy dynamic query splits
func (cfg *DynamicQuerySplitsConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxShardsPerQuery, "querier.max-shards-per-query", 0, "[EXPERIMENTAL] Maximum number of shards for a query, 0 disables it. Dynamically uses a multiple of `split-queries-by-interval` to maintain the number of splits below the limit. If vertical sharding is enabled for a query, the combined total number of vertical and interval shards is kept below this limit.")
	f.DurationVar(&cfg.MaxDurationOfDataFetchedFromStoragePerQuery, "querier.max-duration-of-data-fetched-from-storage-per-query", 0, "[EXPERIMENTAL] Max total duration of data fetched by all query shards from storage, 0 disables it. Dynamically uses a multiple of `split-queries-by-interval` to ensure the total fetched duration of data is lower than the value set. It takes into account additional data fetched by matrix selectors and subqueries.")
}
