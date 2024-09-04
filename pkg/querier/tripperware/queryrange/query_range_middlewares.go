// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Mostly lifted from prometheus/web/api/v1/api.go.

package queryrange

import (
	"flag"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/querysharding"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

const day = 24 * time.Hour

// Config for query_range middleware chain.
type Config struct {
	SplitQueriesByInterval time.Duration `yaml:"split_queries_by_interval"`
	AlignQueriesWithStep   bool          `yaml:"align_queries_with_step"`
	ResultsCacheConfig     `yaml:"results_cache"`
	CacheResults           bool `yaml:"cache_results"`
	MaxRetries             int  `yaml:"max_retries"`
	// List of headers which query_range middleware chain would forward to downstream querier.
	ForwardHeaders flagext.StringSlice `yaml:"forward_headers_list"`

	// Populated based on the query configuration
	VerticalShardSize int `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxRetries, "querier.max-retries-per-request", 5, "Maximum number of retries for a single request; beyond this, the downstream error is returned.")
	f.DurationVar(&cfg.SplitQueriesByInterval, "querier.split-queries-by-interval", 0, "Split queries by an interval and execute in parallel, 0 disables it. You should use an a multiple of 24 hours (same as the storage bucketing scheme), to avoid queriers downloading and processing the same chunks. This also determines how cache keys are chosen when result caching is enabled")
	f.BoolVar(&cfg.AlignQueriesWithStep, "querier.align-querier-with-step", false, "Mutate incoming queries to align their start and end with their step.")
	f.BoolVar(&cfg.CacheResults, "querier.cache-results", false, "Cache query results.")
	f.Var(&cfg.ForwardHeaders, "frontend.forward-headers-list", "List of headers forwarded by the query Frontend to downstream querier.")
	cfg.ResultsCacheConfig.RegisterFlags(f)
}

// Validate validates the config.
func (cfg *Config) Validate(qCfg querier.Config) error {
	if cfg.CacheResults {
		if cfg.SplitQueriesByInterval <= 0 {
			return errors.New("querier.cache-results may only be enabled in conjunction with querier.split-queries-by-interval. Please set the latter")
		}
		if err := cfg.ResultsCacheConfig.Validate(qCfg); err != nil {
			return errors.Wrap(err, "invalid ResultsCache config")
		}
	}
	return nil
}

// Middlewares returns list of middlewares that should be applied for range query.
func Middlewares(
	cfg Config,
	log log.Logger,
	limits tripperware.Limits,
	cacheExtractor Extractor,
	registerer prometheus.Registerer,
	queryAnalyzer querysharding.Analyzer,
	prometheusCodec tripperware.Codec,
	shardedPrometheusCodec tripperware.Codec,
	lookbackDelta time.Duration,
) ([]tripperware.Middleware, cache.Cache, error) {
	// Metric used to keep track of each middleware execution duration.
	metrics := tripperware.NewInstrumentMiddlewareMetrics(registerer)

	queryRangeMiddleware := []tripperware.Middleware{NewLimitsMiddleware(limits, lookbackDelta)}
	if cfg.AlignQueriesWithStep {
		queryRangeMiddleware = append(queryRangeMiddleware, tripperware.InstrumentMiddleware("step_align", metrics), StepAlignMiddleware)
	}
	if cfg.SplitQueriesByInterval != 0 {
		staticIntervalFn := func(_ tripperware.Request) time.Duration { return cfg.SplitQueriesByInterval }
		queryRangeMiddleware = append(queryRangeMiddleware, tripperware.InstrumentMiddleware("split_by_interval", metrics), SplitByIntervalMiddleware(staticIntervalFn, limits, prometheusCodec, registerer))
	}

	var c cache.Cache
	if cfg.CacheResults {
		shouldCache := func(r tripperware.Request) bool {
			if v, ok := r.(*tripperware.PrometheusRequest); ok {
				return !v.CachingOptions.Disabled
			}
			return false
		}
		queryCacheMiddleware, cache, err := NewResultsCacheMiddleware(log, cfg.ResultsCacheConfig, constSplitter(cfg.SplitQueriesByInterval), limits, prometheusCodec, cacheExtractor, shouldCache, registerer)
		if err != nil {
			return nil, nil, err
		}
		c = cache
		queryRangeMiddleware = append(queryRangeMiddleware, tripperware.InstrumentMiddleware("results_cache", metrics), queryCacheMiddleware)
	}

	queryRangeMiddleware = append(queryRangeMiddleware, tripperware.InstrumentMiddleware("shardBy", metrics), tripperware.ShardByMiddleware(log, limits, shardedPrometheusCodec, queryAnalyzer))

	return queryRangeMiddleware, c, nil
}
