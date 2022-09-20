package instantquery

import (
	"flag"

	"github.com/go-kit/log"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

// Config for instant query middleware chain.
type Config struct {
	VerticalShardSize int `yaml:"vertical_shard_size"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.VerticalShardSize, "querier.vertical-shard-size", 0, "Number of shards to use when distributing shardable PromQL queries.")
}

func Middlewares(
	cfg Config,
	log log.Logger,
	limits tripperware.Limits,
) ([]tripperware.Middleware, error) {
	var m []tripperware.Middleware

	if cfg.VerticalShardSize > 1 {
		m = append(m, tripperware.ShardByMiddleware(log, limits, InstantQueryCodec, cfg.VerticalShardSize))
	}
	return m, nil
}
