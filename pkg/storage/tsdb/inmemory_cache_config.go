package tsdb

import (
	"flag"

	"github.com/thanos-io/thanos/pkg/cache"
	"github.com/thanos-io/thanos/pkg/model"
)

type InMemoryCacheConfig struct {
	MaxSize     uint64 `yaml:"max_size"`
	MaxItemSize uint64 `yaml:"max_item_size"`
}

func (cfg *InMemoryCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.Uint64Var(&cfg.MaxSize, prefix+"inmemory.max-size", 250*1024*1024, "The overall maximum size of stored in memory.")
	f.Uint64Var(&cfg.MaxItemSize, prefix+"inmemory.max-item-size", 125*1024*1024, "The maximum size of an item stored in memory. Bigger items are not stored. Must specify a value less than max_size.")
}

func (cfg InMemoryCacheConfig) ToThanosConfig() cache.InMemoryCacheConfig {
	return cache.InMemoryCacheConfig{
		MaxSize:     model.Bytes(cfg.MaxSize),
		MaxItemSize: model.Bytes(cfg.MaxItemSize),
	}
}
