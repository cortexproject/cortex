package kv

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util/jump"
)

// ShardedConfig for a new ShardedClient.
type ShardedConfig struct {
	Shards []Config `yaml:"shards"`
}

// ShardedClient implements ring.KVClient for sharded KV store.
type ShardedClient struct {
	cfg ShardedConfig

	shards []Client
}

func buildShardedClient(cfg ShardedConfig, codec codec.Codec, reg prometheus.Registerer) (*ShardedClient, error) {
	var shards []Client
	for _, shardCfg := range cfg.Shards {
		shard, err := NewClient(shardCfg, codec, reg)
		if err != nil {
			return nil, err
		}

		shards = append(shards, shard)
	}

	return &ShardedClient{
		cfg:    cfg,
		shards: shards,
	}, nil
}

func (c *ShardedClient) List(ctx context.Context, prefix string) ([]string, error) {
	g, ctx := errgroup.WithContext(ctx)

	rss := make(chan []string)
	for _, shard := range c.shards {
		shard := shard
		g.Go(func() error {
			rs, err := shard.List(ctx, prefix)
			if err != nil {
				return err
			}

			rss <- rs
			return nil
		})
	}
	go func() {
		_ = g.Wait()
		close(rss)
	}()

	var result []string
	for rs := range rss {
		result = append(result, rs...)
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *ShardedClient) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, shard := range c.shards {
		go func(shard Client) {
			shard.WatchPrefix(ctx, prefix, f)
			cancel()
		}(shard)
	}

	<-ctx.Done()
}

func (c *ShardedClient) Get(ctx context.Context, key string) (interface{}, error) {
	i := jump.Hash(key, len(c.shards))
	return c.shards[i].Get(ctx, key)
}

func (c *ShardedClient) Delete(ctx context.Context, key string) error {
	i := jump.Hash(key, len(c.shards))
	return c.shards[i].Delete(ctx, key)
}

func (c *ShardedClient) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	i := jump.Hash(key, len(c.shards))
	return c.shards[i].CAS(ctx, key, f)
}

func (c *ShardedClient) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	i := jump.Hash(key, len(c.shards))
	c.shards[i].WatchKey(ctx, key, f)
}
