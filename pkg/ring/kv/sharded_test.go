package kv

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/etcd"
)

func TestSharded(t *testing.T) {
	var shards []Client
	var closers []io.Closer

	defer func() {
		for _, closer := range closers {
			closer.Close()
		}
	}()

	for i := 0; i < 3; i++ {
		shard, closer, err := etcd.Mock(codec.String{})
		require.NoError(t, err)
		shards = append(shards, shard)
		closers = append(closers, closer)
	}

	sharded := &ShardedClient{
		shards: shards,
	}

	// Put three keys, selected to hash to 3 different shards.
	keys := []string{"fooo", "bar", "baz"}
	for _, key := range keys {
		require.NoError(t, sharded.CAS(context.Background(), key, func(in interface{}) (out interface{}, retry bool, err error) {
			return key, false, nil
		}))
	}

	// Make sure we can get them.
	for _, key := range keys {
		value, err := sharded.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, key, value)
	}

	// Make sure we can list them.
	actual, err := sharded.List(context.Background(), "")
	require.NoError(t, err)
	require.ElementsMatch(t, keys, actual)

	// Make sure each shard got one key.
	for _, shard := range shards {
		keys, err := shard.List(context.Background(), "")
		require.NoError(t, err)
		require.Len(t, keys, 1)
	}
}
