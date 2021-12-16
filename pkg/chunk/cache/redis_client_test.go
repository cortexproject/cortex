package cache

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestRedisClient(t *testing.T) {
	single, err := mockRedisClientSingle()
	require.Nil(t, err)
	defer single.Close()

	singleDS, err := mockRedisClientSingleWithDNS()
	require.Nil(t, err)
	defer singleDS.Close()

	cluster, err := mockRedisClientCluster()
	require.Nil(t, err)
	defer cluster.Close()

	clusterHosts, err := mockRedisClientClusterWithHosts()
	require.Nil(t, err)
	defer clusterHosts.Close()

	ctx := context.Background()

	tests := []struct {
		name   string
		client *RedisClient
	}{
		{
			name:   "single redis client",
			client: single,
		},
		{
			name:   "single redis client with dns discovery",
			client: singleDS,
		},
		{
			name:   "cluster redis client",
			client: cluster,
		},
		{
			name:   "cluster redis client with hosts",
			client: clusterHosts,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys := []string{"key1", "key2", "key3"}
			bufs := [][]byte{[]byte("data1"), []byte("data2"), []byte("data3")}
			miss := []string{"miss1", "miss2"}

			// set values
			err := tt.client.MSet(ctx, keys, bufs)
			require.Nil(t, err)

			// get keys
			values, err := tt.client.MGet(ctx, keys)
			require.Nil(t, err)
			require.Len(t, values, len(keys))
			for i, value := range values {
				require.Equal(t, values[i], value)
			}

			// get missing keys
			values, err = tt.client.MGet(ctx, miss)
			require.Nil(t, err)
			require.Len(t, values, len(miss))
			for _, value := range values {
				require.Nil(t, value)
			}
		})
	}
}

func mockRedisClientSingle() (*RedisClient, error) {
	redisServer, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	cfg := &RedisConfig{
		Endpoint: redisServer.Addr(),
	}
	return NewRedisClient(cfg, log.NewNopLogger())
}

func mockRedisClientSingleWithDNS() (*RedisClient, error) {
	redisServer, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	cfg := &RedisConfig{
		Addresses: fmt.Sprintf("dns+localhost:%s", redisServer.Port()),
	}
	return NewRedisClient(cfg, log.NewNopLogger())
}

func mockRedisClientCluster() (*RedisClient, error) {
	redisServer1, err := miniredis.Run()
	if err != nil {
		return nil, err
	}
	redisServer2, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	cfg := &RedisConfig{
		Endpoint: fmt.Sprintf("%s,%s", redisServer1.Addr(), redisServer2.Addr()),
	}
	return NewRedisClient(cfg, log.NewNopLogger())
}

func mockRedisClientClusterWithHosts() (*RedisClient, error) {
	redisServer1, err := miniredis.Run()
	if err != nil {
		return nil, err
	}
	redisServer2, err := miniredis.Run()
	if err != nil {
		return nil, err
	}
	cfg := &RedisConfig{
		Addresses: fmt.Sprintf("localhost:%s,localhost:%s", redisServer1.Port(), redisServer2.Port()),
	}
	return NewRedisClient(cfg, log.NewNopLogger())
}
