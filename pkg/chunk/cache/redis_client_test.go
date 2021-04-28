package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestRedisClient(t *testing.T) {
	single, err := mockRedisClientSingle()
	require.Nil(t, err)
	defer single.Close()

	cluster, err := mockRedisClientCluster()
	require.Nil(t, err)
	defer cluster.Close()

	ctx := context.Background()

	clients := []*RedisClient{single, cluster}
	for i, c := range clients {
		meg := []string{"run on single redis client", "run on cluster redis client"}[i]

		keys := []string{"key1", "key2", "key3"}
		bufs := [][]byte{[]byte("data1"), []byte("data2"), []byte("data3")}
		miss := []string{"miss1", "miss2"}

		// set values
		err := c.MSet(ctx, keys, bufs)
		require.Nil(t, err, meg)

		// get keys
		values, err := c.MGet(ctx, keys)
		require.Nil(t, err, meg)
		require.Len(t, values, len(keys), meg)
		for i, value := range values {
			require.Equal(t, values[i], value, meg)
		}

		// get missing keys
		values, err = c.MGet(ctx, miss)
		require.Nil(t, err, meg)
		require.Len(t, values, len(miss), meg)
		for _, value := range values {
			require.Nil(t, value, meg)
		}
	}
}

func mockRedisClientSingle() (*RedisClient, error) {
	redisServer, err := miniredis.Run()
	if err != nil {
		return nil, err
	}
	return &RedisClient{
		expiration: time.Minute,
		timeout:    100 * time.Millisecond,
		rdb: redis.NewClient(&redis.Options{
			Addr: redisServer.Addr(),
		}),
	}, nil
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
	return &RedisClient{
		expiration: time.Minute,
		timeout:    100 * time.Millisecond,
		rdb: redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{
				redisServer1.Addr(),
				redisServer2.Addr(),
			},
		}),
	}, nil
}
