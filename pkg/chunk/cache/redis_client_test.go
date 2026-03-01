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

	tests := []struct {
		name   string
		client *RedisClient
	}{
		{
			name:   "single redis client",
			client: single,
		},
		{
			name:   "cluster redis client",
			client: cluster,
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

func TestRedisClusterCrossSlotMSet(t *testing.T) {
	cluster, err := mockRedisClientCluster()
	require.Nil(t, err)
	defer cluster.Close()

	ctx := context.Background()

	// Use keys with different hash tags to ensure they hash to different slots
	// In Redis Cluster, hash tags {xxx} determine which slot a key belongs to
	keys := []string{
		"{user1}:cache:query",
		"{user2}:cache:query",
		"{user3}:cache:query",
		"{user4}:cache:query",
		"{user5}:cache:query",
	}
	values := [][]byte{
		[]byte("data1"),
		[]byte("data2"),
		[]byte("data3"),
		[]byte("data4"),
		[]byte("data5"),
	}

	// This should not fail even with cross-slot keys in cluster mode
	err = cluster.MSet(ctx, keys, values)
	require.Nil(t, err, "MSet should work with cross-slot keys in cluster mode")

	// Verify all keys were set correctly
	retrieved, err := cluster.MGet(ctx, keys)
	require.Nil(t, err)
	require.Len(t, retrieved, len(keys))

	for i, value := range retrieved {
		require.Equal(t, values[i], value, "Retrieved value should match set value for key %s", keys[i])
	}
}
