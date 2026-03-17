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
			err := tt.client.MSet(ctx, keys, bufs, 0)
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
	err = cluster.MSet(ctx, keys, values, 0)
	require.Nil(t, err, "MSet should work with cross-slot keys in cluster mode")

	// Verify all keys were set correctly
	retrieved, err := cluster.MGet(ctx, keys)
	require.Nil(t, err)
	require.Len(t, retrieved, len(keys))

	for i, value := range retrieved {
		require.Equal(t, values[i], value, "Retrieved value should match set value for key %s", keys[i])
	}
}

func TestRedisTTLFallback(t *testing.T) {
	defaultExpiration := 100 * time.Second
	client, err := mockRedisClientWithExpiration(defaultExpiration)
	require.Nil(t, err)
	defer client.Close()

	ctx := context.Background()

	// Test 1: TTL=0 should use configured expiration (100s)
	err = client.MSet(ctx, []string{"key1"}, [][]byte{[]byte("value1")}, 0)
	require.Nil(t, err, "MSet with TTL=0 should succeed and use default expiration")

	// Verify key was set with correct value
	values, err := client.MGet(ctx, []string{"key1"})
	require.Nil(t, err)
	require.Equal(t, []byte("value1"), values[0])

	// Verify TTL is set to default expiration (100s, allow 2s delta for test execution time)
	ttl, err := client.rdb.TTL(ctx, "key1").Result()
	require.Nil(t, err)
	require.InDelta(t, defaultExpiration.Seconds(), ttl.Seconds(), 10.0,
		"TTL=0 should use configured default expiration")

	// Test 2: Custom TTL should override configured expiration
	customTTL := 20 * time.Second
	err = client.MSet(ctx, []string{"key2"}, [][]byte{[]byte("value2")}, customTTL)
	require.Nil(t, err, "MSet with custom TTL should succeed")

	// Verify key was set with correct value
	values, err = client.MGet(ctx, []string{"key2"})
	require.Nil(t, err)
	require.Equal(t, []byte("value2"), values[0])

	// Verify TTL is set to custom value (20s, allow 2s delta for test execution time)
	ttl, err = client.rdb.TTL(ctx, "key2").Result()
	require.Nil(t, err)
	require.InDelta(t, customTTL.Seconds(), ttl.Seconds(), 10.0,
		"custom TTL should override default expiration")
}

func mockRedisClientWithExpiration(expiration time.Duration) (*RedisClient, error) {
	redisServer, err := miniredis.Run()
	if err != nil {
		return nil, err
	}
	return &RedisClient{
		expiration: expiration,
		timeout:    100 * time.Millisecond,
		rdb: redis.NewClient(&redis.Options{
			Addr: redisServer.Addr(),
		}),
	}, nil
}
