package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/gomodule/redigo/redis"
	"github.com/rafaeljusto/redigomock"
	"github.com/stretchr/testify/require"
)

type redisMockClient struct {
	conn redis.Conn
}

func (c *redisMockClient) Connection() redis.Conn {
	return c.conn
}

func (c *redisMockClient) Close() error {
	return nil
}

func TestRedisCache(t *testing.T) {
	cfg := cache.RedisConfig{
		Timeout: 10 * time.Millisecond,
	}

	conn := redigomock.NewConn()
	client := &redisMockClient{conn: conn}
	conn.Clear()

	keys := []string{"key1", "key2", "key3"}
	bufs := [][]byte{[]byte("data1"), []byte("data2"), []byte("data3")}
	intf := make([]interface{}, len(keys))

	for i, key := range keys {
		conn.Command("SETEX", key, 0, bufs[i]).Expect("ok")
		intf[i] = key
	}
	conn.Command("MGET", intf...).Expect(bufs)

	c := cache.NewRedisCache(cfg, "mock", client)
	ctx := context.Background()

	c.Store(ctx, keys, bufs)

	_, _, missed := c.Fetch(ctx, keys)
	require.Len(t, missed, 0)

	// TODO add key/value verification
}
