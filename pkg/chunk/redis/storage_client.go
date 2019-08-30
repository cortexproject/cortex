package redis

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-kit/kit/log/level"
	"github.com/gomodule/redigo/redis"
	"github.com/prometheus/common/model"

	"github.com/cortexproject/cortex/pkg/chunk"
	chunkutil "github.com/cortexproject/cortex/pkg/chunk/util"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

// override for testing purposes
var nowFunc = model.Now

// Config values for redis connections
type Config struct {
	Redis          flagext.Strings
	MaxIdle        int
	MaxActive      int
	IdleTimeout    time.Duration
	ChunkRetention time.Duration
}

// RegisterFlags registers CLI flags for Config
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.Var(&cfg.Redis, "redis.url", "Redis endpoint URL(s). When multiple redis addresses are provided keys will be sharded "+
		"across the instances using a consistent hash. Use inmemory:///localhost to use a mock, in-memory implementation.")

	f.IntVar(&cfg.MaxIdle, "redis.max-idle", 32, "maximum idle connections to redis")
	f.IntVar(&cfg.MaxActive, "redis.max-active", 64, "maximum active connections to redis")
	f.DurationVar(&cfg.IdleTimeout, "redis.idle-timeout", time.Second, "redis connection idle timeout")
	f.DurationVar(&cfg.ChunkRetention, "redis.chunk-retention", 0, "how long chunks should live in redis. 0 means forever.")
}

// ObjectClient is an object client that connects to Redis
type ObjectClient struct {
	Pool         *Pool
	memoryServer *miniredis.Miniredis
	retention    time.Duration
}

// NewRedisObjectClient makes a new Redis-backed ObjectClient.
func NewRedisObjectClient(cfg Config, _ chunk.SchemaConfig) (*ObjectClient, error) {
	if len(cfg.Redis) == 0 {
		return nil, fmt.Errorf("no URL specified for Redis")
	}

	client := &ObjectClient{
		retention: cfg.ChunkRetention,
	}

	if len(cfg.Redis) == 1 && strings.HasPrefix(cfg.Redis[0], "inmemory:///") {
		srv, err := miniredis.Run()
		if err != nil {
			level.Error(util.Logger).Log("msg", "starting miniredis failed", "err", err)
			return nil, fmt.Errorf("failed to start miniredis: %+v", err)
		}
		client.memoryServer = srv
		cfg.Redis = []string{srv.Addr()}
		level.Debug(util.Logger).Log("msg", "miniredis server started", "addr", cfg.Redis)
	}

	pool := NewPool(cfg)
	client.Pool = pool
	return client, nil
}

// Stop closes the redis connections and the memory server if applicable
func (a ObjectClient) Stop() {
	a.Pool.Close()
	if a.memoryServer != nil {
		a.memoryServer.Close()
	}
}

// GetChunks retrieves the provided chunks from the store
func (a ObjectClient) GetChunks(ctx context.Context, chunks []chunk.Chunk) ([]chunk.Chunk, error) {
	return chunkutil.GetParallelChunks(ctx, chunks, a.getChunk)
}

func (a ObjectClient) getChunk(ctx context.Context, decodeContext *chunk.DecodeContext, c chunk.Chunk) (chunk.Chunk, error) {
	con, err := a.Pool.Get(ctx, c.ExternalKey())
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to get redis connection", "err", err)
		return chunk.Chunk{}, err
	}
	defer con.Close()
	resp, err := redis.Bytes(con.Do("GET", c.ExternalKey()))
	if err != nil {
		if err == redis.ErrNil {
			return chunk.Chunk{}, nil
		}
		return chunk.Chunk{}, fmt.Errorf("failed to get chunk from redis: %+v", err)
	}
	if err := c.Decode(decodeContext, resp); err != nil {
		return chunk.Chunk{}, err
	}
	return c, nil
}

// PutChunks stores the provided chunks into the store
func (a ObjectClient) PutChunks(ctx context.Context, chunks []chunk.Chunk) error {
	incomingErrors := make(chan error)
	now := nowFunc()

	var putChunk = func(c chunk.Chunk) {
		key := c.ExternalKey()
		buf, err := c.Encoded()
		if err != nil {
			level.Error(util.Logger).Log("msg", "failed encoding chunk", "key", key, "err", err)
			incomingErrors <- err
		}

		// TTL is the Through date plus the configured retention time so that the newest chunk is kept for the retention period
		ttl := int(c.Through.Add(a.retention).Unix() - now.Unix())
		if ttl < 0 {
			level.Debug(util.Logger).Log("msg", "chunk is already expired and will be dropped", "key", key)
			incomingErrors <- nil
		}
		incomingErrors <- a.putRedisChunk(ctx, key, buf, ttl)
	}

	for _, c := range chunks {
		go putChunk(c)
	}

	var lastErr error
	for range chunks {
		err := <-incomingErrors
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (a ObjectClient) putRedisChunk(ctx context.Context, key string, buf []byte, ttl int) error {
	con, err := a.Pool.Get(ctx, key)
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to get connection to redis", "key", key, "err", err)
		return err
	}
	defer con.Close()
	args := []interface{}{key, buf}
	if ttl > 0 {
		args = append(args, "EX", ttl)
	}
	_, err = con.Do("SET", args...)
	if err != nil {
		level.Error(util.Logger).Log("msg", "putRedisChunk failed", "key", key, "err", err)
	}
	return err
}
