package cache

import (
	"context"
	"flag"
	"sync"

	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/cortex/pkg/util"
)

// Cache byte arrays by key.
type Cache interface {
	StoreChunk(ctx context.Context, key string, buf []byte) error
	FetchChunkData(ctx context.Context, keys []string) (found []string, bufs [][]byte, err error)
	Stop() error
}

// Config for building Caches.
type Config struct {
	WriteBackGoroutines int
	WriteBackBuffer     int

	memcache       MemcachedConfig
	memcacheClient MemcachedClientConfig

	disk DiskcacheConfig
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.WriteBackGoroutines, "memcache.write-back-goroutines", 10, "How many goroutines to use to write back to memcache.")
	f.IntVar(&cfg.WriteBackBuffer, "memcache.write-back-buffer", 10000, "How many chunks to buffer for background write back.")

	cfg.memcache.RegisterFlags(f)
	cfg.memcacheClient.RegisterFlags(f)
	cfg.disk.RegisterFlags(f)
}

func New(cfg Config) Cache {
	if cfg.memcacheClient.Host == "" {
		return noopCache{}
	}

	client := newMemcachedClient(cfg.memcacheClient)
	return NewMemcached(cfg.memcache, client)
}

type backgroundCache struct {
	Cache

	wg       sync.WaitGroup
	quit     chan struct{}
	bgWrites chan backgroundWrite
}

type backgroundWrite struct {
	key string
	buf []byte
}

func NewBackground(cfg Config) Cache {
	c := &backgroundCache{
		quit:     make(chan struct{}),
		bgWrites: make(chan backgroundWrite, cfg.WriteBackBuffer),
	}

	c.wg.Add(cfg.WriteBackGoroutines)
	for i := 0; i < cfg.WriteBackGoroutines; i++ {
		go c.writeBackLoop()
	}

	return c
}

// Stop the background flushing goroutines.
func (c *backgroundCache) Stop() error {
	close(c.quit)
	c.wg.Wait()

	return c.Cache.Stop()
}

// BackgroundWrite writes chunks for the cache in the background
func (c *backgroundCache) StoreChunk(ctx context.Context, key string, buf []byte) error {
	bgWrite := backgroundWrite{
		key: key,
		buf: buf,
	}
	select {
	case c.bgWrites <- bgWrite:
	default:
		memcacheDroppedWriteBack.Inc()
	}
	return nil
}

func (c *backgroundCache) writeBackLoop() {
	defer c.wg.Done()

	for {
		select {
		case bgWrite := <-c.bgWrites:
			err := c.Cache.StoreChunk(context.Background(), bgWrite.key, bgWrite.buf)
			if err != nil {
				level.Error(util.Logger).Log("msg", "error writing to memcache", "err", err)
			}
		case <-c.quit:
			return
		}
	}
}

type noopCache struct{}

func (noopCache) StoreChunk(ctx context.Context, key string, buf []byte) error {
	return nil
}

func (noopCache) FetchChunkData(ctx context.Context, keys []string) (found []string, bufs [][]byte, err error) {
	return nil, nil, nil
}

func (noopCache) Stop() error {
	return nil
}
