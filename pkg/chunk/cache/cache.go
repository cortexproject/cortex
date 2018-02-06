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
	FetchChunkData(ctx context.Context, keys []string) (found []string, bufs [][]byte, missing []string, err error)
	Stop() error
}

// Config for building Caches.
type Config struct {
	WriteBackGoroutines int
	WriteBackBuffer     int
	EnableDiskcache     bool

	memcache       MemcachedConfig
	memcacheClient MemcachedClientConfig
	diskcache      DiskcacheConfig
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.EnableDiskcache, "cache.enable-diskcache", false, "Enable on-disk cache")
	f.IntVar(&cfg.WriteBackGoroutines, "memcache.write-back-goroutines", 10, "How many goroutines to use to write back to memcache.")
	f.IntVar(&cfg.WriteBackBuffer, "memcache.write-back-buffer", 10000, "How many chunks to buffer for background write back.")

	cfg.memcache.RegisterFlags(f)
	cfg.memcacheClient.RegisterFlags(f)
	cfg.diskcache.RegisterFlags(f)
}

// New creates a new Cache using Config.
func New(cfg Config) (Cache, error) {
	caches := []Cache{}

	if cfg.memcacheClient.Host != "" {
		client := newMemcachedClient(cfg.memcacheClient)
		caches = append(caches, NewMemcached(cfg.memcache, client))
	}

	if cfg.EnableDiskcache {
		cache, err := NewDiskcache(cfg.diskcache)
		if err != nil {
			return nil, err
		}
		caches = append(caches, cache)
	}

	return multiCache(caches), nil
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

// NewBackground returns a new Cache that does stores on background goroutines.
func NewBackground(cfg Config) (Cache, error) {
	cache, err := New(cfg)
	if err != nil {
		return nil, err
	}

	c := &backgroundCache{
		Cache:    cache,
		quit:     make(chan struct{}),
		bgWrites: make(chan backgroundWrite, cfg.WriteBackBuffer),
	}

	c.wg.Add(cfg.WriteBackGoroutines)
	for i := 0; i < cfg.WriteBackGoroutines; i++ {
		go c.writeBackLoop()
	}

	return c, nil
}

// Stop the background flushing goroutines.
func (c *backgroundCache) Stop() error {
	close(c.quit)
	c.wg.Wait()

	return c.Cache.Stop()
}

// StoreChunk writes chunks for the cache in the background.
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

type multiCache []Cache

func (m multiCache) StoreChunk(ctx context.Context, key string, buf []byte) error {
	for _, c := range []Cache(m) {
		if err := c.StoreChunk(ctx, key, buf); err != nil {
			return err
		}
	}
	return nil
}

func (m multiCache) FetchChunkData(ctx context.Context, keys []string) ([]string, [][]byte, []string, error) {
	found := make(map[string][]byte, len(keys))
	missing := keys

	for _, c := range []Cache(m) {
		var (
			err      error
			passKeys []string
			passBufs [][]byte
		)

		passKeys, passBufs, missing, err = c.FetchChunkData(ctx, missing)
		if err != nil {
			return nil, nil, nil, err
		}

		for i, key := range passKeys {
			found[key] = passBufs[i]
		}
	}

	resultKeys := make([]string, 0, len(found))
	resultBufs := make([][]byte, 0, len(found))
	for _, key := range keys {
		if buf, ok := found[key]; ok {
			resultKeys = append(resultKeys, key)
			resultBufs = append(resultBufs, buf)
		}
	}

	return resultKeys, resultBufs, missing, nil
}

func (m multiCache) Stop() error {
	for _, c := range []Cache(m) {
		if err := c.Stop(); err != nil {
			return err
		}
	}
	return nil
}
