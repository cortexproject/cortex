package tsdb

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb/labels"
)

// IndexCache is a copy of store.indexCache that's been exported
type IndexCache interface {
	SetPostings(b ulid.ULID, l labels.Label, v []byte)
	Postings(b ulid.ULID, l labels.Label) ([]byte, bool)
	SetSeries(b ulid.ULID, id uint64, v []byte)
	Series(b ulid.ULID, id uint64) ([]byte, bool)
}

// IndexMemcachedConfig is the config for an IndexMemcached
type IndexMemcachedConfig struct {
	CacheConfig cache.Config  `yaml:"cache"`
	Timeout     time.Duration `yaml:"timeout"`
}

// RegisterFlags for the IndexMemcached
func (cfg *IndexMemcachedConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.CacheConfig.RegisterFlagsWithPrefix("experimental.index-memcached", "IndexMemcached", f)
	f.DurationVar(&cfg.Timeout, "experimental.index-memcached.timeout", 3*time.Second, "timeout for cache commands")
}

// IndexMemcached is a IndexCache on top of memcached
type IndexMemcached struct {
	c       cache.Cache
	timeout time.Duration
}

func key(b ulid.ULID, l labels.Label) string {
	return b.String() + ":" + l.Name + ":" + l.Value
}

// NewMemcachedIndex implements an index cache with a memcached client
func NewMemcachedIndex(cfg IndexMemcachedConfig) (*IndexMemcached, error) {
	c, err := cache.New(cfg.CacheConfig)
	if err != nil {
		return nil, err
	}

	return &IndexMemcached{
		c:       c,
		timeout: cfg.Timeout,
	}, nil
}

// SetPostings implements the indexCache interface
func (i *IndexMemcached) SetPostings(b ulid.ULID, l labels.Label, v []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), i.timeout)
	defer cancel()

	i.c.Store(ctx, []string{key(b, l)}, [][]byte{v})
}

// Postings implements the indexCache interface
func (i *IndexMemcached) Postings(b ulid.ULID, l labels.Label) ([]byte, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), i.timeout)
	defer cancel()

	_, v, _ := i.c.Fetch(ctx, []string{key(b, l)})
	if len(v) > 0 {
		return v[0], true
	}

	return nil, false
}

// SetSeries implements the indexCache interface
func (i *IndexMemcached) SetSeries(b ulid.ULID, id uint64, v []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), i.timeout)
	defer cancel()

	i.c.Store(ctx, []string{b.String() + fmt.Sprintf("%v", id)}, [][]byte{v})
}

// Series implements the indexCache interface
func (i *IndexMemcached) Series(b ulid.ULID, id uint64) ([]byte, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), i.timeout)
	defer cancel()

	_, v, _ := i.c.Fetch(ctx, []string{b.String() + fmt.Sprintf("%v", id)})
	if len(v) > 0 {
		return v[0], true
	}

	return nil, false
}
