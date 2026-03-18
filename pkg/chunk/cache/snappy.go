package cache

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/snappy"

	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

type snappyCache struct {
	next   Cache
	logger log.Logger
}

// NewSnappy makes a new snappy encoding cache wrapper.
func NewSnappy(next Cache, logger log.Logger) Cache {
	return &snappyCache{
		next:   next,
		logger: logger,
	}
}

func (s *snappyCache) Store(ctx context.Context, keys []string, bufs [][]byte, ttl time.Duration) {
	cs := make([][]byte, 0, len(bufs))
	for _, buf := range bufs {
		c := snappy.Encode(nil, buf)
		cs = append(cs, c)
	}
	s.next.Store(ctx, keys, cs, ttl)
}

func (s *snappyCache) Fetch(ctx context.Context, keys []string, ttl time.Duration) ([]string, [][]byte, []string) {
	found, bufs, missing := s.next.Fetch(ctx, keys, ttl)
	ds := make([][]byte, 0, len(bufs))
	for _, buf := range bufs {
		d, err := snappy.Decode(nil, buf)
		if err != nil {
			level.Error(util_log.WithContext(ctx, s.logger)).Log("msg", "failed to decode cache entry", "err", err)
			return nil, nil, keys
		}
		ds = append(ds, d)
	}
	return found, ds, missing
}

func (s *snappyCache) Stop() {
	s.next.Stop()
}
