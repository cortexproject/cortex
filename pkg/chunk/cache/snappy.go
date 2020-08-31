package cache

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/snappy"
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

func (s *snappyCache) Store(ctx context.Context, data map[string][]byte) {
	cs := make(map[string][]byte, len(data))
	for k, v := range data {
		c := snappy.Encode(nil, v)
		cs[k] = c
	}
	s.next.Store(ctx, cs)
}

func (s *snappyCache) Fetch(ctx context.Context, keys []string) (map[string][]byte, []string) {
	found, missing := s.next.Fetch(ctx, keys)
	for k, v := range found {
		d, err := snappy.Decode(nil, v)
		if err != nil {
			level.Error(s.logger).Log("msg", "failed to decode cache entry", "err", err)
			return nil, keys
		}
		found[k] = d
	}
	return found, missing
}

func (s *snappyCache) Stop() {
	s.next.Stop()
}
