package cache

import (
	"context"
	"time"
)

type tiered []Cache

// NewTiered makes a new tiered cache.
func NewTiered(caches []Cache) Cache {
	if len(caches) == 1 {
		return caches[0]
	}

	return tiered(caches)
}

// IsEmptyTieredCache is used to determine whether the current Cache is implemented by an empty tiered.
func IsEmptyTieredCache(cache Cache) bool {
	c, ok := cache.(tiered)
	return ok && len(c) == 0
}

func (t tiered) Store(ctx context.Context, keys []string, bufs [][]byte, ttl time.Duration) {
	for _, c := range []Cache(t) {
		c.Store(ctx, keys, bufs, ttl)
	}
}

func (t tiered) Fetch(ctx context.Context, keys []string, ttl time.Duration) ([]string, [][]byte, []string) {
	found := make(map[string][]byte, len(keys))
	missing := keys
	previousCaches := make([]Cache, 0, len(t))

	for _, c := range []Cache(t) {
		var (
			passKeys []string
			passBufs [][]byte
		)

		passKeys, passBufs, missing = c.Fetch(ctx, missing, ttl)
		tiered(previousCaches).Store(ctx, passKeys, passBufs, ttl)

		for i, key := range passKeys {
			found[key] = passBufs[i]
		}

		if len(missing) == 0 {
			break
		}

		previousCaches = append(previousCaches, c)
	}

	resultKeys := make([]string, 0, len(found))
	resultBufs := make([][]byte, 0, len(found))
	for _, key := range keys {
		if buf, ok := found[key]; ok {
			resultKeys = append(resultKeys, key)
			resultBufs = append(resultBufs, buf)
		}
	}

	return resultKeys, resultBufs, missing
}

func (t tiered) Stop() {
	for _, c := range []Cache(t) {
		c.Stop()
	}
}
