package cache

import "context"

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

func (t tiered) Store(ctx context.Context, data map[string][]byte) {
	for _, c := range []Cache(t) {
		c.Store(ctx, data)
	}
}

func (t tiered) Fetch(ctx context.Context, keys []string) (map[string][]byte, []string) {
	found := make(map[string][]byte, len(keys))
	missing := keys
	previousCaches := make([]Cache, 0, len(t))

	for _, c := range []Cache(t) {

		passData, missing := c.Fetch(ctx, missing)
		tiered(previousCaches).Store(ctx, passData)

		for k, v := range passData {
			found[k] = v
		}

		if len(missing) == 0 {
			break
		}

		previousCaches = append(previousCaches, c)
	}

	return found, missing
}

func (t tiered) Stop() {
	for _, c := range []Cache(t) {
		c.Stop()
	}
}
