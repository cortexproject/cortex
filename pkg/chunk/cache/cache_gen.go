package cache

import (
	"context"
)

// GenNumMiddleware adds gen number to keys from context. Expected size of gen numbers is upto 2 digits.
// If we start seeing problems with keys exceeding length limit, we need to look into resetting gen numbers
type GenNumMiddleware struct {
	downstreamCache Cache
}

// NewCacheGenNumMiddleware creates a new GenNumMiddleware
func NewCacheGenNumMiddleware(downstreamCache Cache) Cache {
	return &GenNumMiddleware{downstreamCache}
}

// Store adds cache gen number to keys before calling Store method of downstream cache
func (c GenNumMiddleware) Store(ctx context.Context, keys []string, buf [][]byte) {
	keys = AddCacheGenNumToCacheKeys(ctx, keys)
	c.downstreamCache.Store(ctx, keys, buf)
}

// Fetch adds cache gen number to keys before calling Fetch method of downstream cache
// It also removes gen number before responding back with found and missing keys to make sure consumer of response gets to see same keys
func (c GenNumMiddleware) Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missing []string) {
	keys = AddCacheGenNumToCacheKeys(ctx, keys)

	found, bufs, missing = c.downstreamCache.Fetch(ctx, keys)

	found = RemoveCacheGenNumFromKeys(ctx, found)
	missing = RemoveCacheGenNumFromKeys(ctx, missing)

	return
}

// Stop calls Stop method of downstream cache
func (c GenNumMiddleware) Stop() {
	c.downstreamCache.Stop()
}

// InjectCacheGenNumber returns a derived context containing the cache gen.
func InjectCacheGenNumber(ctx context.Context, cacheGen string) context.Context {
	return context.WithValue(ctx, interface{}(cacheGenContextKey), cacheGen)
}

// ExtractCacheGenNumber gets the cache gen from the context.
func ExtractCacheGenNumber(ctx context.Context) string {
	cacheGenNumber, ok := ctx.Value(cacheGenContextKey).(string)
	if !ok {
		return ""
	}
	return cacheGenNumber
}

// AddCacheGenNumToCacheKeys adds gen number to keys as suffix
func AddCacheGenNumToCacheKeys(ctx context.Context, keys []string) []string {
	cacheGen := ExtractCacheGenNumber(ctx)
	if cacheGen == "" {
		return keys
	}

	for i := range keys {
		keys[i] = cacheGen + keys[i]
	}

	return keys
}

// RemoveCacheGenNumFromKeys removes suffixed gen number from keys
func RemoveCacheGenNumFromKeys(ctx context.Context, keys []string) []string {
	cacheGen := ExtractCacheGenNumber(ctx)
	if cacheGen == "" {
		return keys
	}

	cacheGenSuffixLen := len(cacheGen) - 1

	for i := range keys {
		keys[i] = keys[i][cacheGenSuffixLen:]
	}

	return keys
}
