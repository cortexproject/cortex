package cache

import (
	"context"
)

// GenMiddleware adds gen number to keys from context. Expected size of gen numbers is upto 2 digits.
// If we start seeing problems with keys exceeding length limit, we need to look into resetting gen numbers
type GenMiddleware struct {
	downstreamCache Cache
}

// NewCacheGenMiddleware creates a new GenMiddleware
func NewCacheGenMiddleware(downstreamCache Cache) Cache {
	return &GenMiddleware{downstreamCache}
}

// Store adds cache gen number to keys before calling Store method of downstream cache
func (c GenMiddleware) Store(ctx context.Context, keys []string, buf [][]byte) {
	keys = AddCacheGenToCacheKeys(ctx, keys)
	c.downstreamCache.Store(ctx, keys, buf)
}

// Fetch adds cache gen number to keys before calling Fetch method of downstream cache
// It also removes gen number before responding back with found and missing keys to make sure consumer of response gets to see same keys
func (c GenMiddleware) Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missing []string) {
	keys = AddCacheGenToCacheKeys(ctx, keys)

	found, bufs, missing = c.downstreamCache.Fetch(ctx, keys)

	found = RemoveCacheGenFromKeys(ctx, found)
	missing = RemoveCacheGenFromKeys(ctx, missing)

	return
}

// Stop calls Stop method of downstream cache
func (c GenMiddleware) Stop() {
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

// AddCacheGenToCacheKeys adds gen number to keys as suffix
func AddCacheGenToCacheKeys(ctx context.Context, keys []string) []string {
	cacheGen := ExtractCacheGenNumber(ctx)
	if cacheGen == "" {
		return keys
	}

	for i := range keys {
		keys[i] = cacheGen + keys[i]
	}

	return keys
}

// RemoveCacheGenFromKeys removes suffixed gen number from keys
func RemoveCacheGenFromKeys(ctx context.Context, keys []string) []string {
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
