package tsdb

import (
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	// DefaultRefCacheTTL is the default RefCache purge TTL. We use a reasonable
	// value that should cover most use cases. The cache would be ineffective if
	// the scrape interval of a series is greater than this TTL.
	DefaultRefCacheTTL = int64(5 * time.Minute)

	refCacheStripes = 128
)

// RefCache is a single-tenant cache mapping a labels set with the reference
// ID in TSDB, in order to be able to append samples to the TSDB head without having
// to copy write request series labels each time (because the memory buffers used to
// unmarshal the write request is reused).
type RefCache struct {
	// The cache is splitted into stripes, each one with a dedicated lock, in
	// order to reduce lock contention.
	stripes map[uint8]*refCacheStripe
}

// refCacheStripe holds a subset of the series references for a single tenant.
type refCacheStripe struct {
	refsMu sync.Mutex
	refs   map[model.Fingerprint][]*refCacheEntry
}

// refCacheEntry holds a single series reference.
type refCacheEntry struct {
	lbs       labels.Labels
	ref       uint64
	touchedAt int64
}

// NewRefCache makes a new RefCache.
func NewRefCache() *RefCache {
	c := &RefCache{
		stripes: make(map[uint8]*refCacheStripe, refCacheStripes),
	}

	// Stripes are pre-allocated so that we only read on them and no lock is required.
	for i := uint8(0); i < refCacheStripes; i++ {
		c.stripes[i] = &refCacheStripe{
			refs: map[model.Fingerprint][]*refCacheEntry{},
		}
	}

	return c
}

// GetRef returns the cached series reference, and guarantees the input labels set
// is NOT retained.
func (c *RefCache) GetRef(now int64, series []labels.Label) (uint64, bool) {
	fp := client.FastFingerprint(client.FromLabelsToLabelAdapters(series))

	// Get the related stripe
	stripeID := uint8(util.HashFP(fp) % refCacheStripes)
	stripe := c.stripes[stripeID]

	// Look for series within the stripe
	stripe.refsMu.Lock()

	entries, ok := stripe.refs[fp]
	if !ok {
		stripe.refsMu.Unlock()
		return 0, false
	}

	for _, entry := range entries {
		if labels.Equal(entry.lbs, series) {
			// Get the reference and touch the timestamp before releasing the lock
			ref := entry.ref
			entry.touchedAt = now
			stripe.refsMu.Unlock()

			return ref, true
		}
	}

	stripe.refsMu.Unlock()
	return 0, false
}

// SetRef sets/updates the cached series reference. The input labels set IS retained.
func (c *RefCache) SetRef(now int64, series []labels.Label, ref uint64) {
	fp := client.FastFingerprint(client.FromLabelsToLabelAdapters(series))

	// Get the related stripe
	stripeID := uint8(util.HashFP(fp) % refCacheStripes)
	stripe := c.stripes[stripeID]

	stripe.refsMu.Lock()

	// Check if already exists within the entries.
	entries := stripe.refs[fp]
	if entries != nil {
		for _, entry := range entries {
			if !labels.Equal(entry.lbs, series) {
				continue
			}

			entry.ref = ref
			entry.touchedAt = now
			stripe.refsMu.Unlock()
			return
		}
	}

	// The entry doesn't exist, so we have to add a new one.
	stripe.refs[fp] = append(stripe.refs[fp], &refCacheEntry{lbs: series, ref: ref, touchedAt: now})
	stripe.refsMu.Unlock()
}

// Purge removes expired entries from the cache. This function should be called
// periodically to avoid memory leaks.
func (c *RefCache) Purge(now int64, ttl int64) {
	for s := uint8(0); s < refCacheStripes; s++ {
		c.purgeStripe(now, ttl, c.stripes[s])
	}
}

func (c *RefCache) purgeStripe(now int64, ttl int64, stripe *refCacheStripe) {
	stripe.refsMu.Lock()
	defer stripe.refsMu.Unlock()

	for fp, entries := range stripe.refs {
		// Since we do expect very few fingerprint collisions, we
		// have an optimized implementation for the common case.
		if len(entries) == 1 {
			if entries[0].touchedAt < now-ttl {
				delete(stripe.refs, fp)
			}

			continue
		}

		// We have more entries, which means there's a collision,
		// so we have to iterate over the entries.
		for i := 0; i < len(entries); {
			if entries[i].touchedAt < now-ttl {
				entries = append(entries[:i], entries[i+1:]...)
			} else {
				i++
			}
		}

		// Either update or delete the entries in the map
		if len(entries) == 0 {
			delete(stripe.refs, fp)
		} else {
			stripe.refs[fp] = entries
		}
	}
}
