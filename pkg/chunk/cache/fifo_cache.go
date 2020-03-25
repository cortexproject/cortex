package cache

import (
	"context"
	"flag"
	"sync"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/util"
)

var (
	cacheEntriesAdded = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "querier",
		Subsystem: "cache",
		Name:      "added_total",
		Help:      "The total number of Put calls on the cache",
	}, []string{"cache"})

	cacheEntriesAddedNew = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "querier",
		Subsystem: "cache",
		Name:      "added_new_total",
		Help:      "The total number of new entries added to the cache",
	}, []string{"cache"})

	cacheEntriesEvicted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "querier",
		Subsystem: "cache",
		Name:      "evicted_total",
		Help:      "The total number of evicted entries",
	}, []string{"cache"})

	cacheEntriesCurrent = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "querier",
		Subsystem: "cache",
		Name:      "entries",
		Help:      "The total number of entries",
	}, []string{"cache"})

	cacheTotalGets = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "querier",
		Subsystem: "cache",
		Name:      "gets_total",
		Help:      "The total number of Get calls",
	}, []string{"cache"})

	cacheTotalMisses = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "querier",
		Subsystem: "cache",
		Name:      "misses_total",
		Help:      "The total number of Get calls that had no valid entry",
	}, []string{"cache"})

	cacheStaleGets = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "querier",
		Subsystem: "cache",
		Name:      "stale_gets_total",
		Help:      "The total number of Get calls that had an entry which expired",
	}, []string{"cache"})

	cacheMemoryBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "querier",
		Subsystem: "cache",
		Name:      "memory_bytes",
		Help:      "The current cache size in bytes",
	}, []string{"cache"})
)

// This FIFO cache implementation supports two eviction methods - based on number of items in the cache, and based on memory usage.
// For the memory-based eviction, set FifoCacheConfig.MaxSizeBytes to a positive integer, indicating upper limit of memory allocated by items in the cache.
// Alternatively, set FifoCacheConfig.MaxSizeItems to a positive integer, indicating maximum number of items in the cache.
// If both parameters are set, the memory-based eviction method takes precedence.

// FifoCacheConfig holds config for the FifoCache.
type FifoCacheConfig struct {
	MaxSizeBytes int           `yaml:"max_size_bytes"`
	MaxSizeItems int           `yaml:"size"`
	Validity     time.Duration `yaml:"validity"`
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *FifoCacheConfig) RegisterFlagsWithPrefix(prefix, description string, f *flag.FlagSet) {
	f.IntVar(&cfg.MaxSizeBytes, prefix+"fifocache.max-size-bytes", 0, description+"Maximum memory size of the cache.")
	f.IntVar(&cfg.MaxSizeItems, prefix+"fifocache.size", 0, description+"The number of entries to cache. Deprecated, use fifocache.max-size-bytes instead.")
	f.DurationVar(&cfg.Validity, prefix+"fifocache.duration", 0, description+"The expiry duration for the cache.")
}

// FifoCache is a simple string -> interface{} cache which uses a fifo slide to
// manage evictions.  O(1) inserts and updates, O(1) gets.
type FifoCache struct {
	lock         sync.RWMutex
	maxSizeItems int
	maxSizeBytes int
	currSize     int
	validity     time.Duration
	entries      map[string]*cacheEntry

	// indexes into entries to identify the most recent and least recent entry.
	first, last *cacheEntry

	entriesAdded    prometheus.Counter
	entriesAddedNew prometheus.Counter
	entriesEvicted  prometheus.Counter
	entriesCurrent  prometheus.Gauge
	totalGets       prometheus.Counter
	totalMisses     prometheus.Counter
	staleGets       prometheus.Counter
	memoryBytes     prometheus.Gauge
}

type cacheEntry struct {
	updated    time.Time
	key        string
	value      interface{}
	prev, next *cacheEntry
}

// NewFifoCache returns a new initialised FifoCache of size.
// TODO(bwplotka): Fix metrics, get them out of globals, separate or allow prefixing.
func NewFifoCache(name string, cfg FifoCacheConfig) *FifoCache {
	util.WarnExperimentalUse("In-memory (FIFO) cache")

	return &FifoCache{
		maxSizeItems: cfg.MaxSizeItems,
		maxSizeBytes: cfg.MaxSizeBytes,
		validity:     cfg.Validity,
		entries:      make(map[string]*cacheEntry),

		// TODO(bwplotka): There might be simple cache.Cache wrapper for those.
		entriesAdded:    cacheEntriesAdded.WithLabelValues(name),
		entriesAddedNew: cacheEntriesAddedNew.WithLabelValues(name),
		entriesEvicted:  cacheEntriesEvicted.WithLabelValues(name),
		entriesCurrent:  cacheEntriesCurrent.WithLabelValues(name),
		totalGets:       cacheTotalGets.WithLabelValues(name),
		totalMisses:     cacheTotalMisses.WithLabelValues(name),
		staleGets:       cacheStaleGets.WithLabelValues(name),
		memoryBytes:     cacheMemoryBytes.WithLabelValues(name),
	}
}

// Fetch implements Cache.
func (c *FifoCache) Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missing []string) {
	found, missing, bufs = make([]string, 0, len(keys)), make([]string, 0, len(keys)), make([][]byte, 0, len(keys))
	for _, key := range keys {
		val, ok := c.Get(ctx, key)
		if !ok {
			missing = append(missing, key)
			continue
		}

		found = append(found, key)
		bufs = append(bufs, val.([]byte))
	}
	return
}

// Store implements Cache.
func (c *FifoCache) Store(ctx context.Context, keys []string, bufs [][]byte) {
	values := make([]interface{}, 0, len(bufs))
	for _, buf := range bufs {
		values = append(values, buf)
	}
	c.Put(ctx, keys, values)
}

// Stop implements Cache.
func (c *FifoCache) Stop() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.entries = nil
	c.currSize = 0
}

// Put stores the value against the key.
func (c *FifoCache) Put(ctx context.Context, keys []string, values []interface{}) {
	c.entriesAdded.Inc()
	if c.maxSizeBytes == 0 && c.maxSizeItems == 0 {
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	for i := range keys {
		c.put(ctx, keys[i], values[i])
	}
}

func (c *FifoCache) addToHead(entry *cacheEntry) {
	if entry == nil {
		return
	}
	entry.prev, entry.next = nil, c.first
	if c.first != nil {
		c.first.prev = entry
	}
	c.first = entry
	if c.last == nil {
		c.last = entry
	}
}

func (c *FifoCache) deleteFromTail() *cacheEntry {
	if c.last == nil {
		return nil
	}
	ret := c.last
	c.last = ret.prev
	if c.last == nil {
		c.first = nil
	} else {
		c.last.next = nil
	}
	return ret
}

func (c *FifoCache) deleteFromList(entry *cacheEntry) {
	if entry == nil {
		return
	}
	if entry.prev == nil {
		c.first = entry.next
	} else {
		entry.prev.next = entry.next
	}
	if entry.next == nil {
		c.last = entry.prev
	} else {
		entry.next.prev = entry.prev
	}
}

func (c *FifoCache) put(ctx context.Context, key string, value interface{}) {
	// See if we already have the entry
	entry, ok := c.entries[key]
	if ok {
		// Remove this entry from the FIFO linked-list.
		c.deleteFromList(entry)
		entrySz := sizeOf(entry)
		c.currSize -= entrySz

		// New entry size
		entrySz += (sizeOfInterface(value) - sizeOfInterface(entry.value))

		if c.maxSizeBytes > 0 && c.currSize+entrySz > c.maxSizeBytes {
			// New entry does not fit in the cache. Delete the entry.
			delete(c.entries, entry.key)
			c.entriesCurrent.Dec()
			c.entriesEvicted.Inc()
			entry = nil
		} else {
			// Insert it at the beginning.
			entry.updated = time.Now()
			entry.value = value
			c.addToHead(entry)
			c.currSize += entrySz
		}
		c.memoryBytes.Set(float64(c.currSize))
		return
	}

	entry = &cacheEntry{
		updated: time.Now(),
		key:     key,
		value:   value,
	}
	entrySz := sizeOf(entry)

	// Check is the entry fits in the cache
	if c.maxSizeBytes > 0 && entrySz > c.maxSizeBytes {
		// Cannot keep this entry in the cache
		return
	}

	c.entriesAddedNew.Inc()

	// Otherwise, see if we need to evict an entry.
	for (c.maxSizeBytes > 0 && c.currSize+entrySz > c.maxSizeBytes) || (c.maxSizeItems > 0 && len(c.entries) >= c.maxSizeItems) {
		c.entriesEvicted.Inc()
		if evicted := c.deleteFromTail(); evicted != nil {
			delete(c.entries, evicted.key)
			c.currSize -= sizeOf(evicted)
			c.entriesCurrent.Dec()
		} else {
			c.entries = nil
			c.currSize = 0
			c.entriesCurrent.Set(float64(0))
		}
	}

	// Finally, no hit and we have space.
	c.addToHead(entry)
	c.entries[key] = entry
	c.currSize += entrySz
	c.memoryBytes.Set(float64(c.currSize))
	c.entriesCurrent.Inc()
}

// Get returns the stored value against the key and when the key was last updated.
func (c *FifoCache) Get(ctx context.Context, key string) (interface{}, bool) {
	c.totalGets.Inc()
	if c.maxSizeBytes == 0 && c.maxSizeItems == 0 {
		return nil, false
	}

	c.lock.RLock()
	defer c.lock.RUnlock()

	entry, ok := c.entries[key]
	if ok {
		if c.validity == 0 || time.Since(entry.updated) < c.validity {
			return entry.value, true
		}

		c.totalMisses.Inc()
		c.staleGets.Inc()
		return nil, false
	}

	c.totalMisses.Inc()
	return nil, false
}

func sizeOf(entry *cacheEntry) int {
	return int(unsafe.Sizeof(*entry)) + // size of cacheEntry
		sizeOfInterface(entry.value) + // size of entry.value
		(2 * sizeOfInterface(entry.key)) + // counting key twice: in the cacheEntry and in the map
		int(unsafe.Sizeof(entry)) // size of *cacheEntry in the map
}

func sizeOfInterface(i interface{}) int {
	switch v := i.(type) {
	case string:
		return len(v)
	case []int8:
		return len(v)
	case []uint8:
		return len(v)
	case []int32:
		return len(v) * 4
	case []uint32:
		return len(v) * 4
	case []float32:
		return len(v) * 4
	case []int64:
		return len(v) * 8
	case []uint64:
		return len(v) * 8
	case []float64:
		return len(v) * 8
	// next 2 cases are machine dependent
	case []int:
		if l := len(v); l > 0 {
			return int(unsafe.Sizeof(v[0])) * l
		}
		return 0
	case []uint:
		if l := len(v); l > 0 {
			return int(unsafe.Sizeof(v[0])) * l
		}
		return 0
	default:
		return int(unsafe.Sizeof(i))
	}
}
