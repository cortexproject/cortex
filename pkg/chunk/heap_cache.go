package chunk

import (
	"container/heap"
	"sync"
	"time"
)

// heapCache is a simple string -> interface{} cache which uses a heap to manage
// evictions.  O(log N) inserts, O(n) get and update.
type heapCache struct {
	lock    sync.RWMutex
	size    int
	entries []cacheEntry
	index   map[string]int
}

type cacheEntry struct {
	updated time.Time
	key     string
	value   interface{}
}

func newHeapCache(size int) *heapCache {
	return &heapCache{
		size:    size,
		entries: make([]cacheEntry, 0, size),
		index:   make(map[string]int, size),
	}
}

func (c *heapCache) Len() int {
	return len(c.entries)
}

func (c *heapCache) Less(i, j int) bool {
	return c.entries[i].updated.Before(c.entries[j].updated)
}

func (c *heapCache) Swap(i, j int) {
	c.entries[i], c.entries[j] = c.entries[j], c.entries[i]
	c.index[c.entries[i].key] = i
	c.index[c.entries[j].key] = j
}

func (c *heapCache) Push(x interface{}) {
	n := len(c.entries)
	entry := x.(cacheEntry)
	c.entries = append(c.entries, entry)
	c.index[entry.key] = n
}

func (c *heapCache) Pop() interface{} {
	n := len(c.entries)
	entry := c.entries[n-1]
	c.entries = c.entries[0 : n-1]
	delete(c.index, entry.key)
	return entry
}

func (c *heapCache) put(key string, value interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// See if we already have the entry
	index, ok := c.index[key]
	if ok {
		c.entries[index].updated = time.Now()
		c.entries[index].value = value
		heap.Fix(c, index)
		return
	}

	// Otherwise, see if we need to evict an entry (heap will update index).
	if len(c.entries) >= c.size {
		heap.Pop(c)
	}

	// Put new entry on the heap (heap will update index).
	heap.Push(c, cacheEntry{
		updated: time.Now(),
		key:     key,
		value:   value,
	})
}

func (c *heapCache) get(key string) (value interface{}, ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	var index int
	index, ok = c.index[key]
	if ok {
		value = c.entries[index].value
	}
	return
}
