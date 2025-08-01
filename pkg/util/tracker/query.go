package tracker

import (
	"container/heap"
	"sync"
	"time"
)

const (
	ttl               = 2 * time.Second
	slidingWindowSize = 3 * time.Second
	maxTrackedQueries = 100
)

type QueryTracker struct {
	heap   *queryHeap
	lookup map[string]*queryItem
	mu     sync.Mutex
}

type queryItem struct {
	requestID  string
	bytesRate  *slidingWindow
	lastUpdate time.Time
	index      int
}

type queryHeap []*queryItem

func (h queryHeap) Len() int           { return len(h) }
func (h queryHeap) Less(i, j int) bool { return h[i].bytesRate.rate() < h[j].bytesRate.rate() }
func (h queryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *queryHeap) Push(x interface{}) {
	item := x.(*queryItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *queryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	item.index = -1
	*h = old[0 : n-1]
	return item
}

func NewQueryTracker() *QueryTracker {
	h := &queryHeap{}
	heap.Init(h)
	tracker := &QueryTracker{
		heap:   h,
		lookup: make(map[string]*queryItem),
	}

	go tracker.loop()
	return tracker
}

func (q *QueryTracker) loop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		q.cleanup()
	}
}

func (q *QueryTracker) cleanup() {
	now := time.Now()
	stale := now.Add(-ttl)

	q.mu.Lock()
	defer q.mu.Unlock()

	var toRemove []*queryItem
	for _, item := range *q.heap {
		if item.lastUpdate.Before(stale) {
			toRemove = append(toRemove, item)
		}
	}

	for _, item := range toRemove {
		heap.Remove(q.heap, item.index)
		delete(q.lookup, item.requestID)
	}
}

func (q *QueryTracker) Add(requestID string, bytes uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	item, exists := q.lookup[requestID]

	if !exists {
		item = &queryItem{
			requestID: requestID,
			bytesRate: newSlidingWindow(slidingWindowSize),
		}
		item.bytesRate.add(bytes)
		item.lastUpdate = now

		if q.heap.Len() < maxTrackedQueries {
			heap.Push(q.heap, item)
			q.lookup[requestID] = item
		} else {
			minItem := (*q.heap)[0]
			if item.bytesRate.rate() > minItem.bytesRate.rate() {
				delete(q.lookup, minItem.requestID)
				heap.Pop(q.heap)
				heap.Push(q.heap, item)
				q.lookup[requestID] = item
			}
		}
	} else {
		item.bytesRate.add(bytes)
		item.lastUpdate = now
		heap.Fix(q.heap, item.index)
	}
}

func (q *QueryTracker) GetWorstQuery() (string, float64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.heap.Len() == 0 {
		return "", 0
	}

	var worstQueryID string
	var worstRate float64

	for _, item := range *q.heap {
		rate := item.bytesRate.rate()
		if rate > worstRate {
			worstRate = rate
			worstQueryID = item.requestID
		}
	}

	return worstQueryID, worstRate
}

type slidingWindow struct {
	buckets    []uint64
	windowSize time.Duration
	lastUpdate time.Time
	currentIdx int
	mu         sync.Mutex
}

func newSlidingWindow(windowSize time.Duration) *slidingWindow {
	seconds := int(windowSize.Seconds())
	return &slidingWindow{
		buckets:    make([]uint64, seconds),
		windowSize: windowSize,
		lastUpdate: time.Now().Truncate(time.Second),
	}
}

func (swr *slidingWindow) add(bytes uint64) {
	swr.mu.Lock()
	defer swr.mu.Unlock()

	now := time.Now().Truncate(time.Second)

	// Calculate how many seconds have passed since last update
	secondsDrift := int(now.Sub(swr.lastUpdate).Seconds())
	if secondsDrift > 0 {
		// Clear old buckets
		for i := 0; i < min(secondsDrift, len(swr.buckets)); i++ {
			nextIdx := (swr.currentIdx + i) % len(swr.buckets)
			swr.buckets[nextIdx] = 0
		}
		// Update current index
		swr.currentIdx = (swr.currentIdx + secondsDrift) % len(swr.buckets)
		swr.lastUpdate = now
	}

	// Add bytes to current bucket
	swr.buckets[swr.currentIdx] += bytes
}

func (swr *slidingWindow) rate() float64 {
	swr.mu.Lock()
	defer swr.mu.Unlock()

	var totalBytes uint64
	for _, bytes := range swr.buckets {
		totalBytes += bytes
	}

	return float64(totalBytes) / swr.windowSize.Seconds()
}
