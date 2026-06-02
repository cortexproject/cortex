package queryeviction

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gogo/status"
	"google.golang.org/grpc/codes"

	querier_stats "github.com/cortexproject/cortex/pkg/querier/stats"
)

// ErrQueryEvicted is returned when a query is cancelled by the evictor.
type ErrQueryEvicted struct{}

func (e *ErrQueryEvicted) Error() string {
	return status.Error(codes.ResourceExhausted, "resource limit reached").Error()
}

// QueryEntry represents a single running query in the registry.
type QueryEntry struct {
	QueryID      uint64
	Cancel       context.CancelFunc
	Stats        *querier_stats.QueryStats
	QueryExpr    string // PromQL expression for logging
	UserID       string // tenant ID for logging/metrics
	RequestID    string // request ID for correlation
	RegisteredAt time.Time
}

// MetricFunc extracts a comparable weight value from QueryStats.
// Higher values mean "heavier" query.
type MetricFunc func(s *querier_stats.QueryStats) uint64

// QueryRegistry tracks all currently running queries.
type QueryRegistry struct {
	mu      sync.RWMutex
	queries map[uint64]*QueryEntry
	nextID  uint64
	metric  MetricFunc // configurable: default is LoadPeakSamples
}

// NewQueryRegistry creates a registry with the given metric function.
func NewQueryRegistry(metric MetricFunc) *QueryRegistry {
	return &QueryRegistry{
		queries: make(map[uint64]*QueryEntry),
		metric:  metric,
	}
}

// Register adds a running query and returns its unique, monotonically increasing ID.
func (r *QueryRegistry) Register(cancel context.CancelFunc, stats *querier_stats.QueryStats, queryExpr string, userID string, requestID string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextID++
	id := r.nextID

	r.queries[id] = &QueryEntry{
		QueryID:      id,
		Cancel:       cancel,
		Stats:        stats,
		QueryExpr:    queryExpr,
		UserID:       userID,
		RequestID:    requestID,
		RegisteredAt: time.Now(),
	}

	return id
}

// Deregister removes a query from the registry.
// It is a no-op if the ID is not found.
func (r *QueryRegistry) Deregister(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.queries, id)
}

// FindHeaviest returns up to n entries with the highest metric values
// among queries that have been running for at least minAge,
// sorted heaviest first. Returns nil if no eligible queries exist.
func (r *QueryRegistry) FindHeaviest(n int, minAge time.Duration) []*QueryEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()

	// Use a min-heap of size n to find the top-n queries
	h := &weightedHeap{}

	for _, entry := range r.queries {
		if now.Sub(entry.RegisteredAt) < minAge {
			continue
		}
		w := weighted{entry: entry, weight: r.metric(entry.Stats)}

		if h.Len() < n {
			heap.Push(h, w)
		} else if w.weight > (*h)[0].weight {
			(*h)[0] = w
			heap.Fix(h, 0)
		}
	}

	if h.Len() == 0 {
		return nil
	}

	result := make([]*QueryEntry, h.Len())
	for i := len(result) - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(weighted).entry
	}
	return result
}

// weightedHeap is a min-heap of weighted entries (smallest weight at root).
type weightedHeap []weighted

type weighted struct {
	entry  *QueryEntry
	weight uint64
}

func (h weightedHeap) Len() int           { return len(h) }
func (h weightedHeap) Less(i, j int) bool { return h[i].weight < h[j].weight }
func (h weightedHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *weightedHeap) Push(x any)        { *h = append(*h, x.(weighted)) }
func (h *weightedHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// Len returns the number of currently registered queries.
func (r *QueryRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.queries)
}

// ResolveMetricFunc returns the MetricFunc for the given metric name.
// An empty string defaults to "fetched_samples".
func ResolveMetricFunc(metricName string) (MetricFunc, error) {
	switch metricName {
	case "fetched_samples", "":
		return func(s *querier_stats.QueryStats) uint64 {
			return s.LoadFetchedSamples()
		}, nil
	case "fetched_series":
		return func(s *querier_stats.QueryStats) uint64 {
			return s.LoadFetchedSeries()
		}, nil
	case "fetched_chunks":
		return func(s *querier_stats.QueryStats) uint64 {
			return s.LoadFetchedChunks()
		}, nil
	case "fetched_chunk_bytes":
		return func(s *querier_stats.QueryStats) uint64 {
			return s.LoadFetchedChunkBytes()
		}, nil
	default:
		return nil, fmt.Errorf("unsupported eviction metric: %s", metricName)
	}
}
