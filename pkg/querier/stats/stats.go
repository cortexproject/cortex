package stats

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic" //lint:ignore faillint we can't use go.uber.org/atomic with a protobuf struct without wrapping it.
	"time"

	"github.com/weaveworks/common/httpgrpc"
)

type contextKey int

var ctxKey = contextKey(0)

type QueryStats struct {
	Stats
	QueryResponseSeries uint64
	PriorityAssigned    bool
	Priority            int64
	DataSelectMaxTime   int64
	DataSelectMinTime   int64
	SplitInterval       time.Duration
	m                   sync.Mutex

	// Phase tracking fields for timeout classification.
	// Stored as UnixNano int64 for atomic operations.
	queryStart     int64 // nanosecond timestamp when query began
	queueJoinTime  int64 // nanosecond timestamp when request entered scheduler queue
	queueLeaveTime int64 // nanosecond timestamp when request left scheduler queue
}

// ContextWithEmptyStats returns a context with empty stats.
func ContextWithEmptyStats(ctx context.Context) (*QueryStats, context.Context) {
	stats := &QueryStats{}
	ctx = context.WithValue(ctx, ctxKey, stats)
	return stats, ctx
}

// FromContext gets the Stats out of the Context. Returns nil if stats have not
// been initialised in the context.
func FromContext(ctx context.Context) *QueryStats {
	o := ctx.Value(ctxKey)
	if o == nil {
		return nil
	}
	return o.(*QueryStats)
}

// IsEnabled returns whether stats tracking is enabled in the context.
func IsEnabled(ctx context.Context) bool {
	// When query statistics are enabled, the stats object is already initialised
	// within the context, so we can just check it.
	return FromContext(ctx) != nil
}

func (s *QueryStats) Copy() *QueryStats {
	if s == nil {
		return nil
	}

	copied := &QueryStats{}
	copied.Merge(s)
	return copied
}

// AddWallTime adds some time to the counter.
func (s *QueryStats) AddWallTime(t time.Duration) {
	if s == nil {
		return
	}

	atomic.AddInt64((*int64)(&s.WallTime), int64(t))
}

// LoadWallTime returns current wall time.
func (s *QueryStats) LoadWallTime() time.Duration {
	if s == nil {
		return 0
	}

	return time.Duration(atomic.LoadInt64((*int64)(&s.WallTime)))
}

func (s *QueryStats) AddResponseSeries(series uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.QueryResponseSeries, series)
}

func (s *QueryStats) LoadResponseSeries() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.QueryResponseSeries)
}

func (s *QueryStats) AddFetchedSeries(series uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedSeriesCount, series)
}

func (s *QueryStats) AddExtraFields(fieldsVals ...any) {
	if s == nil {
		return
	}

	s.m.Lock()
	defer s.m.Unlock()

	if s.ExtraFields == nil {
		s.ExtraFields = map[string]string{}
	}

	if len(fieldsVals)%2 == 1 {
		fieldsVals = append(fieldsVals, "")
	}

	for i := 0; i < len(fieldsVals); i += 2 {
		if v, ok := fieldsVals[i].(string); ok {
			s.ExtraFields[v] = fmt.Sprintf("%v", fieldsVals[i+1])
		}
	}
}

func (s *QueryStats) LoadExtraFields() []any {
	if s == nil {
		return []any{}
	}

	s.m.Lock()
	defer s.m.Unlock()

	r := make([]any, 0, len(s.ExtraFields))
	for k, v := range s.ExtraFields {
		r = append(r, k, v)
	}

	return r
}

func (s *QueryStats) LoadFetchedSeries() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedSeriesCount)
}

func (s *QueryStats) AddFetchedChunkBytes(bytes uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedChunkBytes, bytes)
}

func (s *QueryStats) LoadFetchedChunkBytes() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedChunkBytes)
}

func (s *QueryStats) AddFetchedDataBytes(bytes uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedDataBytes, bytes)
}

func (s *QueryStats) LoadFetchedDataBytes() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedDataBytes)
}

func (s *QueryStats) AddFetchedSamples(count uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedSamplesCount, count)
}

func (s *QueryStats) LoadFetchedSamples() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedSamplesCount)
}

func (s *QueryStats) AddFetchedChunks(count uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedChunksCount, count)
}

func (s *QueryStats) LoadFetchedChunks() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedChunksCount)
}

// AddQueryStorageWallTime adds some time to the counter.
func (s *QueryStats) AddQueryStorageWallTime(t time.Duration) {
	if s == nil {
		return
	}

	atomic.AddInt64((*int64)(&s.QueryStorageWallTime), int64(t))
}

// LoadQueryStorageWallTime returns current query storage wall time.
func (s *QueryStats) LoadQueryStorageWallTime() time.Duration {
	if s == nil {
		return 0
	}

	return time.Duration(atomic.LoadInt64((*int64)(&s.QueryStorageWallTime)))
}

func (s *QueryStats) AddSplitQueries(count uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.SplitQueries, count)
}

func (s *QueryStats) LoadSplitQueries() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.SplitQueries)
}

func (s *QueryStats) SetPriority(priority int64) {
	if s == nil {
		return
	}

	if !s.PriorityAssigned {
		s.PriorityAssigned = true
	}

	atomic.StoreInt64(&s.Priority, priority)
}

func (s *QueryStats) LoadPriority() (int64, bool) {
	if s == nil {
		return 0, false
	}

	return atomic.LoadInt64(&s.Priority), s.PriorityAssigned
}

func (s *QueryStats) SetDataSelectMaxTime(dataSelectMaxTime int64) {
	if s == nil {
		return
	}

	atomic.StoreInt64(&s.DataSelectMaxTime, dataSelectMaxTime)
}

func (s *QueryStats) LoadDataSelectMaxTime() int64 {
	if s == nil {
		return 0
	}

	return atomic.LoadInt64(&s.DataSelectMaxTime)
}

func (s *QueryStats) SetDataSelectMinTime(dataSelectMinTime int64) {
	if s == nil {
		return
	}

	atomic.StoreInt64(&s.DataSelectMinTime, dataSelectMinTime)
}

func (s *QueryStats) LoadDataSelectMinTime() int64 {
	if s == nil {
		return 0
	}

	return atomic.LoadInt64(&s.DataSelectMinTime)
}

func (s *QueryStats) LoadSplitInterval() time.Duration {
	if s == nil {
		return 0
	}

	return s.SplitInterval
}

// SetQueryStart records when the query began execution.
func (s *QueryStats) SetQueryStart(t time.Time) {
	if s == nil {
		return
	}

	atomic.StoreInt64(&s.queryStart, t.UnixNano())
}

// LoadQueryStart returns the query start time.
func (s *QueryStats) LoadQueryStart() time.Time {
	if s == nil {
		return time.Time{}
	}

	ns := atomic.LoadInt64(&s.queryStart)
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// SetQueueJoinTime records when the request entered the scheduler queue.
func (s *QueryStats) SetQueueJoinTime(t time.Time) {
	if s == nil {
		return
	}

	atomic.StoreInt64(&s.queueJoinTime, t.UnixNano())
}

// LoadQueueJoinTime returns the queue join time.
func (s *QueryStats) LoadQueueJoinTime() time.Time {
	if s == nil {
		return time.Time{}
	}

	ns := atomic.LoadInt64(&s.queueJoinTime)
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// SetQueueLeaveTime records when the request left the scheduler queue.
func (s *QueryStats) SetQueueLeaveTime(t time.Time) {
	if s == nil {
		return
	}

	atomic.StoreInt64(&s.queueLeaveTime, t.UnixNano())
}

// LoadQueueLeaveTime returns the queue leave time.
func (s *QueryStats) LoadQueueLeaveTime() time.Time {
	if s == nil {
		return time.Time{}
	}

	ns := atomic.LoadInt64(&s.queueLeaveTime)
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// updateMaxDuration atomically updates a max duration field if the new value is larger.
func updateMaxDuration(addr *int64, val time.Duration) {
	new := int64(val)
	for {
		old := atomic.LoadInt64(addr)
		if new <= old {
			return
		}
		if atomic.CompareAndSwapInt64(addr, old, new) {
			return
		}
	}
}

// UpdateMaxFetchTime updates the max fetch time if the provided value is larger.
func (s *QueryStats) UpdateMaxFetchTime(t time.Duration) {
	if s == nil {
		return
	}
	updateMaxDuration((*int64)(&s.MaxFetchTime), t)
}

func (s *QueryStats) LoadMaxFetchTime() time.Duration {
	if s == nil {
		return 0
	}
	return time.Duration(atomic.LoadInt64((*int64)(&s.MaxFetchTime)))
}

func (s *QueryStats) UpdateMaxEvalTime(t time.Duration) {
	if s == nil {
		return
	}
	updateMaxDuration((*int64)(&s.MaxEvalTime), t)
}

func (s *QueryStats) LoadMaxEvalTime() time.Duration {
	if s == nil {
		return 0
	}
	return time.Duration(atomic.LoadInt64((*int64)(&s.MaxEvalTime)))
}

func (s *QueryStats) UpdateMaxQueueWaitTime(t time.Duration) {
	if s == nil {
		return
	}
	updateMaxDuration((*int64)(&s.MaxQueueWaitTime), t)
}

func (s *QueryStats) LoadMaxQueueWaitTime() time.Duration {
	if s == nil {
		return 0
	}
	return time.Duration(atomic.LoadInt64((*int64)(&s.MaxQueueWaitTime)))
}

func (s *QueryStats) UpdateMaxTotalTime(t time.Duration) {
	if s == nil {
		return
	}
	updateMaxDuration((*int64)(&s.MaxTotalTime), t)
}

func (s *QueryStats) LoadMaxTotalTime() time.Duration {
	if s == nil {
		return 0
	}
	return time.Duration(atomic.LoadInt64((*int64)(&s.MaxTotalTime)))
}

// ComputeAndStoreTimingBreakdown computes the timing breakdown from phase tracking
// fields and stores them as max values. This should be called after a sub-query
// completes, before stats are sent back to the frontend.
func (s *QueryStats) ComputeAndStoreTimingBreakdown() {
	if s == nil {
		return
	}

	queryStart := s.LoadQueryStart()
	if queryStart.IsZero() {
		return
	}

	fetchTime := s.LoadQueryStorageWallTime()
	totalTime := time.Since(queryStart)
	evalTime := totalTime - fetchTime

	var queueWaitTime time.Duration
	queueJoin := s.LoadQueueJoinTime()
	queueLeave := s.LoadQueueLeaveTime()
	if !queueJoin.IsZero() && !queueLeave.IsZero() {
		queueWaitTime = queueLeave.Sub(queueJoin)
	}

	s.UpdateMaxFetchTime(fetchTime)
	s.UpdateMaxEvalTime(evalTime)
	s.UpdateMaxQueueWaitTime(queueWaitTime)
	s.UpdateMaxTotalTime(totalTime)
}

func (s *QueryStats) AddStoreGatewayTouchedPostings(count uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.StoreGatewayTouchedPostingsCount, count)
}

func (s *QueryStats) LoadStoreGatewayTouchedPostings() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.StoreGatewayTouchedPostingsCount)
}

func (s *QueryStats) AddStoreGatewayTouchedPostingBytes(bytes uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.StoreGatewayTouchedPostingBytes, bytes)
}

func (s *QueryStats) LoadStoreGatewayTouchedPostingBytes() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.StoreGatewayTouchedPostingBytes)
}

func (s *QueryStats) AddScannedSamples(count uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.ScannedSamples, count)
}

func (s *QueryStats) LoadScannedSamples() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.ScannedSamples)
}

func (s *QueryStats) AddPeakSamples(count uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.PeakSamples, count)
}

func (s *QueryStats) SetPeakSamples(count uint64) {
	if s == nil {
		return
	}

	atomic.StoreUint64(&s.PeakSamples, count)
}

func (s *QueryStats) LoadPeakSamples() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.PeakSamples)
}

// Merge the provided Stats into this one.
func (s *QueryStats) Merge(other *QueryStats) {
	if s == nil || other == nil {
		return
	}

	s.AddWallTime(other.LoadWallTime())
	s.AddQueryStorageWallTime(other.LoadQueryStorageWallTime())
	s.AddFetchedSeries(other.LoadFetchedSeries())
	s.AddFetchedChunkBytes(other.LoadFetchedChunkBytes())
	s.AddFetchedDataBytes(other.LoadFetchedDataBytes())
	s.AddFetchedSamples(other.LoadFetchedSamples())
	s.AddFetchedChunks(other.LoadFetchedChunks())
	s.AddStoreGatewayTouchedPostings(other.LoadStoreGatewayTouchedPostings())
	s.AddStoreGatewayTouchedPostingBytes(other.LoadStoreGatewayTouchedPostingBytes())
	s.AddScannedSamples(other.LoadScannedSamples())
	s.SetPeakSamples(max(s.LoadPeakSamples(), other.LoadPeakSamples()))
	s.AddExtraFields(other.LoadExtraFields()...)
	s.UpdateMaxFetchTime(other.LoadMaxFetchTime())
	s.UpdateMaxEvalTime(other.LoadMaxEvalTime())
	s.UpdateMaxQueueWaitTime(other.LoadMaxQueueWaitTime())
	s.UpdateMaxTotalTime(other.LoadMaxTotalTime())
}

func ShouldTrackHTTPGRPCResponse(r *httpgrpc.HTTPResponse) bool {
	// Do no track statistics for requests failed because of a server error.
	return r.Code < 500
}
