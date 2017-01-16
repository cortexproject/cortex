package ingester

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/storage/remote"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex"
	cortex_chunk "github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/user"
	"github.com/weaveworks/cortex/util"
)

const (
	ingesterSubsystem  = "ingester"
	discardReasonLabel = "reason"

	// Reasons to discard samples.
	outOfOrderTimestamp = "timestamp_out_of_order"
	duplicateSample     = "multiple_values_for_timestamp"

	// DefaultConcurrentFlush is the number of series to flush concurrently
	DefaultConcurrentFlush = 50
)

var (
	memorySeriesDesc = prometheus.NewDesc(
		"cortex_ingester_memory_series",
		"The current number of series in memory.",
		nil, nil,
	)
	memoryUsersDesc = prometheus.NewDesc(
		"cortex_ingester_memory_users",
		"The current number of users in memory.",
		nil, nil,
	)
	flushQueueLengthDesc = prometheus.NewDesc(
		"cortex_ingester_flush_queue_length",
		"The total number of series pending in the flush queue.",
		nil, nil,
	)

	// ErrOutOfOrderSample is returned if a sample has a timestamp before the latest
	// timestamp in the series it is appended to.
	ErrOutOfOrderSample = fmt.Errorf("sample timestamp out of order")
	// ErrDuplicateSampleForTimestamp is returned if a sample has the same
	// timestamp as the latest sample in the series it is appended to but a
	// different value. (Appending an identical sample is a no-op and does
	// not cause an error.)
	ErrDuplicateSampleForTimestamp = fmt.Errorf("sample with repeated timestamp but different value")
)

// Ingester deals with "in flight" chunks.
// Its like MemorySeriesStorage, but simpler.
type Ingester struct {
	cfg        Config
	chunkStore cortex_chunk.Store
	stopLock   sync.RWMutex
	stopped    bool
	quit       chan struct{}
	done       sync.WaitGroup

	userStateLock sync.Mutex
	userState     map[string]*userState

	// One queue per flush thread.  Fingerprint is used to
	// pick a queue.
	flushQueues []*util.PriorityQueue

	ingestedSamples  prometheus.Counter
	chunkUtilization prometheus.Histogram
	chunkLength      prometheus.Histogram
	chunkAge         prometheus.Histogram
	queries          prometheus.Counter
	queriedSamples   prometheus.Counter
	memoryChunks     prometheus.Gauge
}

// Config configures an Ingester.
type Config struct {
	FlushCheckPeriod  time.Duration
	MaxChunkIdle      time.Duration
	MaxChunkAge       time.Duration
	RateUpdatePeriod  time.Duration
	ConcurrentFlushes int
	GRPCListenPort    int

	Ring *ring.Ring
}

type userState struct {
	userID          string
	fpLocker        *fingerprintLocker
	fpToSeries      *seriesMap
	mapper          *fpMapper
	index           *invertedIndex
	ingestedSamples *ewmaRate
}

type flushOp struct {
	from      model.Time
	userID    string
	fp        model.Fingerprint
	immediate bool
}

func (o *flushOp) Key() string {
	return fmt.Sprintf("%s-%d-%v", o.userID, o.fp, o.immediate)
}

func (o *flushOp) Priority() int64 {
	return -int64(o.from)
}

// New constructs a new Ingester.
func New(cfg Config, chunkStore cortex_chunk.Store) (*Ingester, error) {
	if cfg.FlushCheckPeriod == 0 {
		cfg.FlushCheckPeriod = 1 * time.Minute
	}
	if cfg.MaxChunkIdle == 0 {
		cfg.MaxChunkIdle = 1 * time.Hour
	}
	if cfg.RateUpdatePeriod == 0 {
		cfg.RateUpdatePeriod = 15 * time.Second
	}
	if cfg.ConcurrentFlushes <= 0 {
		cfg.ConcurrentFlushes = DefaultConcurrentFlush
	}

	i := &Ingester{
		cfg:        cfg,
		chunkStore: chunkStore,
		quit:       make(chan struct{}),

		userState:   map[string]*userState{},
		flushQueues: make([]*util.PriorityQueue, cfg.ConcurrentFlushes, cfg.ConcurrentFlushes),

		ingestedSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_ingested_samples_total",
			Help: "The total number of samples ingested.",
		}),
		chunkUtilization: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_chunk_utilization",
			Help:    "Distribution of stored chunk utilization (when stored).",
			Buckets: prometheus.LinearBuckets(0, 0.2, 6),
		}),
		chunkLength: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_chunk_length",
			Help:    "Distribution of stored chunk lengths (when stored).",
			Buckets: prometheus.ExponentialBuckets(10, 2, 8), // biggest bucket is 10*2^(8-1) = 1280
		}),
		chunkAge: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_chunk_age_seconds",
			Help:    "Distribution of chunk ages (when stored).",
			Buckets: prometheus.ExponentialBuckets(60, 2, 10), // biggest bucket is 60*2^(10-1) = 30720 = 8:32 hrs
		}),
		memoryChunks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingester_memory_chunks",
			Help: "The total number of chunks in memory.",
		}),
		queries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_queries_total",
			Help: "The total number of queries the ingester has handled.",
		}),
		queriedSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_queried_samples_total",
			Help: "The total number of samples returned from queries.",
		}),
	}

	i.done.Add(cfg.ConcurrentFlushes)
	for j := 0; j < cfg.ConcurrentFlushes; j++ {
		i.flushQueues[j] = util.NewPriorityQueue()
		go i.flushLoop(j)
	}

	i.done.Add(1)
	go i.loop()
	return i, nil
}

// Ready is used to indicate to k8s when the ingesters are ready for
// the addition / removal of another ingester.
func (i *Ingester) Ready() bool {
	return i.cfg.Ring.Ready()
}

// Push implements cortex.IngesterServer
func (i *Ingester) Push(ctx context.Context, req *remote.WriteRequest) (*cortex.WriteResponse, error) {
	for _, sample := range util.FromWriteRequest(req) {
		if err := i.append(ctx, sample); err != nil {
			return nil, err
		}
	}
	return &cortex.WriteResponse{}, nil
}

func (i *Ingester) append(ctx context.Context, sample *model.Sample) error {
	if err := util.ValidateSample(sample); err != nil {
		userID, _ := user.GetID(ctx) // ignore err, userID will be empty string if err
		log.Errorf("Error validating sample from user '%s': %v", userID, err)
		return nil
	}

	for ln, lv := range sample.Metric {
		if len(lv) == 0 {
			delete(sample.Metric, ln)
		}
	}

	i.stopLock.RLock()
	defer i.stopLock.RUnlock()
	if i.stopped {
		return fmt.Errorf("ingester stopping")
	}

	state, err := i.getStateFor(ctx)
	if err != nil {
		return err
	}

	fp, series, err := state.getOrCreateSeries(sample.Metric)
	if err != nil {
		return err
	}
	defer func() {
		state.fpLocker.Unlock(fp)
	}()

	prevNumChunks := len(series.chunkDescs)
	if err := series.add(model.SamplePair{
		Value:     sample.Value,
		Timestamp: sample.Timestamp,
	}); err != nil {
		return err
	}

	i.memoryChunks.Add(float64(len(series.chunkDescs) - prevNumChunks))
	i.ingestedSamples.Inc()
	state.ingestedSamples.inc()

	return err
}

func (i *Ingester) getStateFor(ctx context.Context) (*userState, error) {
	userID, err := user.GetID(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id")
	}

	i.userStateLock.Lock()
	defer i.userStateLock.Unlock()
	state, ok := i.userState[userID]
	if !ok {
		state = &userState{
			userID:          userID,
			fpToSeries:      newSeriesMap(),
			fpLocker:        newFingerprintLocker(16),
			index:           newInvertedIndex(),
			ingestedSamples: newEWMARate(0.2, i.cfg.RateUpdatePeriod),
		}
		state.mapper = newFPMapper(state.fpToSeries)
		i.userState[userID] = state
	}
	return state, nil
}

func (u *userState) getOrCreateSeries(metric model.Metric) (model.Fingerprint, *memorySeries, error) {
	rawFP := metric.FastFingerprint()
	u.fpLocker.Lock(rawFP)
	fp := u.mapper.mapFP(rawFP, metric)
	if fp != rawFP {
		u.fpLocker.Unlock(rawFP)
		u.fpLocker.Lock(fp)
	}

	series, ok := u.fpToSeries.get(fp)
	if ok {
		return fp, series, nil
	}

	series = newMemorySeries(metric)
	u.fpToSeries.put(fp, series)
	u.index.add(metric, fp)
	return fp, series, nil
}

// Query implements service.IngesterServer
func (i *Ingester) Query(ctx context.Context, req *cortex.QueryRequest) (*cortex.QueryResponse, error) {
	start, end, matchers, err := util.FromQueryRequest(req)
	if err != nil {
		return nil, err
	}

	matrix, err := i.query(ctx, start, end, matchers...)
	if err != nil {
		return nil, err
	}

	return util.ToQueryResponse(matrix), nil
}

func (i *Ingester) query(ctx context.Context, from, through model.Time, matchers ...*metric.LabelMatcher) (model.Matrix, error) {
	i.queries.Inc()

	state, err := i.getStateFor(ctx)
	if err != nil {
		return nil, err
	}

	fps := state.index.lookup(matchers)

	// fps is sorted, lock them in order to prevent deadlocks
	queriedSamples := 0
	result := model.Matrix{}
	for _, fp := range fps {
		state.fpLocker.Lock(fp)
		series, ok := state.fpToSeries.get(fp)
		if !ok {
			state.fpLocker.Unlock(fp)
			continue
		}

		values, err := series.samplesForRange(from, through)
		state.fpLocker.Unlock(fp)
		if err != nil {
			return nil, err
		}

		result = append(result, &model.SampleStream{
			Metric: series.metric,
			Values: values,
		})
		queriedSamples += len(values)
	}

	i.queriedSamples.Add(float64(queriedSamples))

	return result, nil
}

// LabelValues returns all label values that are associated with a given label name.
func (i *Ingester) LabelValues(ctx context.Context, req *cortex.LabelValuesRequest) (*cortex.LabelValuesResponse, error) {
	state, err := i.getStateFor(ctx)
	if err != nil {
		return nil, err
	}

	resp := &cortex.LabelValuesResponse{}
	for _, v := range state.index.lookupLabelValues(model.LabelName(req.LabelName)) {
		resp.LabelValues = append(resp.LabelValues, string(v))
	}

	return resp, nil
}

// MetricsForLabelMatchers returns all the metrics which match a set of matchers.
func (i *Ingester) MetricsForLabelMatchers(ctx context.Context, req *cortex.MetricsForLabelMatchersRequest) (*cortex.MetricsForLabelMatchersResponse, error) {
	state, err := i.getStateFor(ctx)
	if err != nil {
		return nil, err
	}

	// TODO Right now we ignore start and end.
	_, _, matchersSet, err := util.FromMetricsForLabelMatchersRequest(req)
	if err != nil {
		return nil, err
	}

	fps := map[model.Fingerprint]struct{}{}
	for _, matchers := range matchersSet {
		for _, fp := range state.index.lookup(matchers) {
			fps[fp] = struct{}{}
		}
	}

	result := []model.Metric{}
	for fp := range fps {
		state.fpLocker.Lock(fp)
		series, ok := state.fpToSeries.get(fp)
		if ok {
			result = append(result, series.metric)
		}
		state.fpLocker.Unlock(fp)
	}

	return util.ToMetricsForLabelMatchersResponse(result), nil
}

// UserStats returns ingestion statistics for the current user.
func (i *Ingester) UserStats(ctx context.Context, req *cortex.UserStatsRequest) (*cortex.UserStatsResponse, error) {
	state, err := i.getStateFor(ctx)
	if err != nil {
		return nil, err
	}
	return &cortex.UserStatsResponse{
		IngestionRate: state.ingestedSamples.rate(),
		NumSeries:     uint64(state.fpToSeries.length()),
	}, nil
}

// Stop stops the Ingester.
func (i *Ingester) Stop() {
	i.stopLock.Lock()
	i.stopped = true
	i.stopLock.Unlock()

	// Closing i.quit triggers i.loop() to exit; i.loop() exiting
	// will trigger i.flushLoop()s to exit.
	close(i.quit)

	i.done.Wait()
}

func (i *Ingester) loop() {
	defer func() {
		i.sweepUsers(true)

		// We close flush queue here to ensure the flushLoops pick
		// up all the flushes triggered by the last run
		for _, flushQueue := range i.flushQueues {
			flushQueue.Close()
		}

		log.Infof("Ingester.loop() exited gracefully")
		i.done.Done()
	}()

	flushTick := time.Tick(i.cfg.FlushCheckPeriod)
	rateUpdateTick := time.Tick(i.cfg.RateUpdatePeriod)
	for {
		select {
		case <-flushTick:
			i.sweepUsers(false)
		case <-rateUpdateTick:
			i.updateRates()
		case <-i.quit:
			return
		}
	}
}

// sweepUsers periodically schedules series for flushing and garbage collects users with no series
func (i *Ingester) sweepUsers(immediate bool) {
	if i.chunkStore == nil {
		return
	}

	i.userStateLock.Lock()
	userState := make(map[string]*userState, len(i.userState))
	for id, state := range i.userState {
		userState[id] = state
	}
	i.userStateLock.Unlock()

	for id, state := range userState {
		for pair := range state.fpToSeries.iter() {
			state.fpLocker.Lock(pair.fp)
			i.sweepSeries(id, pair.fp, pair.series, immediate)
			state.fpLocker.Unlock(pair.fp)
		}
	}

	i.userStateLock.Lock()
	for id, state := range userState {
		if state.fpToSeries.length() == 0 {
			delete(i.userState, id)
		}
	}
	i.userStateLock.Unlock()
}

// sweepSeries schedules a series for flushing based on a set of criteria
//
// NB we don't close the head chunk here, as the series could wait in the queue
// for some time, and we want to encourage chunks to be as full as possible.
func (i *Ingester) sweepSeries(userID string, fp model.Fingerprint, series *memorySeries, immediate bool) {
	if len(series.chunkDescs) <= 0 {
		return
	}

	firstTime := series.firstTime()
	flush := i.shouldFlushSeries(series, immediate)

	if flush {
		flushQueueIndex := int(uint64(fp) % uint64(i.cfg.ConcurrentFlushes))
		i.flushQueues[flushQueueIndex].Enqueue(&flushOp{firstTime, userID, fp, immediate})
	}
}

func (i *Ingester) shouldFlushSeries(series *memorySeries, immediate bool) bool {
	// Series should be scheduled for flushing if they have more than one chunk
	if immediate || len(series.chunkDescs) > 1 {
		return true
	}

	// Or if the only existing chunk need flushing
	if len(series.chunkDescs) > 0 {
		return i.shouldFlushChunk(series.chunkDescs[0])
	}

	return false
}

func (i *Ingester) shouldFlushChunk(c *desc) bool {
	// Chunks should be flushed if their oldest entry is older than MaxChunkAge
	if model.Now().Sub(c.FirstTime) > i.cfg.MaxChunkAge {
		return true
	}

	// Chunk should be flushed if their last entry is older then MaxChunkIdle
	if model.Now().Sub(c.LastTime) > i.cfg.MaxChunkIdle {
		return true
	}

	return false
}

func (i *Ingester) flushLoop(j int) {
	defer func() {
		log.Info("Ingester.flushLoop() exited")
		i.done.Done()
	}()

	for {
		o := i.flushQueues[j].Dequeue()
		if o == nil {
			return
		}
		op := o.(*flushOp)

		if err := i.flushUserSeries(op.userID, op.fp, op.immediate); err != nil {
			log.Errorf("Failed to flush user: %v", err)
		}
	}
}

func (i *Ingester) flushUserSeries(userID string, fp model.Fingerprint, immediate bool) error {
	i.userStateLock.Lock()
	userState, ok := i.userState[userID]
	i.userStateLock.Unlock()
	if !ok {
		return nil
	}

	series, ok := userState.fpToSeries.get(fp)
	if !ok {
		return nil
	}

	userState.fpLocker.Lock(fp)
	if !i.shouldFlushSeries(series, immediate) {
		userState.fpLocker.Unlock(fp)
		return nil
	}

	// Assume we're going to flush everything, and maybe don't flush the head chunk if it doesn't need it.
	chunks := series.chunkDescs
	if immediate || (len(chunks) > 0 && i.shouldFlushChunk(series.head())) {
		series.closeHead()
	} else {
		chunks = chunks[:len(chunks)-1]
	}
	userState.fpLocker.Unlock(fp)

	if len(chunks) == 0 {
		return nil
	}

	// flush the chunks without locking the series, as we don't want to hold the series lock for the duration of the dynamo/s3 rpcs.
	ctx := user.WithID(context.Background(), userID)
	err := i.flushChunks(ctx, fp, series.metric, chunks)
	if err != nil {
		return err
	}

	// now remove the chunks
	userState.fpLocker.Lock(fp)
	series.chunkDescs = series.chunkDescs[len(chunks):]
	i.memoryChunks.Sub(float64(len(chunks)))
	if len(series.chunkDescs) == 0 {
		userState.fpToSeries.del(fp)
		userState.index.delete(series.metric, fp)
	}
	userState.fpLocker.Unlock(fp)
	return nil
}

func (i *Ingester) flushChunks(ctx context.Context, fp model.Fingerprint, metric model.Metric, chunkDescs []*desc) error {
	wireChunks := make([]cortex_chunk.Chunk, 0, len(chunkDescs))
	for _, chunkDesc := range chunkDescs {
		i.chunkUtilization.Observe(chunkDesc.C.Utilization())
		i.chunkLength.Observe(float64(chunkDesc.C.Len()))
		i.chunkAge.Observe(model.Now().Sub(chunkDesc.FirstTime).Seconds())
		wireChunks = append(wireChunks, cortex_chunk.NewChunk(fp, metric, chunkDesc.C, chunkDesc.FirstTime, chunkDesc.LastTime))
	}
	return i.chunkStore.Put(ctx, wireChunks)
}

func (i *Ingester) updateRates() {
	i.userStateLock.Lock()
	defer i.userStateLock.Unlock()

	for _, u := range i.userState {
		u.ingestedSamples.tick()
	}
}

// Describe implements prometheus.Collector.
func (i *Ingester) Describe(ch chan<- *prometheus.Desc) {
	ch <- memorySeriesDesc
	ch <- memoryUsersDesc
	ch <- flushQueueLengthDesc
	ch <- i.ingestedSamples.Desc()
	ch <- i.chunkUtilization.Desc()
	ch <- i.chunkLength.Desc()
	ch <- i.chunkAge.Desc()
	ch <- i.queries.Desc()
	ch <- i.queriedSamples.Desc()
	ch <- i.memoryChunks.Desc()
}

// Collect implements prometheus.Collector.
func (i *Ingester) Collect(ch chan<- prometheus.Metric) {
	i.userStateLock.Lock()
	numUsers := len(i.userState)
	numSeries := 0
	for _, state := range i.userState {
		numSeries += state.fpToSeries.length()
	}
	i.userStateLock.Unlock()

	ch <- prometheus.MustNewConstMetric(
		memorySeriesDesc,
		prometheus.GaugeValue,
		float64(numSeries),
	)
	ch <- prometheus.MustNewConstMetric(
		memoryUsersDesc,
		prometheus.GaugeValue,
		float64(numUsers),
	)

	flushQueueLength := 0
	for _, flushQueue := range i.flushQueues {
		flushQueueLength += flushQueue.Length()
	}
	ch <- prometheus.MustNewConstMetric(
		flushQueueLengthDesc,
		prometheus.GaugeValue,
		float64(flushQueueLength),
	)
	ch <- i.ingestedSamples
	ch <- i.chunkUtilization
	ch <- i.chunkLength
	ch <- i.chunkAge
	ch <- i.queries
	ch <- i.queriedSamples
	ch <- i.memoryChunks
}
