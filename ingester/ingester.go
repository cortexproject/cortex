package ingester

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	prom_chunk "github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/metric"
	"golang.org/x/net/context"

	cortex "github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/user"
)

const (
	ingesterSubsystem  = "ingester"
	discardReasonLabel = "reason"

	// Reasons to discard samples.
	outOfOrderTimestamp = "timestamp_out_of_order"
	duplicateSample     = "multiple_values_for_timestamp"

	// For chunk flush errors
	errorReasonLabel = "error"
	otherError       = "other"

	// Backoff for flush
	minBackoff = 100 * time.Millisecond
	maxBackoff = 1 * time.Second

	provisionedThroughputExceededException = "ProvisionedThroughputExceededException"
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
	chunkStore cortex.Store
	stopLock   sync.RWMutex
	stopped    bool
	quit       chan struct{}
	done       sync.WaitGroup

	userStateLock sync.Mutex
	userState     map[string]*userState

	// One queue per flush thread.  Fingerprint is used to
	// pick a queue.
	flushQueues []*priorityQueue

	ingestedSamples    prometheus.Counter
	discardedSamples   *prometheus.CounterVec
	chunkUtilization   prometheus.Histogram
	chunkLength        prometheus.Histogram
	chunkAge           prometheus.Histogram
	chunkStoreFailures *prometheus.CounterVec
	queries            prometheus.Counter
	queriedSamples     prometheus.Counter
	memoryChunks       prometheus.Gauge
}

// Config configures an Ingester.
type Config struct {
	FlushCheckPeriod  time.Duration
	MaxChunkAge       time.Duration
	RateUpdatePeriod  time.Duration
	ConcurrentFlushes int

	Ring *ring.Ring
}

// UserStats models ingestion statistics for one user.
type UserStats struct {
	IngestionRate float64 `json:"ingestionRate"`
	NumSeries     uint64  `json:"numSeries"`
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
func New(cfg Config, chunkStore cortex.Store) (*Ingester, error) {
	if cfg.FlushCheckPeriod == 0 {
		cfg.FlushCheckPeriod = 1 * time.Minute
	}
	if cfg.MaxChunkAge == 0 {
		cfg.MaxChunkAge = 10 * time.Minute
	}
	if cfg.RateUpdatePeriod == 0 {
		cfg.RateUpdatePeriod = 15 * time.Second
	}
	if cfg.ConcurrentFlushes <= 0 {
		cfg.ConcurrentFlushes = 25
	}

	i := &Ingester{
		cfg:        cfg,
		chunkStore: chunkStore,
		quit:       make(chan struct{}),

		userState:   map[string]*userState{},
		flushQueues: make([]*priorityQueue, cfg.ConcurrentFlushes, cfg.ConcurrentFlushes),

		ingestedSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_ingested_samples_total",
			Help: "The total number of samples ingested.",
		}),
		discardedSamples: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "cortex_ingester_out_of_order_samples_total",
				Help: "The total number of samples that were discarded because their timestamps were at or before the last received sample for a series.",
			},
			[]string{discardReasonLabel},
		),
		chunkUtilization: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_chunk_utilization",
			Help:    "Distribution of stored chunk utilization (when stored).",
			Buckets: prometheus.LinearBuckets(0, 0.2, 6),
		}),
		chunkLength: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_chunk_length",
			Help:    "Distribution of stored chunk lengths (when stored).",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		}),
		chunkAge: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_chunk_age_seconds",
			Help:    "Distribution of chunk ages (when stored).",
			Buckets: prometheus.ExponentialBuckets(60, 2, 9),
		}),
		memoryChunks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingester_memory_chunks",
			Help: "The total number of chunks in memory.",
		}),
		chunkStoreFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "cortex_ingester_chunk_store_failures_total",
				Help: "The total number of errors while storing chunks to the chunk store.",
			},
			[]string{errorReasonLabel},
		),
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
		i.flushQueues[j] = newPriorityQueue()
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

// NeedsThrottling implements storage.SampleAppender.
func (*Ingester) NeedsThrottling(_ context.Context) bool {
	return false
}

// Append implements storage.SampleAppender.
func (i *Ingester) Append(ctx context.Context, samples []*model.Sample) error {
	for _, sample := range samples {
		if err := i.append(ctx, sample); err != nil {
			return err
		}
	}
	return nil
}

func (i *Ingester) append(ctx context.Context, sample *model.Sample) error {
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

	if sample.Timestamp == series.lastTime {
		// Don't report "no-op appends", i.e. where timestamp and sample
		// value are the same as for the last append, as they are a
		// common occurrence when using client-side timestamps
		// (e.g. Pushgateway or federation).
		if sample.Timestamp == series.lastTime &&
			series.lastSampleValueSet &&
			sample.Value.Equal(series.lastSampleValue) {
			return nil
		}
		i.discardedSamples.WithLabelValues(duplicateSample).Inc()
		return ErrDuplicateSampleForTimestamp // Caused by the caller.
	}
	if sample.Timestamp < series.lastTime {
		i.discardedSamples.WithLabelValues(outOfOrderTimestamp).Inc()
		return ErrOutOfOrderSample // Caused by the caller.
	}
	prevNumChunks := len(series.chunkDescs)
	_, err = series.add(model.SamplePair{
		Value:     sample.Value,
		Timestamp: sample.Timestamp,
	})
	i.memoryChunks.Add(float64(len(series.chunkDescs) - prevNumChunks))

	if err == nil {
		// TODO: Track append failures too (unlikely to happen).
		i.ingestedSamples.Inc()
		state.ingestedSamples.inc()
	}
	return err
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

// Query implements cortex.Querier.
func (i *Ingester) Query(ctx context.Context, from, through model.Time, matchers ...*metric.LabelMatcher) (model.Matrix, error) {
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

		values, err := samplesForRange(series, from, through)
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

func samplesForRange(s *memorySeries, from, through model.Time) ([]model.SamplePair, error) {
	// Find first chunk with start time after "from".
	fromIdx := sort.Search(len(s.chunkDescs), func(i int) bool {
		return s.chunkDescs[i].FirstTime().After(from)
	})
	// Find first chunk with start time after "through".
	throughIdx := sort.Search(len(s.chunkDescs), func(i int) bool {
		return s.chunkDescs[i].FirstTime().After(through)
	})
	if fromIdx == len(s.chunkDescs) {
		// Even the last chunk starts before "from". Find out if the
		// series ends before "from" and we don't need to do anything.
		lt, err := s.chunkDescs[len(s.chunkDescs)-1].LastTime()
		if err != nil {
			return nil, err
		}
		if lt.Before(from) {
			return nil, nil
		}
	}
	if fromIdx > 0 {
		fromIdx--
	}
	if throughIdx == len(s.chunkDescs) {
		throughIdx--
	}
	var values []model.SamplePair
	in := metric.Interval{
		OldestInclusive: from,
		NewestInclusive: through,
	}
	for idx := fromIdx; idx <= throughIdx; idx++ {
		cd := s.chunkDescs[idx]
		chValues, err := prom_chunk.RangeValues(cd.C.NewIterator(), in)
		if err != nil {
			return nil, err
		}
		values = append(values, chValues...)
	}
	return values, nil
}

// LabelValuesForLabelName returns all label values that are associated with a given label name.
func (i *Ingester) LabelValuesForLabelName(ctx context.Context, name model.LabelName) (model.LabelValues, error) {
	state, err := i.getStateFor(ctx)
	if err != nil {
		return nil, err
	}

	return state.index.lookupLabelValues(name), nil
}

// UserStats returns ingestion statistics for the current user.
func (i *Ingester) UserStats(ctx context.Context) (*UserStats, error) {
	state, err := i.getStateFor(ctx)
	if err != nil {
		return nil, err
	}
	return &UserStats{
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
		i.flushAllUsers(true)

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
			i.flushAllUsers(false)
		case <-rateUpdateTick:
			i.updateRates()
		case <-i.quit:
			return
		}
	}
}

func (i *Ingester) flushAllUsers(immediate bool) {
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
		i.flushUser(id, state, immediate)
	}
}

func (i *Ingester) flushUser(userID string, userState *userState, immediate bool) {
	for pair := range userState.fpToSeries.iter() {
		i.flushSeries(userState, pair.fp, pair.series, immediate)
	}

	// TODO: this is probably slow, and could be done in a better way.
	i.userStateLock.Lock()
	if userState.fpToSeries.length() == 0 {
		delete(i.userState, userID)
	}
	i.userStateLock.Unlock()
}

func (i *Ingester) flushSeries(u *userState, fp model.Fingerprint, series *memorySeries, immediate bool) {
	// Enqueue this series flushing if the oldest chunk is older than the threshold

	u.fpLocker.Lock(fp)
	if len(series.chunkDescs) <= 0 {
		u.fpLocker.Unlock(fp)
		return
	}

	firstTime := series.chunkDescs[0].FirstTime()
	flush := immediate || len(series.chunkDescs) > 1 || model.Now().Sub(firstTime) > i.cfg.MaxChunkAge
	u.fpLocker.Unlock(fp)

	if flush {
		flushQueueIndex := int(uint64(fp) % uint64(i.cfg.ConcurrentFlushes))
		i.flushQueues[flushQueueIndex].Enqueue(&flushOp{firstTime, u.userID, fp, immediate})
	}
}

func (i *Ingester) flushLoop(j int) {
	backoff := minBackoff

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

		// get the user
		i.userStateLock.Lock()
		userState, ok := i.userState[op.userID]
		i.userStateLock.Unlock()
		if !ok {
			continue
		}
		ctx := user.WithID(context.Background(), op.userID)

		// Decide what chunks to flush
		series, ok := userState.fpToSeries.get(op.fp)
		if !ok {
			continue
		}

		userState.fpLocker.Lock(op.fp)

		// Assume we're going to flush everything
		chunks := series.chunkDescs

		// If the head chunk is old enough, close it
		if op.immediate || model.Now().Sub(series.head().FirstTime()) > i.cfg.MaxChunkAge {
			series.closeHead()
		} else {
			chunks = chunks[:len(chunks)-1]
		}
		userState.fpLocker.Unlock(op.fp)

		if len(chunks) == 0 {
			continue
		}

		// flush the chunks without locking the series
		err := i.flushChunks(ctx, op.fp, series.metric, chunks)
		if err != nil {
			log.Errorf("Failed to flush chunks: %v", err)

			if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == provisionedThroughputExceededException {
				i.chunkStoreFailures.WithLabelValues(awsErr.Code()).Add(float64(len(chunks)))
				time.Sleep(backoff)
				backoff = backoff * 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			} else {
				i.chunkStoreFailures.WithLabelValues(otherError).Add(float64(len(chunks)))
			}

			continue
		}

		backoff = minBackoff

		// now remove the chunks
		userState.fpLocker.Lock(op.fp)
		series.chunkDescs = series.chunkDescs[len(chunks):]
		i.memoryChunks.Sub(float64(len(chunks)))
		if len(series.chunkDescs) == 0 {
			userState.fpToSeries.del(op.fp)
			userState.index.delete(series.metric, op.fp)
		}
		userState.fpLocker.Unlock(op.fp)
	}
}

func (i *Ingester) flushChunks(ctx context.Context, fp model.Fingerprint, metric model.Metric, chunks []*prom_chunk.Desc) error {
	wireChunks := make([]cortex.Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		buf := make([]byte, prom_chunk.ChunkLen)
		if err := chunk.C.MarshalToBuf(buf); err != nil {
			return err
		}

		i.chunkUtilization.Observe(chunk.C.Utilization())
		i.chunkLength.Observe(chunk.C.Len())
		i.chunkAge.Observe(model.Now().Sub(chunk.ChunkFirstTime).Seconds())

		wireChunks = append(wireChunks, cortex.Chunk{
			ID:      fmt.Sprintf("%d:%d:%d", fp, chunk.ChunkFirstTime, chunk.ChunkLastTime),
			From:    chunk.ChunkFirstTime,
			Through: chunk.ChunkLastTime,
			Metric:  metric,
			Data:    buf,
		})
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
	i.discardedSamples.Describe(ch)
	ch <- i.chunkUtilization.Desc()
	ch <- i.chunkLength.Desc()
	ch <- i.chunkAge.Desc()
	i.chunkStoreFailures.Describe(ch)
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
	i.discardedSamples.Collect(ch)
	ch <- i.chunkUtilization
	ch <- i.chunkLength
	ch <- i.chunkAge
	i.chunkStoreFailures.Collect(ch)
	ch <- i.queries
	ch <- i.queriedSamples
	ch <- i.memoryChunks
}
