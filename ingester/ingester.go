package ingester

import (
	"fmt"
	"sort"
	"sync"
	"time"

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
	ingesterSubsystem        = "ingester"
	maxConcurrentFlushSeries = 100

	discardReasonLabel = "reason"

	// Reasons to discard samples.
	outOfOrderTimestamp = "timestamp_out_of_order"
	duplicateSample     = "multiple_values_for_timestamp"
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
	cfg                Config
	chunkStore         cortex.Store
	stopLock           sync.RWMutex
	stopped            bool
	quit               chan struct{}
	done               chan struct{}
	flushSeriesLimiter cortex.Semaphore

	userStateLock sync.Mutex
	userState     map[string]*userState

	ingestedSamples    prometheus.Counter
	discardedSamples   *prometheus.CounterVec
	chunkUtilization   prometheus.Histogram
	chunkStoreFailures prometheus.Counter
	queries            prometheus.Counter
	queriedSamples     prometheus.Counter
	memoryChunks       prometheus.Gauge
}

// Config configures an Ingester.
type Config struct {
	FlushCheckPeriod time.Duration
	MaxChunkAge      time.Duration
	RateUpdatePeriod time.Duration
	Ring             *ring.Ring
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

	i := &Ingester{
		cfg:                cfg,
		chunkStore:         chunkStore,
		quit:               make(chan struct{}),
		done:               make(chan struct{}),
		flushSeriesLimiter: cortex.NewSemaphore(maxConcurrentFlushSeries),

		userState: map[string]*userState{},

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
			Help:    "Distribution of stored chunk utilization.",
			Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9},
		}),
		memoryChunks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingester_memory_chunks",
			Help: "The total number of chunks in memory.",
		}),
		chunkStoreFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_chunk_store_failures_total",
			Help: "The total number of errors while storing chunks to the chunk store.",
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

	go i.loop()
	return i, nil
}

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

	close(i.quit)
	<-i.done
}

func (i *Ingester) loop() {
	defer func() {
		i.flushAllUsers(true)
		close(i.done)
		log.Infof("Ingester exited gracefully")
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
	log.Infof("Flushing chunks... (exiting: %v)", immediate)
	defer log.Infof("Done flushing chunks.")

	if i.chunkStore == nil {
		return
	}

	i.userStateLock.Lock()
	userIDs := make([]string, 0, len(i.userState))
	for userID := range i.userState {
		userIDs = append(userIDs, userID)
	}
	i.userStateLock.Unlock()

	var wg sync.WaitGroup
	for _, userID := range userIDs {
		wg.Add(1)
		go func(userID string) {
			i.flushUser(userID, immediate)
			wg.Done()
		}(userID)
	}
	wg.Wait()
}

func (i *Ingester) flushUser(userID string, immediate bool) {
	log.Infof("Flushing user %s...", userID)
	defer log.Infof("Done flushing user %s.", userID)

	i.userStateLock.Lock()
	userState, ok := i.userState[userID]
	i.userStateLock.Unlock()

	// This should happen, right?
	if !ok {
		return
	}

	ctx := user.WithID(context.Background(), userID)
	i.flushAllSeries(ctx, userState, immediate)

	// TODO: this is probably slow, and could be done in a better way.
	i.userStateLock.Lock()
	if userState.fpToSeries.length() == 0 {
		delete(i.userState, userID)
	}
	i.userStateLock.Unlock()
}

func (i *Ingester) flushAllSeries(ctx context.Context, state *userState, immediate bool) {
	var wg sync.WaitGroup
	for pair := range state.fpToSeries.iter() {
		wg.Add(1)
		i.flushSeriesLimiter.Acquire()
		go func(pair fingerprintSeriesPair) {
			if err := i.flushSeries(ctx, state, pair.fp, pair.series, immediate); err != nil {
				log.Errorf("Failed to flush chunks for series: %v", err)
			}
			i.flushSeriesLimiter.Release()
			wg.Done()
		}(pair)
	}
	wg.Wait()
}

func (i *Ingester) flushSeries(ctx context.Context, u *userState, fp model.Fingerprint, series *memorySeries, immediate bool) error {
	u.fpLocker.Lock(fp)

	// Decide what chunks to flush
	if immediate || time.Now().Sub(series.head().FirstTime().Time()) > i.cfg.MaxChunkAge {
		series.headChunkClosed = true
		series.head().MaybePopulateLastTime()
	}
	chunks := series.chunkDescs
	if !series.headChunkClosed {
		chunks = chunks[:len(chunks)-1]
	}
	u.fpLocker.Unlock(fp)
	if len(chunks) == 0 {
		return nil
	}

	// flush the chunks without locking the series
	if err := i.flushChunks(ctx, fp, series.metric, chunks); err != nil {
		i.chunkStoreFailures.Add(float64(len(chunks)))
		return err
	}

	// now remove the chunks
	u.fpLocker.Lock(fp)
	series.chunkDescs = series.chunkDescs[len(chunks):]
	i.memoryChunks.Sub(float64(len(chunks)))
	if len(series.chunkDescs) == 0 {
		u.fpToSeries.del(fp)
		u.index.delete(series.metric, fp)
	}
	u.fpLocker.Unlock(fp)
	return nil
}

func (i *Ingester) flushChunks(ctx context.Context, fp model.Fingerprint, metric model.Metric, chunks []*prom_chunk.Desc) error {
	wireChunks := make([]cortex.Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		buf := make([]byte, prom_chunk.ChunkLen)
		if err := chunk.C.MarshalToBuf(buf); err != nil {
			return err
		}

		i.chunkUtilization.Observe(chunk.C.Utilization())

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
	ch <- i.ingestedSamples.Desc()
	i.discardedSamples.Describe(ch)
	ch <- i.chunkUtilization.Desc()
	ch <- i.chunkStoreFailures.Desc()
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
	ch <- i.ingestedSamples
	i.discardedSamples.Collect(ch)
	ch <- i.chunkUtilization
	ch <- i.chunkStoreFailures
	ch <- i.queries
	ch <- i.queriedSamples
	ch <- i.memoryChunks
}

type invertedIndex struct {
	mtx sync.RWMutex
	idx map[model.LabelName]map[model.LabelValue][]model.Fingerprint // entries are sorted in fp order?
}

func newInvertedIndex() *invertedIndex {
	return &invertedIndex{
		idx: map[model.LabelName]map[model.LabelValue][]model.Fingerprint{},
	}
}

func (i *invertedIndex) add(metric model.Metric, fp model.Fingerprint) {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	for name, value := range metric {
		values, ok := i.idx[name]
		if !ok {
			values = map[model.LabelValue][]model.Fingerprint{}
		}
		fingerprints := values[value]
		j := sort.Search(len(fingerprints), func(i int) bool {
			return fingerprints[i] >= fp
		})
		fingerprints = append(fingerprints, 0)
		copy(fingerprints[j+1:], fingerprints[j:])
		fingerprints[j] = fp
		values[value] = fingerprints
		i.idx[name] = values
	}
}

func (i *invertedIndex) lookup(matchers []*metric.LabelMatcher) []model.Fingerprint {
	if len(matchers) == 0 {
		return nil
	}
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	// intersection is initially nil, which is a special case.
	var intersection []model.Fingerprint
	for _, matcher := range matchers {
		values, ok := i.idx[matcher.Name]
		if !ok {
			return nil
		}
		var toIntersect []model.Fingerprint
		for value, fps := range values {
			if matcher.Match(value) {
				toIntersect = merge(toIntersect, fps)
			}
		}
		intersection = intersect(intersection, toIntersect)
		if len(intersection) == 0 {
			return nil
		}
	}

	return intersection
}

func (i *invertedIndex) lookupLabelValues(name model.LabelName) model.LabelValues {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	values, ok := i.idx[name]
	if !ok {
		return nil
	}
	res := make(model.LabelValues, 0, len(values))
	for val := range values {
		res = append(res, val)
	}
	return res
}

func (i *invertedIndex) delete(metric model.Metric, fp model.Fingerprint) {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	for name, value := range metric {
		values, ok := i.idx[name]
		if !ok {
			continue
		}
		fingerprints, ok := values[value]
		if !ok {
			continue
		}

		j := sort.Search(len(fingerprints), func(i int) bool {
			return fingerprints[i] >= fp
		})
		fingerprints = fingerprints[:j+copy(fingerprints[j:], fingerprints[j+1:])]

		if len(fingerprints) == 0 {
			delete(values, value)
		} else {
			values[value] = fingerprints
		}

		if len(values) == 0 {
			delete(i.idx, name)
		} else {
			i.idx[name] = values
		}
	}
}

// intersect two sorted lists of fingerprints.  Assumes there are no duplicate
// fingerprints within the input lists.
func intersect(a, b []model.Fingerprint) []model.Fingerprint {
	if a == nil {
		return b
	}
	result := []model.Fingerprint{}
	for i, j := 0, 0; i < len(a) && j < len(b); {
		if a[i] == b[j] {
			result = append(result, a[i])
		}
		if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return result
}

// merge two sorted lists of fingerprints.  Assumes there are no duplicate
// fingerprints between or within the input lists.
func merge(a, b []model.Fingerprint) []model.Fingerprint {
	result := make([]model.Fingerprint, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}
	for ; i < len(a); i++ {
		result = append(result, a[i])
	}
	for ; j < len(b); j++ {
		result = append(result, b[j])
	}
	return result
}
