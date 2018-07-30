package ingester

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	// Needed for gRPC compatibility.
	old_ctx "golang.org/x/net/context"
	"google.golang.org/grpc/health/grpc_health_v1"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"

	"github.com/weaveworks/common/httpgrpc"
	cortex_chunk "github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/ring"
	"github.com/weaveworks/cortex/pkg/util"
	"github.com/weaveworks/cortex/pkg/util/validation"
)

const (
	ingesterSubsystem = "ingester"

	// Reasons to discard samples.
	outOfOrderTimestamp = "timestamp_out_of_order"
	duplicateSample     = "multiple_values_for_timestamp"

	// DefaultConcurrentFlush is the number of series to flush concurrently
	DefaultConcurrentFlush = 50
	// DefaultMaxSeriesPerUser is the maximum number of series allowed per user.
	DefaultMaxSeriesPerUser = 5000000
	// DefaultMaxSeriesPerMetric is the maximum number of series in one metric (of a single user).
	DefaultMaxSeriesPerMetric = 50000
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
)

// Config for an Ingester.
type Config struct {
	LifecyclerConfig ring.LifecyclerConfig
	userStatesConfig UserStatesConfig
	clientConfig     client.Config

	// Config for transferring chunks.
	SearchPendingFor time.Duration

	// Config for chunk flushing.
	FlushCheckPeriod  time.Duration
	MaxChunkIdle      time.Duration
	FlushOpTimeout    time.Duration
	MaxChunkAge       time.Duration
	ConcurrentFlushes int
	ChunkEncoding     string

	// For testing, you can override the address and ID of this ingester.
	ingesterClientFactory func(addr string, cfg client.Config) (client.IngesterClient, error)
}

// SetClientConfig sets clientConfig in config
func (cfg *Config) SetClientConfig(clientConfig client.Config) {
	cfg.clientConfig = clientConfig
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.LifecyclerConfig.RegisterFlags(f)
	cfg.userStatesConfig.RegisterFlags(f)
	cfg.clientConfig.RegisterFlags(f)

	f.DurationVar(&cfg.SearchPendingFor, "ingester.search-pending-for", 30*time.Second, "Time to spend searching for a pending ingester when shutting down.")
	f.DurationVar(&cfg.FlushCheckPeriod, "ingester.flush-period", 1*time.Minute, "Period with which to attempt to flush chunks.")
	f.DurationVar(&cfg.FlushOpTimeout, "ingester.flush-op-timeout", 1*time.Minute, "Timeout for individual flush operations.")
	f.DurationVar(&cfg.MaxChunkIdle, "ingester.max-chunk-idle", 5*time.Minute, "Maximum chunk idle time before flushing.")
	f.DurationVar(&cfg.MaxChunkAge, "ingester.max-chunk-age", 12*time.Hour, "Maximum chunk age before flushing.")
	f.IntVar(&cfg.ConcurrentFlushes, "ingester.concurrent-flushes", DefaultConcurrentFlush, "Number of concurrent goroutines flushing to dynamodb.")
	f.StringVar(&cfg.ChunkEncoding, "ingester.chunk-encoding", "1", "Encoding version to use for chunks.")

	// DEPRECATED, no-op
	f.Bool("ingester.reject-old-samples", false, "DEPRECATED. Reject old samples.")
	f.Duration("ingester.reject-old-samples.max-age", 0, "DEPRECATED. Maximum accepted sample age before rejecting.")
	f.Int("ingester.validation.max-length-label-name", 0, "DEPRECATED. Maximum length accepted for label names.")
	f.Int("ingester.validation.max-length-label-value", 0, "DEPRECATED. Maximum length accepted for label value. This setting also applies to the metric name.")
	f.Int("ingester.max-label-names-per-series", 0, "DEPRECATED. Maximum number of label names per series.")
}

// Ingester deals with "in flight" chunks.  Based on Prometheus 1.x
// MemorySeriesStorage.
type Ingester struct {
	cfg        Config
	chunkStore ChunkStore
	lifecycler *ring.Lifecycler

	stopLock sync.RWMutex
	stopped  bool
	quit     chan struct{}
	done     sync.WaitGroup

	userStatesMtx sync.RWMutex
	userStates    *userStates

	// One queue per flush thread.  Fingerprint is used to
	// pick a queue.
	flushQueues     []*util.PriorityQueue
	flushQueuesDone sync.WaitGroup

	// Hook for injecting behaviour from tests.
	preFlushUserSeries func()

	ingestedSamples     prometheus.Counter
	ingestedSamplesFail prometheus.Counter
	chunkUtilization    prometheus.Histogram
	chunkLength         prometheus.Histogram
	chunkAge            prometheus.Histogram
	queries             prometheus.Counter
	queriedSamples      prometheus.Counter
	memoryChunks        prometheus.Gauge
}

// ChunkStore is the interface we need to store chunks
type ChunkStore interface {
	Put(ctx context.Context, chunks []cortex_chunk.Chunk) error
}

// New constructs a new Ingester.
func New(cfg Config, chunkStore ChunkStore) (*Ingester, error) {
	if cfg.FlushCheckPeriod == 0 {
		cfg.FlushCheckPeriod = 1 * time.Minute
	}
	if cfg.MaxChunkIdle == 0 {
		cfg.MaxChunkIdle = 1 * time.Hour
	}
	if cfg.ConcurrentFlushes <= 0 {
		cfg.ConcurrentFlushes = DefaultConcurrentFlush
	}
	if cfg.ChunkEncoding == "" {
		cfg.ChunkEncoding = "1"
	}
	if cfg.userStatesConfig.RateUpdatePeriod == 0 {
		cfg.userStatesConfig.RateUpdatePeriod = 15 * time.Second
	}
	if cfg.userStatesConfig.MaxSeriesPerUser <= 0 {
		cfg.userStatesConfig.MaxSeriesPerUser = DefaultMaxSeriesPerUser
	}
	if cfg.userStatesConfig.MaxSeriesPerMetric <= 0 {
		cfg.userStatesConfig.MaxSeriesPerMetric = DefaultMaxSeriesPerMetric
	}
	if cfg.ingesterClientFactory == nil {
		cfg.ingesterClientFactory = client.MakeIngesterClient
	}

	if err := chunk.DefaultEncoding.Set(cfg.ChunkEncoding); err != nil {
		return nil, err
	}

	i := &Ingester{
		cfg: cfg,

		chunkStore: chunkStore,
		userStates: newUserStates(&cfg.userStatesConfig),

		quit:        make(chan struct{}),
		flushQueues: make([]*util.PriorityQueue, cfg.ConcurrentFlushes, cfg.ConcurrentFlushes),

		ingestedSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_ingested_samples_total",
			Help: "The total number of samples ingested.",
		}),
		ingestedSamplesFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_ingested_samples_failures_total",
			Help: "The total number of samples that errored on ingestion.",
		}),
		chunkUtilization: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_chunk_utilization",
			Help:    "Distribution of stored chunk utilization (when stored).",
			Buckets: prometheus.LinearBuckets(0, 0.2, 6),
		}),
		chunkLength: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_chunk_length",
			Help:    "Distribution of stored chunk lengths (when stored).",
			Buckets: prometheus.ExponentialBuckets(5, 2, 11), // biggest bucket is 5*2^(11-1) = 5120
		}),
		chunkAge: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "cortex_ingester_chunk_age_seconds",
			Help: "Distribution of chunk ages (when stored).",
			// with default settings chunks should flush between 5 min and 12 hours
			// so buckets at 1min, 5min, 10min, 30min, 1hr, 2hr, 4hr, 10hr, 12hr, 16hr
			Buckets: []float64{60, 300, 600, 1800, 3600, 7200, 14400, 36000, 43200, 57600},
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

	var err error
	i.lifecycler, err = ring.NewLifecycler(cfg.LifecyclerConfig, i)
	if err != nil {
		return nil, err
	}

	i.flushQueuesDone.Add(cfg.ConcurrentFlushes)
	for j := 0; j < cfg.ConcurrentFlushes; j++ {
		i.flushQueues[j] = util.NewPriorityQueue()
		go i.flushLoop(j)
	}

	i.done.Add(1)
	go i.loop()

	return i, nil
}

func (i *Ingester) loop() {
	defer i.done.Done()

	flushTicker := time.NewTicker(i.cfg.FlushCheckPeriod)
	defer flushTicker.Stop()

	rateUpdateTicker := time.NewTicker(i.cfg.userStatesConfig.RateUpdatePeriod)
	defer rateUpdateTicker.Stop()

	for {
		select {
		case <-flushTicker.C:
			i.sweepUsers(false)

		case <-rateUpdateTicker.C:
			i.userStates.updateRates()

		case <-i.quit:
			return
		}
	}
}

// Shutdown beings the process to stop this ingester.
func (i *Ingester) Shutdown() {
	// First wait for our flush loop to stop.
	close(i.quit)
	i.done.Wait()

	// Next initiate our graceful exit from the ring.
	i.lifecycler.Shutdown()
}

// StopIncomingRequests is called during the shutdown process.
func (i *Ingester) StopIncomingRequests() {
	i.stopLock.Lock()
	defer i.stopLock.Unlock()
	i.stopped = true
}

// Push implements client.IngesterServer
func (i *Ingester) Push(ctx old_ctx.Context, req *client.WriteRequest) (*client.WriteResponse, error) {
	var lastPartialErr error
	samples := client.FromWriteRequest(req)

	for j := range samples {
		err := i.append(ctx, &samples[j])
		if err == nil {
			continue
		}

		i.ingestedSamplesFail.Inc()
		if httpResp, ok := httpgrpc.HTTPResponseFromError(err); ok {
			switch httpResp.Code {
			case http.StatusBadRequest, http.StatusTooManyRequests:
				lastPartialErr = err
				continue
			}
		}

		return nil, err
	}

	return &client.WriteResponse{}, lastPartialErr
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

	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, fp, series, err := i.userStates.getOrCreateSeries(ctx, sample.Metric)
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
		if mse, ok := err.(*memorySeriesError); ok {
			validation.DiscardedSamples.WithLabelValues(mse.errorType, state.userID).Inc()
			// Use a dumb string template to avoid the message being parsed as a template
			err = httpgrpc.Errorf(http.StatusBadRequest, "%s", mse.message)
		}
		return err
	}

	i.memoryChunks.Add(float64(len(series.chunkDescs) - prevNumChunks))
	i.ingestedSamples.Inc()
	state.ingestedSamples.inc()

	return err
}

// Query implements service.IngesterServer
func (i *Ingester) Query(ctx old_ctx.Context, req *client.QueryRequest) (*client.QueryResponse, error) {
	start, end, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return nil, err
	}

	matrix, err := i.query(ctx, start, end, matchers)
	if err != nil {
		return nil, err
	}

	return client.ToQueryResponse(matrix), nil
}

func (i *Ingester) query(ctx context.Context, from, through model.Time, matchers []*labels.Matcher) (model.Matrix, error) {
	i.queries.Inc()

	result := model.Matrix{}
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, ok, err := i.userStates.getViaContext(ctx)
	if err != nil {
		return nil, err
	} else if !ok {
		return result, nil
	}

	queriedSamples := 0
	err = state.forSeriesMatching(matchers, func(_ model.Fingerprint, series *memorySeries) error {
		values, err := series.samplesForRange(from, through)
		if err != nil {
			return err
		}

		result = append(result, &model.SampleStream{
			Metric: series.metric,
			Values: values,
		})
		queriedSamples += len(values)
		return nil
	})
	i.queriedSamples.Add(float64(queriedSamples))
	return result, err
}

// LabelValues returns all label values that are associated with a given label name.
func (i *Ingester) LabelValues(ctx old_ctx.Context, req *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, ok, err := i.userStates.getViaContext(ctx)
	if err != nil {
		return nil, err
	} else if !ok {
		return &client.LabelValuesResponse{}, nil
	}

	resp := &client.LabelValuesResponse{}
	for _, v := range state.index.lookupLabelValues(model.LabelName(req.LabelName)) {
		resp.LabelValues = append(resp.LabelValues, string(v))
	}

	return resp, nil
}

// MetricsForLabelMatchers returns all the metrics which match a set of matchers.
func (i *Ingester) MetricsForLabelMatchers(ctx old_ctx.Context, req *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, ok, err := i.userStates.getViaContext(ctx)
	if err != nil {
		return nil, err
	} else if !ok {
		return client.ToMetricsForLabelMatchersResponse(nil), nil
	}

	// TODO Right now we ignore start and end.
	_, _, matchersSet, err := client.FromMetricsForLabelMatchersRequest(req)
	if err != nil {
		return nil, err
	}

	metrics := map[model.Fingerprint]model.Metric{}
	for _, matchers := range matchersSet {
		if err := state.forSeriesMatching(matchers, func(fp model.Fingerprint, series *memorySeries) error {
			if _, ok := metrics[fp]; !ok {
				metrics[fp] = series.metric
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	result := []model.Metric{}
	for _, metric := range metrics {
		result = append(result, metric)
	}

	return client.ToMetricsForLabelMatchersResponse(result), nil
}

// UserStats returns ingestion statistics for the current user.
func (i *Ingester) UserStats(ctx old_ctx.Context, req *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, ok, err := i.userStates.getViaContext(ctx)
	if err != nil {
		return nil, err
	} else if !ok {
		return &client.UserStatsResponse{}, nil
	}

	return &client.UserStatsResponse{
		IngestionRate: state.ingestedSamples.rate(),
		NumSeries:     uint64(state.fpToSeries.length()),
	}, nil
}

// AllUserStats returns ingestion statistics for all users known to this ingester.
func (i *Ingester) AllUserStats(ctx old_ctx.Context, req *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	users := i.userStates.cp()

	response := &client.UsersStatsResponse{
		Stats: make([]*client.UserIDStatsResponse, 0, len(users)),
	}
	for userID, state := range users {
		response.Stats = append(response.Stats, &client.UserIDStatsResponse{
			UserId: userID,
			Data: &client.UserStatsResponse{
				IngestionRate: state.ingestedSamples.rate(),
				NumSeries:     uint64(state.fpToSeries.length()),
			},
		})
	}
	return response, nil
}

// Check implements the grpc healthcheck
func (i *Ingester) Check(ctx old_ctx.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// ReadinessHandler is used to indicate to k8s when the ingesters are ready for
// the addition removal of another ingester. Returns 204 when the ingester is
// ready, 500 otherwise.
func (i *Ingester) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	if i.lifecycler.IsReady(r.Context()) {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
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
	sp := opentracing.StartSpan("HTTP - Metrics")
	defer sp.Finish()

	seriesSpan := opentracing.StartSpan("count_series", opentracing.ChildOf(sp.Context()))
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	numUsers := i.userStates.numUsers()
	numSeries := i.userStates.numSeries()

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
	seriesSpan.Finish()

	flushQueueSpan := opentracing.StartSpan("flush_queue", opentracing.ChildOf(sp.Context()))
	flushQueueLength := 0
	for _, flushQueue := range i.flushQueues {
		flushQueueLength += flushQueue.Length()
	}

	ch <- prometheus.MustNewConstMetric(
		flushQueueLengthDesc,
		prometheus.GaugeValue,
		float64(flushQueueLength),
	)
	flushQueueSpan.Finish()

	ch <- i.ingestedSamples
	ch <- i.chunkUtilization
	ch <- i.chunkLength
	ch <- i.chunkAge
	ch <- i.queries
	ch <- i.queriedSamples
	ch <- i.memoryChunks
}
