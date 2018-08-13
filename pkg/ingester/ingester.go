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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"

	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	cortex_chunk "github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/ring"
	"github.com/weaveworks/cortex/pkg/util"
	"github.com/weaveworks/cortex/pkg/util/validation"
)

const (
	// Reasons to discard samples.
	outOfOrderTimestamp = "timestamp_out_of_order"
	duplicateSample     = "multiple_values_for_timestamp"
)

var (
	flushQueueLength = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_flush_queue_length",
		Help: "The total number of series pending in the flush queue.",
	})
	ingestedSamples = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_ingested_samples_total",
		Help: "The total number of samples ingested.",
	})
	ingestedSamplesFail = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_ingested_samples_failures_total",
		Help: "The total number of samples that errored on ingestion.",
	})
	queries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_queries_total",
		Help: "The total number of queries the ingester has handled.",
	})
	queriedSamples = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "cortex_ingester_queried_samples",
		Help:    "The total number of samples returned from queries.",
		Buckets: prometheus.DefBuckets,
	})
	queriedSeries = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "cortex_ingester_queried_series",
		Help:    "The total number of series returned from queries.",
		Buckets: prometheus.DefBuckets,
	})
)

// Config for an Ingester.
type Config struct {
	LifecyclerConfig ring.LifecyclerConfig
	clientConfig     client.Config
	limits           validation.Limits

	// Config for transferring chunks.
	SearchPendingFor time.Duration

	// Config for chunk flushing.
	FlushCheckPeriod  time.Duration
	MaxChunkIdle      time.Duration
	FlushOpTimeout    time.Duration
	MaxChunkAge       time.Duration
	ConcurrentFlushes int
	ChunkEncoding     string

	RateUpdatePeriod time.Duration

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
	cfg.clientConfig.RegisterFlags(f)
	cfg.limits.RegisterFlags(f)

	f.DurationVar(&cfg.SearchPendingFor, "ingester.search-pending-for", 30*time.Second, "Time to spend searching for a pending ingester when shutting down.")
	f.DurationVar(&cfg.FlushCheckPeriod, "ingester.flush-period", 1*time.Minute, "Period with which to attempt to flush chunks.")
	f.DurationVar(&cfg.FlushOpTimeout, "ingester.flush-op-timeout", 1*time.Minute, "Timeout for individual flush operations.")
	f.DurationVar(&cfg.MaxChunkIdle, "ingester.max-chunk-idle", 5*time.Minute, "Maximum chunk idle time before flushing.")
	f.DurationVar(&cfg.MaxChunkAge, "ingester.max-chunk-age", 12*time.Hour, "Maximum chunk age before flushing.")
	f.IntVar(&cfg.ConcurrentFlushes, "ingester.concurrent-flushes", 50, "Number of concurrent goroutines flushing to dynamodb.")
	f.StringVar(&cfg.ChunkEncoding, "ingester.chunk-encoding", "1", "Encoding version to use for chunks.")
	f.DurationVar(&cfg.RateUpdatePeriod, "ingester.rate-update-period", 15*time.Second, "Period with which to update the per-user ingestion rates.")

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
	limits     *validation.Overrides

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
}

// ChunkStore is the interface we need to store chunks
type ChunkStore interface {
	Put(ctx context.Context, chunks []cortex_chunk.Chunk) error
}

// New constructs a new Ingester.
func New(cfg Config, chunkStore ChunkStore) (*Ingester, error) {
	if cfg.ingesterClientFactory == nil {
		cfg.ingesterClientFactory = client.MakeIngesterClient
	}

	if err := chunk.DefaultEncoding.Set(cfg.ChunkEncoding); err != nil {
		return nil, err
	}

	limits, err := validation.NewOverrides(cfg.limits)
	if err != nil {
		return nil, err
	}

	i := &Ingester{
		cfg: cfg,

		limits:     limits,
		chunkStore: chunkStore,
		userStates: newUserStates(limits, cfg),

		quit:        make(chan struct{}),
		flushQueues: make([]*util.PriorityQueue, cfg.ConcurrentFlushes, cfg.ConcurrentFlushes),
	}

	i.lifecycler, err = ring.NewLifecycler(cfg.LifecyclerConfig, i)
	if err != nil {
		return nil, err
	}

	i.flushQueuesDone.Add(cfg.ConcurrentFlushes)
	for j := 0; j < cfg.ConcurrentFlushes; j++ {
		i.flushQueues[j] = util.NewPriorityQueue(flushQueueLength)
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

	rateUpdateTicker := time.NewTicker(i.cfg.RateUpdatePeriod)
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
	i.limits.Stop()

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

		ingestedSamplesFail.Inc()
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

	memoryChunks.Add(float64(len(series.chunkDescs) - prevNumChunks))
	ingestedSamples.Inc()
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
	queries.Inc()

	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	result := model.Matrix{}
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, ok, err := i.userStates.getViaContext(ctx)
	if err != nil {
		return nil, err
	} else if !ok {
		return result, nil
	}

	numSamples := 0
	numSeries := 0
	maxSamplesPerQuery := i.limits.MaxSamplesPerQuery(userID)
	err = state.forSeriesMatching(matchers, func(_ model.Fingerprint, series *memorySeries) error {
		numSeries++
		values, err := series.samplesForRange(from, through)
		if err != nil {
			return err
		}

		numSamples += len(values)
		if numSamples > maxSamplesPerQuery {
			return httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "exceeded maximum number of samples in a query (%d)", maxSamplesPerQuery)
		}

		result = append(result, &model.SampleStream{
			Metric: series.metric,
			Values: values,
		})

		return nil
	})
	queriedSamples.Observe(float64(numSamples))
	queriedSeries.Observe(float64(numSeries))
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
