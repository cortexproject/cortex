package ingester

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/gogo/status"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"

	cortex_chunk "github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	// Number of timeseries to return in each batch of a QueryStream.
	queryStreamBatchSize = 128
)

var (
	// This is initialised if the WAL is enabled and the records are fetched from this pool.
	recordPool sync.Pool
)

// Config for an Ingester.
type Config struct {
	WALConfig        WALConfig             `yaml:"walconfig,omitempty"`
	LifecyclerConfig ring.LifecyclerConfig `yaml:"lifecycler,omitempty"`

	// Config for transferring chunks. Zero or negative = no retries.
	MaxTransferRetries int `yaml:"max_transfer_retries,omitempty"`

	// Config for chunk flushing.
	FlushCheckPeriod  time.Duration
	RetainPeriod      time.Duration
	MaxChunkIdle      time.Duration
	MaxStaleChunkIdle time.Duration
	FlushOpTimeout    time.Duration
	MaxChunkAge       time.Duration
	ChunkAgeJitter    time.Duration
	ConcurrentFlushes int
	SpreadFlushes     bool

	RateUpdatePeriod time.Duration

	// Use tsdb block storage
	TSDBEnabled bool        `yaml:"-"`
	TSDBConfig  tsdb.Config `yaml:"-"`

	// Injected at runtime and read from the distributor config, required
	// to accurately apply global limits.
	ShardByAllLabels bool `yaml:"-"`

	// For testing, you can override the address and ID of this ingester.
	ingesterClientFactory func(addr string, cfg client.Config) (client.HealthAndIngesterClient, error)
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.LifecyclerConfig.RegisterFlags(f)
	cfg.WALConfig.RegisterFlags(f)

	f.IntVar(&cfg.MaxTransferRetries, "ingester.max-transfer-retries", 10, "Number of times to try and transfer chunks before falling back to flushing. Negative value or zero disables hand-over.")
	f.DurationVar(&cfg.FlushCheckPeriod, "ingester.flush-period", 1*time.Minute, "Period with which to attempt to flush chunks.")
	f.DurationVar(&cfg.RetainPeriod, "ingester.retain-period", 5*time.Minute, "Period chunks will remain in memory after flushing.")
	f.DurationVar(&cfg.FlushOpTimeout, "ingester.flush-op-timeout", 1*time.Minute, "Timeout for individual flush operations.")
	f.DurationVar(&cfg.MaxChunkIdle, "ingester.max-chunk-idle", 5*time.Minute, "Maximum chunk idle time before flushing.")
	f.DurationVar(&cfg.MaxStaleChunkIdle, "ingester.max-stale-chunk-idle", 0, "Maximum chunk idle time for chunks terminating in stale markers before flushing. 0 disables it and a stale series is not flushed until the max-chunk-idle timeout is reached.")
	f.DurationVar(&cfg.MaxChunkAge, "ingester.max-chunk-age", 12*time.Hour, "Maximum chunk age before flushing.")
	f.DurationVar(&cfg.ChunkAgeJitter, "ingester.chunk-age-jitter", 20*time.Minute, "Range of time to subtract from MaxChunkAge to spread out flushes")
	f.BoolVar(&cfg.SpreadFlushes, "ingester.spread-flushes", false, "If true, spread series flushes across the whole period of MaxChunkAge")
	f.IntVar(&cfg.ConcurrentFlushes, "ingester.concurrent-flushes", 50, "Number of concurrent goroutines flushing to dynamodb.")
	f.DurationVar(&cfg.RateUpdatePeriod, "ingester.rate-update-period", 15*time.Second, "Period with which to update the per-user ingestion rates.")
}

// Ingester deals with "in flight" chunks.  Based on Prometheus 1.x
// MemorySeriesStorage.
type Ingester struct {
	services.Service

	cfg          Config
	clientConfig client.Config

	metrics *ingesterMetrics

	chunkStore ChunkStore
	lifecycler *ring.Lifecycler
	limits     *validation.Overrides
	limiter    *SeriesLimiter

	userStatesMtx sync.RWMutex // protects userStates and stopped
	userStates    *userStates
	stopped       bool // protected by userStatesMtx

	// One queue per flush thread.  Fingerprint is used to
	// pick a queue.
	flushQueues     []*util.PriorityQueue
	flushQueuesDone sync.WaitGroup

	// This should never be nil.
	wal WAL

	// Hook for injecting behaviour from tests.
	preFlushUserSeries func()

	// Prometheus block storage
	TSDBState TSDBState
}

// ChunkStore is the interface we need to store chunks
type ChunkStore interface {
	Put(ctx context.Context, chunks []cortex_chunk.Chunk) error
}

// New constructs a new Ingester.
func New(cfg Config, clientConfig client.Config, limits *validation.Overrides, chunkStore ChunkStore, registerer prometheus.Registerer) (*Ingester, error) {
	if cfg.ingesterClientFactory == nil {
		cfg.ingesterClientFactory = client.MakeIngesterClient
	}

	if cfg.TSDBEnabled {
		return NewV2(cfg, clientConfig, limits, registerer)
	}

	if cfg.WALConfig.WALEnabled {
		// If WAL is enabled, we don't transfer out the data to any ingester.
		// Either the next ingester which takes it's place should recover from WAL
		// or the data has to be flushed during scaledown.
		cfg.MaxTransferRetries = 0

		recordPool = sync.Pool{
			New: func() interface{} {
				return &Record{}
			},
		}
	}

	i := &Ingester{
		cfg:          cfg,
		clientConfig: clientConfig,
		metrics:      newIngesterMetrics(registerer, true),
		limits:       limits,
		chunkStore:   chunkStore,
		flushQueues:  make([]*util.PriorityQueue, cfg.ConcurrentFlushes),
	}

	var err error
	// During WAL recovery, it will create new user states which requires the limiter.
	// Hence initialise the limiter before creating the WAL.
	// The '!cfg.WALConfig.WALEnabled' argument says don't flush on shutdown if the WAL is enabled.
	i.lifecycler, err = ring.NewLifecycler(cfg.LifecyclerConfig, i, "ingester", ring.IngesterRingKey, !cfg.WALConfig.WALEnabled)
	if err != nil {
		return nil, err
	}
	i.limiter = NewSeriesLimiter(limits, i.lifecycler, cfg.LifecyclerConfig.RingConfig.ReplicationFactor, cfg.ShardByAllLabels)

	if cfg.WALConfig.Recover {
		level.Info(util.Logger).Log("msg", "recovering from WAL")
		start := time.Now()
		if err := recoverFromWAL(i); err != nil {
			level.Error(util.Logger).Log("msg", "failed to recover from WAL", "time", time.Since(start).String())
			return nil, err
		}
		elapsed := time.Since(start)
		level.Info(util.Logger).Log("msg", "recovery from WAL completed", "time", elapsed.String())
		i.metrics.walReplayDuration.Set(elapsed.Seconds())
	}

	// If the WAL recover happened, then the userStates would already be set.
	if i.userStates == nil {
		i.userStates = newUserStates(i.limiter, cfg, i.metrics)
	}

	i.wal, err = newWAL(cfg.WALConfig, i.userStates.cp)
	if err != nil {
		return nil, err
	}

	// TODO: lot more stuff can be put into startingFn (esp. WAL replay), but for now keep it in New
	i.Service = services.NewBasicService(i.starting, i.loop, i.stopping)
	return i, nil
}

func (i *Ingester) starting(ctx context.Context) error {
	// Now that user states have been created, we can start the lifecycler.
	// Important: we want to keep lifecycler running until we ask it to stop, so we need to give it independent context
	if err := i.lifecycler.StartAsync(context.Background()); err != nil {
		return errors.Wrap(err, "failed to start lifecycler")
	}
	if err := i.lifecycler.AwaitRunning(ctx); err != nil {
		return errors.Wrap(err, "failed to start lifecycler")
	}

	i.flushQueuesDone.Add(i.cfg.ConcurrentFlushes)
	for j := 0; j < i.cfg.ConcurrentFlushes; j++ {
		i.flushQueues[j] = util.NewPriorityQueue(i.metrics.flushQueueLength)
		go i.flushLoop(j)
	}

	return nil
}

func (i *Ingester) loop(ctx context.Context) error {
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

		case <-ctx.Done():
			return nil
		}
	}
}

// stopping is run when ingester is asked to stop
func (i *Ingester) stopping() error {
	i.wal.Stop()

	// Next initiate our graceful exit from the ring.
	return services.StopAndAwaitTerminated(context.Background(), i.lifecycler)
}

// ShutdownHandler triggers the following set of operations in order:
//     * Change the state of ring to stop accepting writes.
//     * Flush all the chunks.
func (i *Ingester) ShutdownHandler(w http.ResponseWriter, r *http.Request) {
	originalState := i.lifecycler.FlushOnShutdown()
	// We want to flush the chunks if transfer fails irrespective of original flag.
	i.lifecycler.SetFlushOnShutdown(true)
	_ = services.StopAndAwaitTerminated(context.Background(), i)
	i.lifecycler.SetFlushOnShutdown(originalState)
	w.WriteHeader(http.StatusNoContent)
}

// StopIncomingRequests is called during the shutdown process.
func (i *Ingester) StopIncomingRequests() {
	i.userStatesMtx.Lock()
	defer i.userStatesMtx.Unlock()
	i.stopped = true
}

// Push implements client.IngesterServer
func (i *Ingester) Push(ctx context.Context, req *client.WriteRequest) (*client.WriteResponse, error) {
	if i.cfg.TSDBEnabled {
		return i.v2Push(ctx, req)
	}

	// NOTE: because we use `unsafe` in deserialisation, we must not
	// retain anything from `req` past the call to ReuseSlice
	defer client.ReuseSlice(req.Timeseries)

	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id")
	}

	var lastPartialErr *validationError
	var record *Record
	if i.cfg.WALConfig.WALEnabled {
		record = recordPool.Get().(*Record)
		record.UserId = userID
		// Assuming there is not much churn in most cases, there is no use
		// keeping the record.Labels slice hanging around.
		record.Labels = nil
		if cap(record.Samples) < len(req.Timeseries) {
			record.Samples = make([]Sample, 0, len(req.Timeseries))
		} else {
			record.Samples = record.Samples[:0]
		}
	}

	for _, ts := range req.Timeseries {
		for _, s := range ts.Samples {
			// append() copies the memory in `ts.Labels` except on the error path
			err := i.append(ctx, userID, ts.Labels, model.Time(s.TimestampMs), model.SampleValue(s.Value), req.Source, record)
			if err == nil {
				continue
			}

			i.metrics.ingestedSamplesFail.Inc()
			if ve, ok := err.(*validationError); ok {
				lastPartialErr = ve
				continue
			}

			// non-validation error: abandon this request
			return nil, grpcForwardableError(userID, http.StatusInternalServerError, err)
		}
	}

	if lastPartialErr != nil {
		// grpcForwardableError turns the error into a string so it no longer references `req`
		return &client.WriteResponse{}, grpcForwardableError(userID, lastPartialErr.code, lastPartialErr)
	}

	if record != nil {
		// Log the record only if there was no error in ingestion.
		if err := i.wal.Log(record); err != nil {
			return nil, err
		}
		recordPool.Put(record)
	}

	return &client.WriteResponse{}, nil
}

// NOTE: memory for `labels` is unsafe; anything retained beyond the
// life of this function must be copied
func (i *Ingester) append(ctx context.Context, userID string, labels labelPairs, timestamp model.Time, value model.SampleValue, source client.WriteRequest_SourceEnum, record *Record) error {
	labels.removeBlanks()

	var (
		state *userState
		fp    model.Fingerprint
	)
	i.userStatesMtx.RLock()
	defer func() {
		i.userStatesMtx.RUnlock()
		if state != nil {
			state.fpLocker.Unlock(fp)
		}
	}()
	if i.stopped {
		return fmt.Errorf("ingester stopping")
	}

	// getOrCreateSeries copies the memory for `labels`, except on the error path.
	state, fp, series, err := i.userStates.getOrCreateSeries(ctx, userID, labels, record)
	if err != nil {
		if ve, ok := err.(*validationError); ok {
			state.discardedSamples.WithLabelValues(ve.errorType).Inc()
		}

		// Reset the state so that the defer will not try to unlock the fpLocker
		// in case of error, because that lock has already been released on error.
		state = nil
		return err
	}

	prevNumChunks := len(series.chunkDescs)
	if i.cfg.SpreadFlushes && prevNumChunks > 0 {
		// Map from the fingerprint hash to a point in the cycle of period MaxChunkAge
		startOfCycle := timestamp.Add(-(timestamp.Sub(model.Time(0)) % i.cfg.MaxChunkAge))
		slot := startOfCycle.Add(time.Duration(uint64(fp) % uint64(i.cfg.MaxChunkAge)))
		// If adding this sample means the head chunk will span that point in time, close so it will get flushed
		if series.head().FirstTime < slot && timestamp >= slot {
			series.closeHead(reasonSpreadFlush)
		}
	}

	if err := series.add(model.SamplePair{
		Value:     value,
		Timestamp: timestamp,
	}); err != nil {
		if ve, ok := err.(*validationError); ok {
			state.discardedSamples.WithLabelValues(ve.errorType).Inc()
			if ve.noReport {
				return nil
			}
		}
		return err
	}

	if record != nil {
		record.Samples = append(record.Samples, Sample{
			Fingerprint: uint64(fp),
			Timestamp:   uint64(timestamp),
			Value:       float64(value),
		})
	}

	memoryChunks.Add(float64(len(series.chunkDescs) - prevNumChunks))
	i.metrics.ingestedSamples.Inc()
	switch source {
	case client.RULE:
		state.ingestedRuleSamples.inc()
	case client.API:
		fallthrough
	default:
		state.ingestedAPISamples.inc()
	}

	return err
}

// Query implements service.IngesterServer
func (i *Ingester) Query(ctx context.Context, req *client.QueryRequest) (*client.QueryResponse, error) {
	if i.cfg.TSDBEnabled {
		return i.v2Query(ctx, req)
	}

	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	from, through, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return nil, err
	}

	i.metrics.queries.Inc()

	i.userStatesMtx.RLock()
	state, ok, err := i.userStates.getViaContext(ctx)
	i.userStatesMtx.RUnlock()
	if err != nil {
		return nil, err
	} else if !ok {
		return &client.QueryResponse{}, nil
	}

	result := &client.QueryResponse{}
	numSeries, numSamples := 0, 0
	maxSamplesPerQuery := i.limits.MaxSamplesPerQuery(userID)
	err = state.forSeriesMatching(ctx, matchers, func(ctx context.Context, _ model.Fingerprint, series *memorySeries) error {
		values, err := series.samplesForRange(from, through)
		if err != nil {
			return err
		}
		if len(values) == 0 {
			return nil
		}
		numSeries++

		numSamples += len(values)
		if numSamples > maxSamplesPerQuery {
			return httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "exceeded maximum number of samples in a query (%d)", maxSamplesPerQuery)
		}

		ts := client.TimeSeries{
			Labels:  client.FromLabelsToLabelAdapters(series.metric),
			Samples: make([]client.Sample, 0, len(values)),
		}
		for _, s := range values {
			ts.Samples = append(ts.Samples, client.Sample{
				Value:       float64(s.Value),
				TimestampMs: int64(s.Timestamp),
			})
		}
		result.Timeseries = append(result.Timeseries, ts)
		return nil
	}, nil, 0)
	i.metrics.queriedSeries.Observe(float64(numSeries))
	i.metrics.queriedSamples.Observe(float64(numSamples))
	return result, err
}

// QueryStream implements service.IngesterServer
func (i *Ingester) QueryStream(req *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
	if i.cfg.TSDBEnabled {
		return fmt.Errorf("Unimplemented for V2")
	}

	log, ctx := spanlogger.New(stream.Context(), "QueryStream")

	from, through, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return err
	}

	i.metrics.queries.Inc()

	i.userStatesMtx.RLock()
	state, ok, err := i.userStates.getViaContext(ctx)
	i.userStatesMtx.RUnlock()
	if err != nil {
		return err
	} else if !ok {
		return nil
	}

	numSeries, numChunks := 0, 0
	batch := make([]client.TimeSeriesChunk, 0, queryStreamBatchSize)
	// We'd really like to have series in label order, not FP order, so we
	// can iteratively merge them with entries coming from the chunk store.  But
	// that would involve locking all the series & sorting, so until we have
	// a better solution in the ingesters I'd rather take the hit in the queriers.
	err = state.forSeriesMatching(stream.Context(), matchers, func(ctx context.Context, _ model.Fingerprint, series *memorySeries) error {
		chunks := make([]*desc, 0, len(series.chunkDescs))
		for _, chunk := range series.chunkDescs {
			if !(chunk.FirstTime.After(through) || chunk.LastTime.Before(from)) {
				chunks = append(chunks, chunk.slice(from, through))
			}
		}

		if len(chunks) == 0 {
			return nil
		}

		numSeries++
		wireChunks, err := toWireChunks(chunks, nil)
		if err != nil {
			return err
		}

		numChunks += len(wireChunks)
		batch = append(batch, client.TimeSeriesChunk{
			Labels: client.FromLabelsToLabelAdapters(series.metric),
			Chunks: wireChunks,
		})

		return nil
	}, func(ctx context.Context) error {
		if len(batch) == 0 {
			return nil
		}
		err = stream.Send(&client.QueryStreamResponse{
			Timeseries: batch,
		})
		batch = batch[:0]
		return err
	}, queryStreamBatchSize)
	if err != nil {
		return err
	}

	i.metrics.queriedSeries.Observe(float64(numSeries))
	i.metrics.queriedChunks.Observe(float64(numChunks))
	level.Debug(log).Log("streams", numSeries)
	level.Debug(log).Log("chunks", numChunks)
	return err
}

// LabelValues returns all label values that are associated with a given label name.
func (i *Ingester) LabelValues(ctx context.Context, req *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	if i.cfg.TSDBEnabled {
		return i.v2LabelValues(ctx, req)
	}

	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, ok, err := i.userStates.getViaContext(ctx)
	if err != nil {
		return nil, err
	} else if !ok {
		return &client.LabelValuesResponse{}, nil
	}

	resp := &client.LabelValuesResponse{}
	resp.LabelValues = append(resp.LabelValues, state.index.LabelValues(req.LabelName)...)

	return resp, nil
}

// LabelNames return all the label names.
func (i *Ingester) LabelNames(ctx context.Context, req *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	if i.cfg.TSDBEnabled {
		return i.v2LabelNames(ctx, req)
	}

	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, ok, err := i.userStates.getViaContext(ctx)
	if err != nil {
		return nil, err
	} else if !ok {
		return &client.LabelNamesResponse{}, nil
	}

	resp := &client.LabelNamesResponse{}
	resp.LabelNames = append(resp.LabelNames, state.index.LabelNames()...)

	return resp, nil
}

// MetricsForLabelMatchers returns all the metrics which match a set of matchers.
func (i *Ingester) MetricsForLabelMatchers(ctx context.Context, req *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	if i.cfg.TSDBEnabled {
		return i.v2MetricsForLabelMatchers(ctx, req)
	}

	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, ok, err := i.userStates.getViaContext(ctx)
	if err != nil {
		return nil, err
	} else if !ok {
		return &client.MetricsForLabelMatchersResponse{}, nil
	}

	// TODO Right now we ignore start and end.
	_, _, matchersSet, err := client.FromMetricsForLabelMatchersRequest(req)
	if err != nil {
		return nil, err
	}

	lss := map[model.Fingerprint]labels.Labels{}
	for _, matchers := range matchersSet {
		if err := state.forSeriesMatching(ctx, matchers, func(ctx context.Context, fp model.Fingerprint, series *memorySeries) error {
			if _, ok := lss[fp]; !ok {
				lss[fp] = series.metric
			}
			return nil
		}, nil, 0); err != nil {
			return nil, err
		}
	}

	result := &client.MetricsForLabelMatchersResponse{
		Metric: make([]*client.Metric, 0, len(lss)),
	}
	for _, ls := range lss {
		result.Metric = append(result.Metric, &client.Metric{Labels: client.FromLabelsToLabelAdapters(ls)})
	}

	return result, nil
}

// UserStats returns ingestion statistics for the current user.
func (i *Ingester) UserStats(ctx context.Context, req *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	if i.cfg.TSDBEnabled {
		return i.v2UserStats(ctx, req)
	}

	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	state, ok, err := i.userStates.getViaContext(ctx)
	if err != nil {
		return nil, err
	} else if !ok {
		return &client.UserStatsResponse{}, nil
	}

	apiRate := state.ingestedAPISamples.rate()
	ruleRate := state.ingestedRuleSamples.rate()
	return &client.UserStatsResponse{
		IngestionRate:     apiRate + ruleRate,
		ApiIngestionRate:  apiRate,
		RuleIngestionRate: ruleRate,
		NumSeries:         uint64(state.fpToSeries.length()),
	}, nil
}

// AllUserStats returns ingestion statistics for all users known to this ingester.
func (i *Ingester) AllUserStats(ctx context.Context, req *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	if i.cfg.TSDBEnabled {
		return i.v2AllUserStats(ctx, req)
	}

	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	users := i.userStates.cp()

	response := &client.UsersStatsResponse{
		Stats: make([]*client.UserIDStatsResponse, 0, len(users)),
	}
	for userID, state := range users {
		apiRate := state.ingestedAPISamples.rate()
		ruleRate := state.ingestedRuleSamples.rate()
		response.Stats = append(response.Stats, &client.UserIDStatsResponse{
			UserId: userID,
			Data: &client.UserStatsResponse{
				IngestionRate:     apiRate + ruleRate,
				ApiIngestionRate:  apiRate,
				RuleIngestionRate: ruleRate,
				NumSeries:         uint64(state.fpToSeries.length()),
			},
		})
	}
	return response, nil
}

// Check implements the grpc healthcheck
func (i *Ingester) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// Watch implements the grpc healthcheck.
func (i *Ingester) Watch(in *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "Watching is not supported")
}

// ReadinessHandler is used to indicate to k8s when the ingesters are ready for
// the addition removal of another ingester. Returns 204 when the ingester is
// ready, 500 otherwise.
func (i *Ingester) CheckReady(ctx context.Context) error {
	if s := i.State(); s != services.Running {
		return fmt.Errorf("service not Running: %v", s)
	}
	return i.lifecycler.CheckReady(ctx)
}
