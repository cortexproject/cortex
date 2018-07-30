package distributor

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/go-kit/kit/log/level"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"

	billing "github.com/weaveworks/billing-client"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	ingester_client "github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/prom1/storage/metric"
	"github.com/weaveworks/cortex/pkg/ring"
	"github.com/weaveworks/cortex/pkg/util"
	"github.com/weaveworks/cortex/pkg/util/extract"
	"github.com/weaveworks/cortex/pkg/util/validation"
)

var (
	queryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "distributor_query_duration_seconds",
		Help:      "Time spent executing expression queries.",
		Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30},
	}, []string{"method", "status_code"})
	receivedSamples = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "distributor_received_samples_total",
		Help:      "The total number of received samples.",
	}, []string{"user"})
	sendDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "distributor_send_duration_seconds",
		Help:      "Time spent sending a sample batch to multiple replicated ingesters.",
		Buckets:   []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"method", "status_code"})
	ingesterAppends = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "distributor_ingester_appends_total",
		Help:      "The total number of batch appends sent to ingesters.",
	}, []string{"ingester"})
	ingesterAppendFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "distributor_ingester_append_failures_total",
		Help:      "The total number of failed batch appends sent to ingesters.",
	}, []string{"ingester"})
	ingesterQueries = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "distributor_ingester_queries_total",
		Help:      "The total number of queries sent to ingesters.",
	}, []string{"ingester"})
	ingesterQueryFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "distributor_ingester_query_failures_total",
		Help:      "The total number of failed queries sent to ingesters.",
	}, []string{"ingester"})
	replicationFactor = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "distributor_replication_factor",
		Help:      "The configured replication factor.",
	})
)

// Distributor is a storage.SampleAppender and a client.Querier which
// forwards appends and queries to individual ingesters.
type Distributor struct {
	cfg          Config
	ring         ring.ReadRing
	ingesterPool *ingester_client.Pool

	billingClient *billing.Client

	// Per-user rate limiters.
	ingestLimitersMtx sync.Mutex
	ingestLimiters    map[string]*rate.Limiter
}

// Config contains the configuration require to
// create a Distributor
type Config struct {
	EnableBilling        bool
	BillingConfig        billing.Config
	IngesterClientConfig ingester_client.Config
	PoolConfig           ingester_client.PoolConfig
	validationConfig     validation.Config

	RemoteTimeout      time.Duration
	IngestionRateLimit float64
	IngestionBurstSize int

	ShardByAllLabels bool

	// for testing
	ingesterClientFactory client.Factory
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.BillingConfig.RegisterFlags(f)
	cfg.IngesterClientConfig.RegisterFlags(f)
	cfg.PoolConfig.RegisterFlags(f)
	cfg.validationConfig.RegisterFlags(f)

	f.BoolVar(&cfg.EnableBilling, "distributor.enable-billing", false, "Report number of ingested samples to billing system.")
	f.DurationVar(&cfg.RemoteTimeout, "distributor.remote-timeout", 2*time.Second, "Timeout for downstream ingesters.")
	f.Float64Var(&cfg.IngestionRateLimit, "distributor.ingestion-rate-limit", 25000, "Per-user ingestion rate limit in samples per second.")
	f.IntVar(&cfg.IngestionBurstSize, "distributor.ingestion-burst-size", 50000, "Per-user allowed ingestion burst size (in number of samples).")
	f.BoolVar(&cfg.ShardByAllLabels, "distributor.shard-by-all-labels", false, "Distribute samples based on all labels, as opposed to solely by user and metric name.")
}

// New constructs a new Distributor
func New(cfg Config, ring ring.ReadRing) (*Distributor, error) {
	if cfg.ingesterClientFactory == nil {
		cfg.ingesterClientFactory = func(addr string) (grpc_health_v1.HealthClient, error) {
			return ingester_client.MakeIngesterClient(addr, cfg.IngesterClientConfig)
		}
	}

	var billingClient *billing.Client
	if cfg.EnableBilling {
		var err error
		billingClient, err = billing.NewClient(cfg.BillingConfig)
		if err != nil {
			return nil, err
		}
	}

	replicationFactor.Set(float64(ring.ReplicationFactor()))
	cfg.PoolConfig.RemoteTimeout = cfg.RemoteTimeout

	d := &Distributor{
		cfg:            cfg,
		ring:           ring,
		ingesterPool:   ingester_client.NewPool(cfg.PoolConfig, ring, cfg.ingesterClientFactory, util.Logger),
		billingClient:  billingClient,
		ingestLimiters: map[string]*rate.Limiter{},
	}
	return d, nil
}

// Stop stops the distributor's maintenance loop.
func (d *Distributor) Stop() {
	d.ingesterPool.Stop()
}

func (d *Distributor) tokenForLabels(userID string, labels []client.LabelPair) (uint32, error) {
	if d.cfg.ShardByAllLabels {
		return shardByAllLabels(userID, labels)
	}

	metricName, err := extract.MetricNameFromLabelPairs(labels)
	if err != nil {
		return 0, err
	}
	return shardByMetricName(userID, metricName), nil
}

func shardByMetricName(userID string, metricName []byte) uint32 {
	h := fnv.New32()
	h.Write([]byte(userID))
	h.Write(metricName)
	return h.Sum32()
}

func shardByAllLabels(userID string, labels []client.LabelPair) (uint32, error) {
	h := fnv.New32()
	h.Write([]byte(userID))
	lastLabelName := []byte{}
	for _, label := range labels {
		if bytes.Compare(lastLabelName, label.Name) >= 0 {
			return 0, fmt.Errorf("Labels not sorted")
		}
		h.Write(label.Name)
		h.Write(label.Value)
	}
	return h.Sum32(), nil
}

type sampleTracker struct {
	labels      []client.LabelPair
	samples     []client.Sample
	minSuccess  int
	maxFailures int
	succeeded   int32
	failed      int32
}

type pushTracker struct {
	rpcsPending int32
	rpcsFailed  int32
	done        chan struct{}
	err         chan error
}

// Push implements client.IngesterServer
func (d *Distributor) Push(ctx context.Context, req *client.WriteRequest) (*client.WriteResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	var lastPartialErr error

	// Build slice of sampleTrackers, one per timeseries.
	sampleTrackers := make([]sampleTracker, 0, len(req.Timeseries))
	keys := make([]uint32, 0, len(req.Timeseries))
	numSamples := 0
	for _, ts := range req.Timeseries {
		key, err := d.tokenForLabels(userID, ts.Labels)
		if err != nil {
			return nil, err
		}

		if err := d.cfg.validationConfig.ValidateLabels(ts.Labels); err != nil {
			level.Error(util.WithContext(ctx, util.Logger)).Log("msg", "error validating sample", "err", err)
			lastPartialErr = err
			continue
		}

		metricName, _ := extract.MetricNameFromLabelPairs(ts.Labels)
		for _, s := range ts.Samples {
			if err := d.cfg.validationConfig.ValidateSample(metricName, s); err != nil {
				level.Error(util.WithContext(ctx, util.Logger)).Log("msg", "error validating sample", "err", err)
				lastPartialErr = err
				continue
			}
		}

		keys = append(keys, key)
		sampleTrackers = append(sampleTrackers, sampleTracker{
			labels:  ts.Labels,
			samples: ts.Samples,
		})
		numSamples += len(ts.Samples)
	}
	receivedSamples.WithLabelValues(userID).Add(float64(numSamples))

	if len(sampleTrackers) == 0 {
		return &client.WriteResponse{}, lastPartialErr
	}

	limiter := d.getOrCreateIngestLimiter(userID)
	if !limiter.AllowN(time.Now(), numSamples) {
		// Return a 4xx here to have the client discard the data and not retry. If a client
		// is sending too much data consistently we will unlikely ever catch up otherwise.
		return nil, httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (%v) exceeded while adding %d samples", limiter.Limit(), numSamples)
	}

	replicationSets, err := d.ring.BatchGet(keys, ring.Write)
	if err != nil {
		return nil, err
	}

	samplesByIngester := map[*ring.IngesterDesc][]*sampleTracker{}
	for i, replicationSet := range replicationSets {
		sampleTrackers[i].minSuccess = len(replicationSet.Ingesters) - replicationSet.MaxErrors
		sampleTrackers[i].maxFailures = replicationSet.MaxErrors
		for _, ingester := range replicationSet.Ingesters {
			samplesByIngester[ingester] = append(samplesByIngester[ingester], &sampleTrackers[i])
		}
	}

	pushTracker := pushTracker{
		rpcsPending: int32(len(sampleTrackers)),
		done:        make(chan struct{}),
		err:         make(chan error),
	}
	for ingester, sampleTrackers := range samplesByIngester {
		go func(ingester *ring.IngesterDesc, sampleTrackers []*sampleTracker) {
			// Use a background context to make sure all ingesters get samples even if we return early
			localCtx, cancel := context.WithTimeout(context.Background(), d.cfg.RemoteTimeout)
			defer cancel()
			localCtx = user.InjectOrgID(localCtx, userID)
			if sp := opentracing.SpanFromContext(ctx); sp != nil {
				localCtx = opentracing.ContextWithSpan(localCtx, sp)
			}
			d.sendSamples(localCtx, ingester, sampleTrackers, &pushTracker)
		}(ingester, sampleTrackers)
	}
	select {
	case err := <-pushTracker.err:
		return nil, err
	case <-pushTracker.done:
		return &client.WriteResponse{}, lastPartialErr
	}
}

func (d *Distributor) getOrCreateIngestLimiter(userID string) *rate.Limiter {
	d.ingestLimitersMtx.Lock()
	defer d.ingestLimitersMtx.Unlock()

	if limiter, ok := d.ingestLimiters[userID]; ok {
		return limiter
	}

	limiter := rate.NewLimiter(rate.Limit(d.cfg.IngestionRateLimit), d.cfg.IngestionBurstSize)
	d.ingestLimiters[userID] = limiter
	return limiter
}

func (d *Distributor) sendSamples(ctx context.Context, ingester *ring.IngesterDesc, sampleTrackers []*sampleTracker, pushTracker *pushTracker) {
	err := d.sendSamplesErr(ctx, ingester, sampleTrackers)

	// If we succeed, decrement each sample's pending count by one.  If we reach
	// the required number of successful puts on this sample, then decrement the
	// number of pending samples by one.  If we successfully push all samples to
	// min success ingesters, wake up the waiting rpc so it can return early.
	// Similarly, track the number of errors, and if it exceeds maxFailures
	// shortcut the waiting rpc.
	//
	// The use of atomic increments here guarantees only a single sendSamples
	// goroutine will write to either channel.
	for i := range sampleTrackers {
		if err != nil {
			if atomic.AddInt32(&sampleTrackers[i].failed, 1) <= int32(sampleTrackers[i].maxFailures) {
				continue
			}
			if atomic.AddInt32(&pushTracker.rpcsFailed, 1) == 1 {
				pushTracker.err <- err
			}
		} else {
			if atomic.AddInt32(&sampleTrackers[i].succeeded, 1) != int32(sampleTrackers[i].minSuccess) {
				continue
			}
			if atomic.AddInt32(&pushTracker.rpcsPending, -1) == 0 {
				pushTracker.done <- struct{}{}
			}
		}
	}
}

func (d *Distributor) sendSamplesErr(ctx context.Context, ingester *ring.IngesterDesc, samples []*sampleTracker) error {
	h, err := d.ingesterPool.GetClientFor(ingester.Addr)
	if err != nil {
		return err
	}
	c := h.(ingester_client.IngesterClient)

	req := client.WriteRequest{
		Timeseries: make([]client.PreallocTimeseries, len(samples)),
	}
	for i, s := range samples {
		req.Timeseries[i].Labels = s.labels
		req.Timeseries[i].Samples = s.samples
	}

	_, err = c.Push(ctx, &req)

	ingesterAppends.WithLabelValues(ingester.Addr).Inc()
	if err != nil {
		ingesterAppendFailures.WithLabelValues(ingester.Addr).Inc()
	}
	return err
}

// Query implements Querier.
func (d *Distributor) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	var matrix model.Matrix
	err := instrument.TimeRequestHistogram(ctx, "Distributor.Query", queryDuration, func(ctx context.Context) error {
		userID, err := user.ExtractOrgID(ctx)
		if err != nil {
			return err
		}

		metricNameMatcher, _, ok := extract.MetricNameMatcherFromMatchers(matchers)

		req, err := ingester_client.ToQueryRequest(from, to, matchers)
		if err != nil {
			return err
		}

		// Get ingesters by metricName if one exists, otherwise get all ingesters
		var replicationSet ring.ReplicationSet
		if !d.cfg.ShardByAllLabels && ok && metricNameMatcher.Type == labels.MatchEqual {
			replicationSet, err = d.ring.Get(shardByMetricName(userID, []byte(metricNameMatcher.Value)), ring.Read)
		} else {
			replicationSet, err = d.ring.GetAll()
		}
		if err != nil {
			return promql.ErrStorage(err)
		}

		matrix, err = d.queryIngesters(ctx, replicationSet, req)
		return promql.ErrStorage(err)
	})
	return matrix, err
}

// Query implements Querier.
func (d *Distributor) queryIngesters(ctx context.Context, replicationSet ring.ReplicationSet, req *client.QueryRequest) (model.Matrix, error) {
	// Fetch samples from multiple ingesters
	errs := make(chan error, len(replicationSet.Ingesters))
	results := make(chan model.Matrix, len(replicationSet.Ingesters))
	for _, ing := range replicationSet.Ingesters {
		go func(ing *ring.IngesterDesc) {
			result, err := d.queryIngester(ctx, ing, req)
			if err != nil {
				errs <- err
			} else {
				results <- result
			}
		}(ing)
	}

	// Only wait for minSuccessful responses (or maxErrors), and accumulate the samples
	// by fingerprint, merging them into any existing samples.
	fpToSampleStream := map[model.Fingerprint]*model.SampleStream{}
	minSuccess := len(replicationSet.Ingesters) - replicationSet.MaxErrors
	var numErrs, numSuccess int
	for numSuccess < minSuccess {
		select {
		case err := <-errs:
			numErrs++
			if numErrs > replicationSet.MaxErrors {
				return nil, err
			}

		case result := <-results:
			numSuccess++
			for _, ss := range result {
				fp := ss.Metric.Fingerprint()
				mss, ok := fpToSampleStream[fp]
				if !ok {
					mss = &model.SampleStream{
						Metric: ss.Metric,
					}
					fpToSampleStream[fp] = mss
				}
				mss.Values = util.MergeSampleSets(mss.Values, ss.Values)
			}
		}
	}

	result := model.Matrix{}
	for _, ss := range fpToSampleStream {
		result = append(result, ss)
	}
	return result, nil
}

func (d *Distributor) queryIngester(ctx context.Context, ing *ring.IngesterDesc, req *client.QueryRequest) (model.Matrix, error) {
	client, err := d.ingesterPool.GetClientFor(ing.Addr)
	if err != nil {
		return nil, err
	}

	resp, err := client.(ingester_client.IngesterClient).Query(ctx, req)
	ingesterQueries.WithLabelValues(ing.Addr).Inc()
	if err != nil {
		ingesterQueryFailures.WithLabelValues(ing.Addr).Inc()
		return nil, err
	}

	return ingester_client.FromQueryResponse(resp), nil
}

// forAllIngesters runs f, in parallel, for all ingesters
func (d *Distributor) forAllIngesters(f func(client.IngesterClient) (interface{}, error)) ([]interface{}, error) {
	replicationSet, err := d.ring.GetAll()
	if err != nil {
		return nil, err
	}

	resps, errs := make(chan interface{}), make(chan error)
	for _, ingester := range replicationSet.Ingesters {
		go func(ingester *ring.IngesterDesc) {
			client, err := d.ingesterPool.GetClientFor(ingester.Addr)
			if err != nil {
				errs <- err
				return
			}

			resp, err := f(client.(ingester_client.IngesterClient))
			if err != nil {
				errs <- err
			} else {
				resps <- resp
			}
		}(ingester)
	}

	var lastErr error
	result, numErrs := []interface{}{}, 0
	for range replicationSet.Ingesters {
		select {
		case resp := <-resps:
			result = append(result, resp)
		case lastErr = <-errs:
			numErrs++
		}
	}

	if numErrs > replicationSet.MaxErrors {
		return nil, lastErr
	}

	return result, nil
}

// LabelValuesForLabelName returns all of the label values that are associated with a given label name.
func (d *Distributor) LabelValuesForLabelName(ctx context.Context, labelName model.LabelName) (model.LabelValues, error) {
	req := &client.LabelValuesRequest{
		LabelName: string(labelName),
	}
	resps, err := d.forAllIngesters(func(client client.IngesterClient) (interface{}, error) {
		return client.LabelValues(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	valueSet := map[model.LabelValue]struct{}{}
	for _, resp := range resps {
		for _, v := range resp.(*client.LabelValuesResponse).LabelValues {
			valueSet[model.LabelValue(v)] = struct{}{}
		}
	}

	values := make(model.LabelValues, 0, len(valueSet))
	for v := range valueSet {
		values = append(values, v)
	}
	return values, nil
}

// MetricsForLabelMatchers gets the metrics that match said matchers
func (d *Distributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	req, err := ingester_client.ToMetricsForLabelMatchersRequest(from, through, matchers)
	if err != nil {
		return nil, err
	}

	resps, err := d.forAllIngesters(func(client client.IngesterClient) (interface{}, error) {
		return client.MetricsForLabelMatchers(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	metrics := map[model.Fingerprint]model.Metric{}
	for _, resp := range resps {
		ms := ingester_client.FromMetricsForLabelMatchersResponse(resp.(*client.MetricsForLabelMatchersResponse))
		for _, m := range ms {
			metrics[m.Fingerprint()] = m
		}
	}

	result := make([]metric.Metric, 0, len(metrics))
	for _, m := range metrics {
		result = append(result, metric.Metric{
			Metric: m,
		})
	}
	return result, nil
}

// UserStats returns statistics about the current user.
func (d *Distributor) UserStats(ctx context.Context) (*UserStats, error) {
	req := &client.UserStatsRequest{}
	resps, err := d.forAllIngesters(func(client client.IngesterClient) (interface{}, error) {
		return client.UserStats(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	totalStats := &UserStats{}
	for _, resp := range resps {
		totalStats.IngestionRate += resp.(*client.UserStatsResponse).IngestionRate
		totalStats.NumSeries += resp.(*client.UserStatsResponse).NumSeries
	}

	totalStats.IngestionRate /= float64(d.ring.ReplicationFactor())
	totalStats.NumSeries /= uint64(d.ring.ReplicationFactor())

	return totalStats, nil
}

// UserIDStats models ingestion statistics for one user, including the user ID
type UserIDStats struct {
	UserID string `json:"userID"`
	UserStats
}

// AllUserStats returns statistics about all users.
// Note it does not divide by the ReplicationFactor like UserStats()
func (d *Distributor) AllUserStats(ctx context.Context) ([]UserIDStats, error) {
	// Add up by user, across all responses from ingesters
	perUserTotals := make(map[string]UserStats)

	req := &client.UserStatsRequest{}
	ctx = user.InjectOrgID(ctx, "1") // fake: ingester insists on having an org ID
	// Not using d.forAllIngesters(), so we can fail after first error.
	replicationSet, err := d.ring.GetAll()
	if err != nil {
		return nil, err
	}
	for _, ingester := range replicationSet.Ingesters {
		client, err := d.ingesterPool.GetClientFor(ingester.Addr)
		if err != nil {
			return nil, err
		}
		resp, err := client.(ingester_client.IngesterClient).AllUserStats(ctx, req)
		if err != nil {
			return nil, err
		}
		for _, u := range resp.Stats {
			s := perUserTotals[u.UserId]
			s.IngestionRate += u.Data.IngestionRate
			s.NumSeries += u.Data.NumSeries
			perUserTotals[u.UserId] = s
		}
	}

	// Turn aggregated map into a slice for return
	response := make([]UserIDStats, 0, len(perUserTotals))
	for id, stats := range perUserTotals {
		response = append(response, UserIDStats{
			UserID: id,
			UserStats: UserStats{
				IngestionRate: stats.IngestionRate,
				NumSeries:     stats.NumSeries,
			},
		})
	}

	return response, nil
}
