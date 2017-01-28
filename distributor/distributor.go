package distributor

import (
	"flag"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/util"
)

var (
	numClientsDesc = prometheus.NewDesc(
		"cortex_distributor_ingester_clients",
		"The current number of ingester clients.",
		nil, nil,
	)
)

// Distributor is a storage.SampleAppender and a cortex.Querier which
// forwards appends and queries to individual ingesters.
type Distributor struct {
	cfg        Config
	ring       ReadRing
	clientsMtx sync.RWMutex
	clients    map[string]ingesterClient
	quit       chan struct{}
	done       chan struct{}

	queryDuration          *prometheus.HistogramVec
	receivedSamples        prometheus.Counter
	sendDuration           *prometheus.HistogramVec
	ingesterAppends        *prometheus.CounterVec
	ingesterAppendFailures *prometheus.CounterVec
	ingesterQueries        *prometheus.CounterVec
	ingesterQueryFailures  *prometheus.CounterVec
}

type ingesterClient struct {
	cortex.IngesterClient
	conn *grpc.ClientConn
}

// ReadRing represents the read inferface to the ring.
type ReadRing interface {
	prometheus.Collector

	Get(key uint32, n int, op ring.Operation) ([]*ring.IngesterDesc, error)
	BatchGet(keys []uint32, n int, op ring.Operation) ([][]*ring.IngesterDesc, error)
	GetAll() []*ring.IngesterDesc
}

// Config contains the configuration require to
// create a Distributor
type Config struct {
	ReplicationFactor   int
	MinReadSuccesses    int
	HeartbeatTimeout    time.Duration
	RemoteTimeout       time.Duration
	ClientCleanupPeriod time.Duration
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	flag.IntVar(&cfg.ReplicationFactor, "distributor.replication-factor", 3, "The number of ingesters to write to and read from.")
	flag.IntVar(&cfg.MinReadSuccesses, "distributor.min-read-successes", 2, "The minimum number of ingesters from which a read must succeed.")
	flag.DurationVar(&cfg.HeartbeatTimeout, "distributor.heartbeat-timeout", time.Minute, "The heartbeat timeout after which ingesters are skipped for reads/writes.")
	flag.DurationVar(&cfg.RemoteTimeout, "distributor.remote-timeout", 2*time.Second, "Timeout for downstream ingesters.")
	flag.DurationVar(&cfg.ClientCleanupPeriod, "distributor.client-cleanup-period", 15*time.Second, "How frequently to clean up clients for ingesters that have gone away.")
}

// New constructs a new Distributor
func New(cfg Config, ring ReadRing) (*Distributor, error) {
	if 0 > cfg.ReplicationFactor {
		return nil, fmt.Errorf("ReplicationFactor must be greater than zero: %d", cfg.ReplicationFactor)
	}
	if cfg.MinReadSuccesses > cfg.ReplicationFactor {
		return nil, fmt.Errorf("MinReadSuccesses > ReplicationFactor: %d > %d", cfg.MinReadSuccesses, cfg.ReplicationFactor)
	}
	d := &Distributor{
		cfg:     cfg,
		ring:    ring,
		clients: map[string]ingesterClient{},
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
		queryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "distributor_query_duration_seconds",
			Help:      "Time spent executing expression queries.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30},
		}, []string{"method", "status_code"}),
		receivedSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_received_samples_total",
			Help:      "The total number of received samples.",
		}),
		sendDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "distributor_send_duration_seconds",
			Help:      "Time spent sending a sample batch to multiple replicated ingesters.",
			Buckets:   []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"method", "status_code"}),
		ingesterAppends: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_ingester_appends_total",
			Help:      "The total number of batch appends sent to ingesters.",
		}, []string{"ingester"}),
		ingesterAppendFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_ingester_append_failures_total",
			Help:      "The total number of failed batch appends sent to ingesters.",
		}, []string{"ingester"}),
		ingesterQueries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_ingester_queries_total",
			Help:      "The total number of queries sent to ingesters.",
		}, []string{"ingester"}),
		ingesterQueryFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_ingester_query_failures_total",
			Help:      "The total number of failed queries sent to ingesters.",
		}, []string{"ingester"}),
	}
	prometheus.MustRegister(d)
	go d.Run()
	return d, nil
}

// Run starts the distributor's maintenance loop.
func (d *Distributor) Run() {
	cleanupClients := time.NewTicker(d.cfg.ClientCleanupPeriod)
	for {
		select {
		case <-cleanupClients.C:
			d.removeStaleIngesterClients()
		case <-d.quit:
			close(d.done)
			return
		}
	}
}

// Stop stops the distributor's maintenance loop.
func (d *Distributor) Stop() {
	close(d.quit)
	<-d.done
}

func (d *Distributor) removeStaleIngesterClients() {
	d.clientsMtx.Lock()
	defer d.clientsMtx.Unlock()

	ingesters := map[string]struct{}{}
	for _, ing := range d.ring.GetAll() {
		ingesters[ing.Addr] = struct{}{}
	}

	for addr, client := range d.clients {
		if _, ok := ingesters[addr]; ok {
			continue
		}
		log.Info("Removing stale ingester client for ", addr)
		delete(d.clients, addr)

		// Do the gRPC closing in the background since it might take a while and
		// we're holding a mutex.
		go func(addr string, conn *grpc.ClientConn) {
			if err := conn.Close(); err != nil {
				log.Errorf("Error closing connection to ingester %q: %v", addr, err)
			}
		}(addr, client.conn)
	}
}

func (d *Distributor) getClientFor(ingester *ring.IngesterDesc) (cortex.IngesterClient, error) {
	d.clientsMtx.RLock()
	client, ok := d.clients[ingester.Addr]
	d.clientsMtx.RUnlock()
	if ok {
		return client, nil
	}

	d.clientsMtx.Lock()
	defer d.clientsMtx.Unlock()
	client, ok = d.clients[ingester.Addr]
	if ok {
		return client, nil
	}

	conn, err := grpc.Dial(
		ingester.Addr,
		grpc.WithTimeout(d.cfg.RemoteTimeout),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
		)),
	)
	if err != nil {
		return nil, err
	}

	client = ingesterClient{
		IngesterClient: cortex.NewIngesterClient(conn),
		conn:           conn,
	}
	d.clients[ingester.Addr] = client
	return client, nil
}

func tokenForMetric(userID string, metric model.Metric) uint32 {
	name := metric[model.MetricNameLabel]
	return tokenFor(userID, name)
}

func tokenFor(userID string, name model.LabelValue) uint32 {
	h := fnv.New32()
	h.Write([]byte(userID))
	h.Write([]byte(name))
	return h.Sum32()
}

type sampleTracker struct {
	sample     *model.Sample
	minSuccess int
	succeeded  int32
}

// Push implements cortex.IngesterServer
func (d *Distributor) Push(ctx context.Context, req *remote.WriteRequest) (*cortex.WriteResponse, error) {
	userID, err := user.GetID(ctx)
	if err != nil {
		return nil, err
	}

	samples := util.FromWriteRequest(req)
	d.receivedSamples.Add(float64(len(samples)))

	keys := make([]uint32, len(samples), len(samples))
	for i, sample := range samples {
		keys[i] = tokenForMetric(userID, sample.Metric)
	}

	ingesters, err := d.ring.BatchGet(keys, d.cfg.ReplicationFactor, ring.Write)
	if err != nil {
		return nil, err
	}

	sampleTrackers := make([]sampleTracker, len(samples), len(samples))
	samplesByIngester := map[*ring.IngesterDesc][]*sampleTracker{}
	for i := range samples {
		sampleTrackers[i] = sampleTracker{
			sample: samples[i],
			// We need a response from a quorum of ingesters, which is n/2 + 1.
			minSuccess: (len(ingesters[i]) / 2) + 1,
			succeeded:  0,
		}

		// Skip those that have not heartbeated in a while. NB these are still
		// included in the calculation of minSuccess, so if too many failed ingesters
		// will cause the whole write to fail.
		liveIngesters := make([]*ring.IngesterDesc, 0, len(ingesters[i]))
		for _, ingester := range ingesters[i] {
			if time.Now().Sub(time.Unix(ingester.Timestamp, 0)) <= d.cfg.HeartbeatTimeout {
				liveIngesters = append(liveIngesters, ingester)
			}
		}

		// This is just a shortcut - if there are not minSuccess available ingesters,
		// after filtering out dead ones, don't even both trying.
		if len(liveIngesters) < sampleTrackers[i].minSuccess {
			return nil, fmt.Errorf("wanted at least %d live ingesters to process write, had %d",
				sampleTrackers[i].minSuccess, len(liveIngesters))
		}

		for _, liveIngester := range liveIngesters {
			sampleForIngester := samplesByIngester[liveIngester]
			samplesByIngester[liveIngester] = append(sampleForIngester, &sampleTrackers[i])
		}
	}

	errs := make(chan error)
	for hostname, samples := range samplesByIngester {
		go func(ingester *ring.IngesterDesc, samples []*sampleTracker) {
			errs <- d.sendSamples(ctx, ingester, samples)
		}(hostname, samples)
	}
	var lastErr error
	for i := 0; i < len(samplesByIngester); i++ {
		if err := <-errs; err != nil {
			lastErr = err
			continue
		}
	}
	for i := range sampleTrackers {
		if sampleTrackers[i].succeeded < int32(sampleTrackers[i].minSuccess) {
			return nil, fmt.Errorf("need %d successful writes, only got %d, last error was: %v",
				sampleTrackers[i].minSuccess, sampleTrackers[i].succeeded, lastErr)
		}
	}
	return &cortex.WriteResponse{}, nil
}

func (d *Distributor) sendSamples(ctx context.Context, ingester *ring.IngesterDesc, sampleTrackers []*sampleTracker) error {
	client, err := d.getClientFor(ingester)
	if err != nil {
		return err
	}
	samples := make([]*model.Sample, len(sampleTrackers), len(sampleTrackers))
	for i := range sampleTrackers {
		samples[i] = sampleTrackers[i].sample
	}
	err = instrument.TimeRequestHistogram(ctx, "Distributor.sendSamples", d.sendDuration, func(ctx context.Context) error {
		_, err := client.Push(ctx, util.ToWriteRequest(samples))
		return err
	})
	if err != nil {
		d.ingesterAppendFailures.WithLabelValues(ingester.Addr).Inc()
	}
	d.ingesterAppends.WithLabelValues(ingester.Addr).Inc()
	for i := range sampleTrackers {
		atomic.AddInt32(&sampleTrackers[i].succeeded, 1)
	}
	return err
}

func metricNameFromLabelMatchers(matchers ...*metric.LabelMatcher) (model.LabelValue, error) {
	for _, m := range matchers {
		if m.Name == model.MetricNameLabel {
			if m.Type != metric.Equal {
				return "", fmt.Errorf("non-equality matchers are not supported on the metric name")
			}
			return m.Value, nil
		}
	}
	return "", fmt.Errorf("no metric name matcher found")
}

// Query implements Querier.
func (d *Distributor) Query(ctx context.Context, from, to model.Time, matchers ...*metric.LabelMatcher) (model.Matrix, error) {
	var result model.Matrix
	err := instrument.TimeRequestHistogram(ctx, "Distributor.Query", d.queryDuration, func(ctx context.Context) error {
		fpToSampleStream := map[model.Fingerprint]*model.SampleStream{}

		metricName, err := metricNameFromLabelMatchers(matchers...)
		if err != nil {
			return err
		}

		userID, err := user.GetID(ctx)
		if err != nil {
			return err
		}

		ingesters, err := d.ring.Get(tokenFor(userID, metricName), d.cfg.ReplicationFactor, ring.Read)
		if err != nil {
			return err
		}

		if len(ingesters) < d.cfg.MinReadSuccesses {
			return fmt.Errorf("could only find %d ingesters for query. Need at least %d", len(ingesters), d.cfg.MinReadSuccesses)
		}

		// Fetch samples from multiple ingesters and group them by fingerprint (unsorted
		// and with overlap).
		successes := 0
		var lastErr error
		for _, ing := range ingesters {
			client, err := d.getClientFor(ing)
			if err != nil {
				return err
			}

			req, err := util.ToQueryRequest(from, to, matchers)
			if err != nil {
				return err
			}

			resp, err := client.Query(ctx, req)
			d.ingesterQueries.WithLabelValues(ing.Addr).Inc()
			if err != nil {
				lastErr = err
				d.ingesterQueryFailures.WithLabelValues(ing.Addr).Inc()
				continue
			}
			successes++

			for _, ss := range util.FromQueryResponse(resp) {
				fp := ss.Metric.Fingerprint()
				if mss, ok := fpToSampleStream[fp]; !ok {
					fpToSampleStream[fp] = &model.SampleStream{
						Metric: ss.Metric,
						Values: ss.Values,
					}
				} else {
					mss.Values = util.MergeSamples(fpToSampleStream[fp].Values, ss.Values)
				}
			}
		}

		if successes < d.cfg.MinReadSuccesses {
			return fmt.Errorf("too few successful reads, last error was: %v", lastErr)
		}

		result = make(model.Matrix, 0, len(fpToSampleStream))
		for _, ss := range fpToSampleStream {
			result = append(result, ss)
		}

		return nil
	})
	return result, err
}

// forAllIngesters runs f, in parallel, for all ingesters
func (d *Distributor) forAllIngesters(f func(cortex.IngesterClient) (interface{}, error)) ([]interface{}, error) {
	resps, errs := make(chan interface{}), make(chan error)
	ingesters := d.ring.GetAll()
	for _, ingester := range ingesters {
		go func(ingester *ring.IngesterDesc) {
			client, err := d.getClientFor(ingester)
			if err != nil {
				errs <- err
				return
			}

			resp, err := f(client)
			if err != nil {
				errs <- err
			} else {
				resps <- resp
			}
		}(ingester)
	}

	var lastErr error
	result, numErrs := []interface{}{}, 0
	for range ingesters {
		select {
		case resp := <-resps:
			result = append(result, resp)
		case lastErr = <-errs:
			numErrs++
		}
	}
	if numErrs > (d.cfg.ReplicationFactor - d.cfg.MinReadSuccesses) {
		return nil, lastErr
	}
	return result, nil
}

// LabelValuesForLabelName returns all of the label values that are associated with a given label name.
func (d *Distributor) LabelValuesForLabelName(ctx context.Context, labelName model.LabelName) (model.LabelValues, error) {
	req := &cortex.LabelValuesRequest{
		LabelName: string(labelName),
	}
	resps, err := d.forAllIngesters(func(client cortex.IngesterClient) (interface{}, error) {
		return client.LabelValues(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	valueSet := map[model.LabelValue]struct{}{}
	for _, resp := range resps {
		for _, v := range resp.(*cortex.LabelValuesResponse).LabelValues {
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
func (d *Distributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...metric.LabelMatchers) ([]metric.Metric, error) {
	req, err := util.ToMetricsForLabelMatchersRequest(from, through, matchers)
	if err != nil {
		return nil, err
	}

	resps, err := d.forAllIngesters(func(client cortex.IngesterClient) (interface{}, error) {
		return client.MetricsForLabelMatchers(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	metrics := map[model.Fingerprint]model.Metric{}
	for _, resp := range resps {
		ms := util.FromMetricsForLabelMatchersResponse(resp.(*cortex.MetricsForLabelMatchersResponse))
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
	req := &cortex.UserStatsRequest{}
	resps, err := d.forAllIngesters(func(client cortex.IngesterClient) (interface{}, error) {
		return client.UserStats(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	totalStats := &UserStats{}
	for _, resp := range resps {
		totalStats.IngestionRate += resp.(*cortex.UserStatsResponse).IngestionRate
		totalStats.NumSeries += resp.(*cortex.UserStatsResponse).NumSeries
	}

	totalStats.IngestionRate /= float64(d.cfg.ReplicationFactor)
	totalStats.NumSeries /= uint64(d.cfg.ReplicationFactor)

	return totalStats, nil
}

// Describe implements prometheus.Collector.
func (d *Distributor) Describe(ch chan<- *prometheus.Desc) {
	d.queryDuration.Describe(ch)
	ch <- d.receivedSamples.Desc()
	d.sendDuration.Describe(ch)
	d.ring.Describe(ch)
	ch <- numClientsDesc
	d.ingesterAppends.Describe(ch)
	d.ingesterAppendFailures.Describe(ch)
	d.ingesterQueries.Describe(ch)
	d.ingesterQueryFailures.Describe(ch)
}

// Collect implements prometheus.Collector.
func (d *Distributor) Collect(ch chan<- prometheus.Metric) {
	d.queryDuration.Collect(ch)
	ch <- d.receivedSamples
	d.sendDuration.Collect(ch)
	d.ring.Collect(ch)
	d.ingesterAppends.Collect(ch)
	d.ingesterAppendFailures.Collect(ch)
	d.ingesterQueries.Collect(ch)
	d.ingesterQueryFailures.Collect(ch)
	d.clientsMtx.RLock()
	defer d.clientsMtx.RUnlock()
	ch <- prometheus.MustNewConstMetric(
		numClientsDesc,
		prometheus.GaugeValue,
		float64(len(d.clients)),
	)
}
