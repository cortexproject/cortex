package distributor

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weaveworks/scope/common/instrument"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/user"
	"github.com/weaveworks/cortex/util"
	"github.com/weaveworks/cortex/util/middleware"
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
	clientsMtx sync.RWMutex
	clients    map[string]cortex.IngesterClient

	queryDuration          *prometheus.HistogramVec
	receivedSamples        prometheus.Counter
	sendDuration           *prometheus.HistogramVec
	ingesterAppends        *prometheus.CounterVec
	ingesterAppendFailures *prometheus.CounterVec
	ingesterQueries        *prometheus.CounterVec
	ingesterQueryFailures  *prometheus.CounterVec
}

// ReadRing represents the read inferface to the ring.
type ReadRing interface {
	prometheus.Collector

	Get(key uint32, n int, op ring.Operation) ([]ring.IngesterDesc, error)
	BatchGet(keys []uint32, n int, op ring.Operation) ([][]ring.IngesterDesc, error)
	GetAll() []ring.IngesterDesc
}

// Config contains the configuration require to
// create a Distributor
type Config struct {
	Ring ReadRing

	ReplicationFactor int
	MinReadSuccesses  int
	HeartbeatTimeout  time.Duration
	RemoteTimeout     time.Duration
}

// New constructs a new Distributor
func New(cfg Config) (*Distributor, error) {
	if 0 > cfg.ReplicationFactor {
		return nil, fmt.Errorf("ReplicationFactor must be greater than zero: %d", cfg.ReplicationFactor)
	}
	if cfg.MinReadSuccesses > cfg.ReplicationFactor {
		return nil, fmt.Errorf("MinReadSuccesses > ReplicationFactor: %d > %d", cfg.MinReadSuccesses, cfg.ReplicationFactor)
	}
	return &Distributor{
		cfg:     cfg,
		clients: map[string]cortex.IngesterClient{},
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
	}, nil
}

func (d *Distributor) getClientFor(ingester ring.IngesterDesc) (cortex.IngesterClient, error) {
	d.clientsMtx.RLock()
	client, ok := d.clients[ingester.Hostname]
	d.clientsMtx.RUnlock()
	if ok {
		return client, nil
	}

	d.clientsMtx.Lock()
	defer d.clientsMtx.Unlock()
	client, ok = d.clients[ingester.Hostname]
	if ok {
		return client, nil
	}

	if ingester.GRPCHostname != "" {
		conn, err := grpc.Dial(
			ingester.GRPCHostname,
			grpc.WithInsecure(),
			grpc.WithUnaryInterceptor(middleware.ClientUserHeaderInterceptor),
		)
		if err != nil {
			return nil, err
		}
		client = cortex.NewIngesterClient(conn)
	} else {
		var err error
		client, err = NewHTTPIngesterClient(ingester.Hostname, d.cfg.RemoteTimeout)
		if err != nil {
			return nil, err
		}
	}

	d.clients[ingester.Hostname] = client
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

	ingesters, err := d.cfg.Ring.BatchGet(keys, d.cfg.ReplicationFactor, ring.Write)
	if err != nil {
		return nil, err
	}

	sampleTrackers := make([]sampleTracker, len(samples), len(samples))
	samplesByIngester := map[ring.IngesterDesc][]*sampleTracker{}
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
		liveIngesters := make([]ring.IngesterDesc, 0, len(ingesters[i]))
		for _, ingester := range ingesters[i] {
			if time.Now().Sub(ingester.Timestamp) <= d.cfg.HeartbeatTimeout {
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
		go func(ingester ring.IngesterDesc, samples []*sampleTracker) {
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

func (d *Distributor) sendSamples(ctx context.Context, ingester ring.IngesterDesc, sampleTrackers []*sampleTracker) error {
	client, err := d.getClientFor(ingester)
	if err != nil {
		return err
	}
	samples := make([]*model.Sample, len(sampleTrackers), len(sampleTrackers))
	for i := range sampleTrackers {
		samples[i] = sampleTrackers[i].sample
	}
	err = instrument.TimeRequestHistogram("send", d.sendDuration, func() error {
		_, err := client.Push(ctx, util.ToWriteRequest(samples))
		return err
	})
	if err != nil {
		d.ingesterAppendFailures.WithLabelValues(ingester.Hostname).Inc()
	}
	d.ingesterAppends.WithLabelValues(ingester.Hostname).Inc()
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
	err := instrument.TimeRequestHistogram("duration", d.queryDuration, func() error {
		fpToSampleStream := map[model.Fingerprint]*model.SampleStream{}

		metricName, err := metricNameFromLabelMatchers(matchers...)
		if err != nil {
			return err
		}

		userID, err := user.GetID(ctx)
		if err != nil {
			return err
		}

		ingesters, err := d.cfg.Ring.Get(tokenFor(userID, metricName), d.cfg.ReplicationFactor, ring.Read)
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

			req, err := util.ToQueryRequest(from, to, matchers...)
			if err != nil {
				return err
			}

			resp, err := client.Query(ctx, req)
			d.ingesterQueries.WithLabelValues(ing.Hostname).Inc()
			if err != nil {
				lastErr = err
				d.ingesterQueryFailures.WithLabelValues(ing.Hostname).Inc()
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

// LabelValuesForLabelName returns all of the label values that are associated with a given label name.
func (d *Distributor) LabelValuesForLabelName(ctx context.Context, labelName model.LabelName) (model.LabelValues, error) {
	valueSet := map[model.LabelValue]struct{}{}
	for _, ingester := range d.cfg.Ring.GetAll() {
		client, err := d.getClientFor(ingester)
		if err != nil {
			return nil, err
		}
		resp, err := client.LabelValues(ctx, &cortex.LabelValuesRequest{
			LabelName: string(labelName),
		})
		if err != nil {
			return nil, err
		}
		for _, v := range resp.LabelValues {
			valueSet[model.LabelValue(v)] = struct{}{}
		}
	}

	values := make(model.LabelValues, 0, len(valueSet))
	for v := range valueSet {
		values = append(values, v)
	}
	return values, nil
}

// UserStats returns statistics about the current user.
func (d *Distributor) UserStats(ctx context.Context) (*UserStats, error) {
	totalStats := &UserStats{}
	for _, ingester := range d.cfg.Ring.GetAll() {
		client, err := d.getClientFor(ingester)
		if err != nil {
			return nil, err
		}
		resp, err := client.UserStats(ctx, &cortex.UserStatsRequest{})
		if err != nil {
			return nil, err
		}
		totalStats.IngestionRate += resp.IngestionRate
		totalStats.NumSeries += resp.NumSeries
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
	d.cfg.Ring.Describe(ch)
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
	d.cfg.Ring.Collect(ch)
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
