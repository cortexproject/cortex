// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cortex

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/weaveworks/scope/common/instrument"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex/ingester"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/user"
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
	cfg        DistributorConfig
	clientsMtx sync.RWMutex
	clients    map[string]*IngesterClient

	queryDuration          *prometheus.HistogramVec
	receivedSamples        prometheus.Counter
	sendDuration           *prometheus.HistogramVec
	ingesterAppends        *prometheus.CounterVec
	ingesterAppendFailures *prometheus.CounterVec
	ingesterQueries        *prometheus.CounterVec
	ingesterQueryFailures  *prometheus.CounterVec
	ingestersAlive         *prometheus.Desc
}

// ReadRing represents the read inferface to the ring.
type ReadRing interface {
	prometheus.Collector

	Get(key uint32, n int, heartbeatTimeout time.Duration) ([]ring.IngesterDesc, error)
	GetAll(heartbeatTimeout time.Duration) []ring.IngesterDesc
}

// IngesterClientFactory creates ingester clients.
type IngesterClientFactory func(string) (*IngesterClient, error)

// DistributorConfig contains the configuration require to
// create a Distributor
type DistributorConfig struct {
	Ring          ReadRing
	ClientFactory IngesterClientFactory

	ReplicationFactor int
	MinReadSuccesses  int
	MinWriteSuccesses int
	HeartbeatTimeout  time.Duration
}

// NewDistributor constructs a new Distributor
func NewDistributor(cfg DistributorConfig) (*Distributor, error) {
	if 0 > cfg.ReplicationFactor {
		return nil, fmt.Errorf("ReplicationFactor must be greater than zero: %d", cfg.ReplicationFactor)
	}
	if cfg.MinWriteSuccesses > cfg.ReplicationFactor {
		return nil, fmt.Errorf("MinWriteSuccesses > ReplicationFactor: %d > %d", cfg.MinWriteSuccesses, cfg.ReplicationFactor)
	}
	if cfg.MinReadSuccesses > cfg.ReplicationFactor {
		return nil, fmt.Errorf("MinReadSuccesses > ReplicationFactor: %d > %d", cfg.MinReadSuccesses, cfg.ReplicationFactor)
	}
	return &Distributor{
		cfg:     cfg,
		clients: map[string]*IngesterClient{},
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
		ingestersAlive: prometheus.NewDesc(
			"cortex_distributor_ingesters_alive",
			"Number of ingesters in the ring that have heartbeats within timeout.",
			nil, nil,
		),
	}, nil
}

func (d *Distributor) getClientFor(hostname string) (*IngesterClient, error) {
	d.clientsMtx.RLock()
	client, ok := d.clients[hostname]
	d.clientsMtx.RUnlock()
	if ok {
		return client, nil
	}

	d.clientsMtx.Lock()
	defer d.clientsMtx.Unlock()
	client, ok = d.clients[hostname]
	if ok {
		return client, nil
	}

	client, err := d.cfg.ClientFactory(hostname)
	if err != nil {
		return nil, err
	}
	d.clients[hostname] = client
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

// Append implements SampleAppender.
func (d *Distributor) Append(ctx context.Context, samples []*model.Sample) error {
	userID, err := user.GetID(ctx)
	if err != nil {
		return err
	}

	d.receivedSamples.Add(float64(len(samples)))

	samplesByIngester := map[string][]*model.Sample{}
	for _, sample := range samples {
		key := tokenForMetric(userID, sample.Metric)
		ingesters, err := d.cfg.Ring.Get(key, d.cfg.ReplicationFactor, d.cfg.HeartbeatTimeout)
		if err != nil {
			return err
		}
		for _, ingester := range ingesters {
			otherSamples := samplesByIngester[ingester.Hostname]
			samplesByIngester[ingester.Hostname] = append(otherSamples, sample)
		}
	}

	errs := make(chan error)
	for hostname, samples := range samplesByIngester {
		go func(hostname string, samples []*model.Sample) {
			errs <- d.sendSamples(ctx, hostname, samples)
		}(hostname, samples)
	}
	var lastErr error
	successes := 0
	for i := 0; i < len(samplesByIngester); i++ {
		if err := <-errs; err != nil {
			lastErr = err
			continue
		}
		successes++
	}

	if successes < d.cfg.MinWriteSuccesses {
		return fmt.Errorf("too few successful writes, last error was: %v", lastErr)
	}
	return nil
}

func (d *Distributor) sendSamples(ctx context.Context, hostname string, samples []*model.Sample) error {
	client, err := d.getClientFor(hostname)
	if err != nil {
		return err
	}
	err = instrument.TimeRequestHistogram("send", d.sendDuration, func() error {
		return client.Append(ctx, samples)
	})
	if err != nil {
		d.ingesterAppendFailures.WithLabelValues(hostname).Inc()
	}
	d.ingesterAppends.WithLabelValues(hostname).Inc()
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

		ingesters, err := d.cfg.Ring.Get(tokenFor(userID, metricName), d.cfg.ReplicationFactor, d.cfg.HeartbeatTimeout)
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
			client, err := d.getClientFor(ing.Hostname)
			if err != nil {
				return err
			}
			matrix, err := client.Query(ctx, from, to, matchers...)
			d.ingesterQueries.WithLabelValues(ing.Hostname).Inc()
			if err != nil {
				lastErr = err
				d.ingesterQueryFailures.WithLabelValues(ing.Hostname).Inc()
				continue
			}
			successes++

			for _, ss := range matrix {
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
	for _, c := range d.cfg.Ring.GetAll(d.cfg.HeartbeatTimeout) {
		client, err := d.getClientFor(c.Hostname)
		if err != nil {
			return nil, err
		}
		vals, err := client.LabelValuesForLabelName(ctx, labelName)
		if err != nil {
			return nil, err
		}
		for _, v := range vals {
			valueSet[v] = struct{}{}
		}
	}

	values := make(model.LabelValues, 0, len(valueSet))
	for v := range valueSet {
		values = append(values, v)
	}
	return values, nil
}

// UserStats returns statistics about the current user.
func (d *Distributor) UserStats(ctx context.Context) (*ingester.UserStats, error) {
	totalStats := &ingester.UserStats{}
	for _, c := range d.cfg.Ring.GetAll(d.cfg.HeartbeatTimeout) {
		client, err := d.getClientFor(c.Hostname)
		if err != nil {
			return nil, err
		}
		stats, err := client.UserStats(ctx)
		if err != nil {
			return nil, err
		}
		totalStats.IngestionRate += stats.IngestionRate
		totalStats.NumSeries += stats.NumSeries
	}

	totalStats.IngestionRate /= float64(d.cfg.ReplicationFactor)
	totalStats.NumSeries /= uint64(d.cfg.ReplicationFactor)

	return totalStats, nil
}

// NeedsThrottling implements SampleAppender.
func (*Distributor) NeedsThrottling(_ context.Context) bool {
	return false
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
	ch <- d.ingestersAlive
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

	ch <- prometheus.MustNewConstMetric(
		d.ingestersAlive,
		prometheus.GaugeValue,
		float64(len(d.cfg.Ring.GetAll(d.cfg.HeartbeatTimeout))),
	)

	d.clientsMtx.RLock()
	defer d.clientsMtx.RUnlock()
	ch <- prometheus.MustNewConstMetric(
		numClientsDesc,
		prometheus.GaugeValue,
		float64(len(d.clients)),
	)
}
