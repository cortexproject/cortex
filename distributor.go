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

package frankenstein

import (
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/weaveworks/scope/common/instrument"
	"golang.org/x/net/context"

	"github.com/weaveworks/frankenstein/ring"
	"github.com/weaveworks/frankenstein/user"
)

var (
	numClientsDesc = prometheus.NewDesc(
		"prometheus_distributor_ingester_clients",
		"The current number of ingester clients.",
		nil, nil,
	)
)

// Distributor is a storage.SampleAppender and a frankenstein.Querier which
// forwards appends and queries to individual ingesters.
type Distributor struct {
	ring          ReadRing
	clientFactory IngesterClientFactory
	clientsMtx    sync.RWMutex
	clients       map[string]*IngesterClient

	queryDuration   *prometheus.HistogramVec
	consulUpdates   prometheus.Counter
	receivedSamples prometheus.Counter
	sendDuration    *prometheus.HistogramVec
}

// ReadRing represents the read inferface to the ring.
type ReadRing interface {
	prometheus.Collector

	Get(key uint32) (ring.IngesterDesc, error)
	GetAll() []ring.IngesterDesc
}

// IngesterClientFactory creates ingester clients.
type IngesterClientFactory func(string) (*IngesterClient, error)

// DistributorConfig contains the configuration require to
// create a Distributor
type DistributorConfig struct {
	Ring          ReadRing
	ClientFactory IngesterClientFactory
}

// NewDistributor constructs a new Distributor
func NewDistributor(cfg DistributorConfig) *Distributor {
	return &Distributor{
		ring:          cfg.Ring,
		clientFactory: cfg.ClientFactory,
		clients:       map[string]*IngesterClient{},
		queryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "prometheus",
			Name:      "distributor_query_duration_seconds",
			Help:      "Time spent executing expression queries.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30},
		}, []string{"method", "status_code"}),
		consulUpdates: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "prometheus",
			Name:      "distributor_consul_updates_total",
			Help:      "The total number of received Consul updates.",
		}),
		receivedSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "prometheus",
			Name:      "distributor_received_samples_total",
			Help:      "The total number of received samples.",
		}),
		sendDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "prometheus",
			Name:      "distributor_send_duration_seconds",
			Help:      "Time spent sending sample batches to ingesters.",
			Buckets:   []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"method", "status_code"}),
	}
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

	client, err := d.clientFactory(hostname)
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
		collector, err := d.ring.Get(key)
		if err != nil {
			return err
		}
		otherSamples := samplesByIngester[collector.Hostname]
		samplesByIngester[collector.Hostname] = append(otherSamples, sample)
	}

	errs := make(chan error)
	for hostname, samples := range samplesByIngester {
		go func(hostname string, samples []*model.Sample) {
			errs <- d.sendSamples(ctx, hostname, samples)
		}(hostname, samples)
	}
	var lastErr error
	for i := 0; i < len(samplesByIngester); i++ {
		if err := <-errs; err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (d *Distributor) sendSamples(ctx context.Context, hostname string, samples []*model.Sample) error {
	client, err := d.getClientFor(hostname)
	if err != nil {
		return err
	}
	return instrument.TimeRequestHistogram("send", d.sendDuration, func() error {
		return client.Append(ctx, samples)
	})
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
		metricName, err := metricNameFromLabelMatchers(matchers...)
		if err != nil {
			return err
		}

		userID, err := user.GetID(ctx)
		if err != nil {
			return err
		}

		collector, err := d.ring.Get(tokenFor(userID, metricName))
		if err != nil {
			return err
		}

		client, err := d.getClientFor(collector.Hostname)
		if err != nil {
			return err
		}

		result, err = client.Query(ctx, from, to, matchers...)
		if err != nil {
			return err
		}
		return nil
	})
	return result, err
}

// LabelValuesForLabelName returns all of the label values that are associated with a given label name.
func (d *Distributor) LabelValuesForLabelName(ctx context.Context, labelName model.LabelName) (model.LabelValues, error) {
	valueSet := map[model.LabelValue]struct{}{}
	for _, c := range d.ring.GetAll() {
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

// NeedsThrottling implements SampleAppender.
func (*Distributor) NeedsThrottling(_ context.Context) bool {
	return false
}

// Describe implements prometheus.Collector.
func (d *Distributor) Describe(ch chan<- *prometheus.Desc) {
	d.queryDuration.Describe(ch)
	ch <- d.consulUpdates.Desc()
	ch <- d.receivedSamples.Desc()
	d.sendDuration.Describe(ch)
	d.ring.Describe(ch)
	ch <- numClientsDesc
}

// Collect implements prometheus.Collector.
func (d *Distributor) Collect(ch chan<- prometheus.Metric) {
	d.queryDuration.Collect(ch)
	ch <- d.consulUpdates
	ch <- d.receivedSamples
	d.sendDuration.Collect(ch)
	d.ring.Collect(ch)

	d.clientsMtx.RLock()
	defer d.clientsMtx.RUnlock()
	ch <- prometheus.MustNewConstMetric(
		numClientsDesc,
		prometheus.GaugeValue,
		float64(len(d.clients)),
	)
}
