package test

import (
	"flag"
	"math"
	"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	api "github.com/prometheus/client_golang/api/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/instrument"
	"golang.org/x/net/context"
)

var (
	testRate         = flag.Float64("test-rate", 1, "Query QPS")
	testQueryMinSize = flag.Duration("test-query-min-size", 5*time.Minute, "The min query size to Prometheus.")
	testQueryMaxSize = flag.Duration("test-query-max-size", 60*time.Minute, "The max query size to Prometheus.")
	testTimeEpsilon  = flag.Duration("test-time-epsilion", 1*time.Second, "Amount samples are allowed to be off by")
	testEpsilon      = flag.Float64("test-epsilion", 0.01, "Amount samples are allowed to be off by this %%")
	prometheusAddr   = flag.String("prometheus-address", "", "Address of Prometheus instance to query.")

	// By default, we only query for values from when this process started
	testStart = NewTimeValue(time.Now())

	sampleResult = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subsystem,
			Name:      "sample_result_total",
			Help:      "Number of samples that succeed / fail.",
		},
		[]string{"result"},
	)
	prometheusRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: subsystem,
		Name:      "prometheus_request_duration_seconds",
		Help:      "Time spent doing Prometheus requests.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation", "status_code"})
)

func init() {
	flag.Var(&testStart, "test-query-start", "Minimum start date for queries")
	prometheus.MustRegister(sampleResult)
}

type TestCase interface {
	prometheus.Collector

	Query(ctx context.Context, client api.QueryAPI, start time.Time, duration time.Duration) ([]model.SamplePair, error)
	ExpectedValueAt(time.Time) float64
}

type TestCases struct {
	mtx    sync.RWMutex
	cases  []TestCase
	quit   chan struct{}
	wg     sync.WaitGroup
	client api.QueryAPI
}

func NewTestCases() (*TestCases, error) {
	client, err := api.New(api.Config{
		Address: *prometheusAddr,
	})
	if err != nil {
		return nil, err
	}

	tc := &TestCases{
		quit:   make(chan struct{}),
		client: api.NewQueryAPI(client),
	}
	tc.wg.Add(1)
	go tc.verifyLoop()
	return tc, nil
}

func (ts *TestCases) Stop() {
	close(ts.quit)
	ts.wg.Wait()
}

func (ts *TestCases) Add(tc TestCase) {
	ts.mtx.Lock()
	defer ts.mtx.Unlock()
	ts.cases = append(ts.cases, tc)
}

func (ts *TestCases) Describe(c chan<- *prometheus.Desc) {
	ts.mtx.RLock()
	defer ts.mtx.RUnlock()
	for _, t := range ts.cases {
		t.Describe(c)
	}
}

func (ts *TestCases) Collect(c chan<- prometheus.Metric) {
	ts.mtx.RLock()
	defer ts.mtx.RUnlock()
	for _, t := range ts.cases {
		t.Collect(c)
	}
}

func (ts *TestCases) verifyLoop() {
	defer ts.wg.Done()

	ticker := time.NewTicker(time.Second / time.Duration(*testRate))
	defer ticker.Stop()

	for {
		select {
		case <-ts.quit:
			return
		case <-ticker.C:
			ts.runRandomTest()
		}
	}
}

func (ts *TestCases) runRandomTest() {
	ts.mtx.Lock()
	tc := ts.cases[rand.Intn(len(ts.cases))]
	ts.mtx.Unlock()

	// pick a random time to start testStart and now
	// pick a random length between minDuration and maxDuration
	now := time.Now()
	start := testStart.Time.Add(time.Duration(rand.Int63n(int64(now.Sub(testStart.Time)))))
	duration := *testQueryMinSize +
		time.Duration(rand.Int63n(int64(*testQueryMaxSize)-int64(*testQueryMinSize)))

	var pairs []model.SamplePair
	err := instrument.TimeRequestHistogram(context.Background(), "Prometheus.Query", prometheusRequestDuration, func(ctx context.Context) error {
		var err error
		pairs, err = tc.Query(ctx, ts.client, start, duration)
		return err
	})
	if err != nil {
		log.Errorf("Error running test: %v", err)
		return
	}
	success := sampleResult.WithLabelValues("success")
	failure := sampleResult.WithLabelValues("fail")

	for _, pair := range pairs {
		correct := timeEpsilonCorrect(tc.ExpectedValueAt, pair) || valueEpsilonCorrect(tc.ExpectedValueAt, pair)
		if correct {
			success.Inc()
		} else {
			failure.Inc()
			log.Errorf("Wrong value: %f !~ %f", tc.ExpectedValueAt(pair.Timestamp.Time()), pair.Value)
		}
	}
}

func timeEpsilonCorrect(f func(time.Time) float64, pair model.SamplePair) bool {
	minExpected := f(pair.Timestamp.Time().Add(-*testTimeEpsilon))
	maxExpected := f(pair.Timestamp.Time().Add(*testTimeEpsilon))
	if minExpected > maxExpected {
		minExpected, maxExpected = maxExpected, minExpected
	}
	return minExpected < float64(pair.Value) && float64(pair.Value) < maxExpected
}

func valueEpsilonCorrect(f func(time.Time) float64, pair model.SamplePair) bool {
	expected := f(pair.Timestamp.Time())
	delta := math.Abs((float64(pair.Value) - expected) / expected)
	return delta < *testEpsilon
}

func maxDuration(a, b time.Duration) time.Duration {
	if a < b {
		return b
	}
	return a
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
