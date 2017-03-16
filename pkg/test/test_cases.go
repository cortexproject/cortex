package test

import (
	"container/heap"
	"flag"
	"math"
	"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	api "github.com/prometheus/client_golang/api/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"golang.org/x/net/context"
)

var (
	testRate         = flag.Float64("test-rate", 1, "Query QPS")
	testQueryMinSize = flag.Duration("test-query-min-size", 5*time.Minute, "The min query size to Prometheus")
	testQueryMaxSize = flag.Duration("test-query-max-size", 4*60*time.Minute, "The max query size to Prometheus.")
	testQueryStep    = flag.Duration("test-query-step", 3*time.Second, "The query step.")
	testEpsilon      = flag.Float64("test-epsilion", 0.05, "Amount samples are allowed to be off by")
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
)

func init() {
	flag.Var(&testStart, "test-query-start", "Minimum start date for queries")
	prometheus.MustRegister(sampleResult)
}

type TestCase interface {
	prometheus.Collector

	Query() string
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
	now := time.Now()
	start := testStart.Time.Add(time.Duration(rand.Int63n(int64(now.Sub(testStart.Time)))))

	// pick a random length between minDuration and min(testQueryMaxSize, now)
	duration := *testQueryMinSize + time.Duration(rand.Int63n(int64(
		minDuration(
			now.Sub(start),
			*testQueryMaxSize-*testQueryMinSize,
		),
	)))

	log.Println("query =", tc.Query(), "start = ", start, "duration =", duration)

	value, err := ts.client.QueryRange(context.Background(), tc.Query(), api.Range{
		Start: start,
		End:   start.Add(duration),
		Step:  *testQueryStep,
	})
	if err != nil {
		log.Errorf("Error querying Prometheus: %v", err)
		return
	}
	if value.Type() != model.ValMatrix {
		log.Errorf("Didn't get matrix from Prom!")
		return
	}

	ms, ok := value.(model.Matrix)
	if !ok {
		log.Errorf("Didn't get matrix from Prom!")
		return
	}

	success := sampleResult.WithLabelValues("success")
	failure := sampleResult.WithLabelValues("fail")

	mergeSamples(ms, func(s model.SamplePair) {
		expected := tc.ExpectedValueAt(s.Timestamp.Time())
		correct := within(float64(s.Value), expected, *testEpsilon)
		if correct {
			success.Inc()
		} else {
			failure.Inc()
			log.Errorf("Wrong value: %f !~ %f", s.Value, expected)
		}
	})
}

type SampleStreamHeap []*model.SampleStream

func (h SampleStreamHeap) Len() int { return len(h) }
func (h SampleStreamHeap) Less(i, j int) bool {
	return h[i].Values[0].Timestamp < h[j].Values[0].Timestamp
}
func (h SampleStreamHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *SampleStreamHeap) Push(x interface{}) { *h = append(*h, x.(*model.SampleStream)) }
func (h *SampleStreamHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// We assume only one value is valid, but as the test exporter might restart,
// we will have a series per-instance.  Also, need to deal with staleness.
// Sould be N log(n), where N is total samples and n is number of timeseries.
func mergeSamples(m model.Matrix, callback func(model.SamplePair)) {
	// To deal with staleness, we track last value by fingerprint
	// and ignore repetition (and ignore the first sample we see
	// for each fingerprint).
	lastSeen := map[model.Fingerprint]model.SampleValue{}
	h := &SampleStreamHeap{}
	for _, stream := range m {
		if len(stream.Values) > 0 {
			heap.Push(h, stream)
		}
	}

	for h.Len() > 0 {
		stream := heap.Pop(h).(*model.SampleStream)
		fingerprint := stream.Metric.FastFingerprint()
		pair := stream.Values[0]
		lastValue, ok := lastSeen[fingerprint]
		if ok && lastValue != pair.Value {
			callback(pair)
		}

		lastSeen[fingerprint] = pair.Value

		if len(stream.Values) > 1 {
			stream.Values = stream.Values[1:]
			heap.Push(h, stream)
		}
	}
}

func within(a, b, epsilon float64) bool {
	return math.Abs((a-b)/a) < epsilon
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
