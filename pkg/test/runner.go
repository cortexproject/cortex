package test

import (
	"flag"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	api "github.com/prometheus/client_golang/api/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
)

const (
	success = "success"
	fail    = "fail"
)

var (
	testcaseResult = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subsystem,
			Name:      "test_case_result_total",
			Help:      "Number of test cases that succeed / fail.",
		},
		[]string{"result"},
	)
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
	prometheus.MustRegister(testcaseResult)
	prometheus.MustRegister(sampleResult)
	prometheus.MustRegister(prometheusRequestDuration)
}

// RunnerConfig is config, for the runner.
type RunnerConfig struct {
	testRate         float64
	testQueryMinSize time.Duration
	testQueryMaxSize time.Duration
	testTimeEpsilon  time.Duration
	testEpsilon      float64
	prometheusAddr   string
	userID           string
	MinTime          TimeValue
	extraSelectors   string
}

// RegisterFlags does what it says.
func (cfg *RunnerConfig) RegisterFlags(f *flag.FlagSet) {
	f.Float64Var(&cfg.testRate, "test-rate", 1, "Query QPS")
	f.DurationVar(&cfg.testQueryMinSize, "test-query-min-size", 5*time.Minute, "The min query size to Prometheus.")
	f.DurationVar(&cfg.testQueryMaxSize, "test-query-max-size", 60*time.Minute, "The max query size to Prometheus.")
	f.DurationVar(&cfg.testTimeEpsilon, "test-time-epsilion", 1*time.Second, "Amount samples are allowed to be off by")
	f.Float64Var(&cfg.testEpsilon, "test-epsilion", 0.01, "Amount samples are allowed to be off by this %%")
	f.StringVar(&cfg.prometheusAddr, "prometheus-address", "", "Address of Prometheus instance to query.")
	f.StringVar(&cfg.userID, "user-id", "", "UserID to send to Cortex.")

	// By default, we only query for values from when this process started
	cfg.MinTime = NewTimeValue(time.Now())
	f.Var(&cfg.MinTime, "test-query-start", "Minimum start date for queries")

	f.StringVar(&cfg.extraSelectors, "extra-selectors", "", "Extra selectors to be included in queries, eg to identify different instances of this job.")
}

// Runner runs a bunch of test cases, periodically checking their value.
type Runner struct {
	cfg    RunnerConfig
	mtx    sync.RWMutex
	cases  []Case
	quit   chan struct{}
	wg     sync.WaitGroup
	client api.QueryAPI
}

// NewRunner makes a new Runner.
func NewRunner(cfg RunnerConfig) (*Runner, error) {
	apiCfg := api.Config{
		Address: cfg.prometheusAddr,
	}
	if cfg.userID != "" {
		apiCfg.Middleware = func(r *http.Request) {
			user.InjectIntoHTTPRequest(user.Inject(context.Background(), cfg.userID), r)
		}
	}
	client, err := api.New(apiCfg)
	if err != nil {
		return nil, err
	}

	tc := &Runner{
		cfg:    cfg,
		quit:   make(chan struct{}),
		client: api.NewQueryAPI(client),
	}
	tc.wg.Add(1)
	go tc.verifyLoop()
	return tc, nil
}

// Stop the checking goroutine.
func (r *Runner) Stop() {
	close(r.quit)
	r.wg.Wait()
}

// Add a new TestCase.
func (r *Runner) Add(tc Case) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.cases = append(r.cases, tc)
}

// Describe implements prometheus.Collector.
func (r *Runner) Describe(c chan<- *prometheus.Desc) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	for _, t := range r.cases {
		t.Describe(c)
	}
}

// Collect implements prometheus.Collector.
func (r *Runner) Collect(c chan<- prometheus.Metric) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	for _, t := range r.cases {
		t.Collect(c)
	}
}

func (r *Runner) verifyLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(time.Second / time.Duration(r.cfg.testRate))
	defer ticker.Stop()

	for {
		select {
		case <-r.quit:
			return
		case <-ticker.C:
			r.runRandomTest()
		}
	}
}

func (r *Runner) runRandomTest() {
	r.mtx.Lock()
	tc := r.cases[rand.Intn(len(r.cases))]
	r.mtx.Unlock()

	// pick a random time to start testStart and now
	// pick a random length between minDuration and maxDuration
	now := time.Now()
	start := r.cfg.MinTime.Time.Add(time.Duration(rand.Int63n(int64(now.Sub(r.cfg.MinTime.Time)))))
	duration := r.cfg.testQueryMinSize +
		time.Duration(rand.Int63n(int64(r.cfg.testQueryMaxSize)-int64(r.cfg.testQueryMinSize)))

	var pairs []model.SamplePair
	err := instrument.TimeRequestHistogram(context.Background(), "Prometheus.Query", prometheusRequestDuration, func(ctx context.Context) error {
		var err error
		pairs, err = tc.Query(ctx, r.client, r.cfg.extraSelectors, start, duration)
		return err
	})
	if err != nil {
		log.Errorf("Error running test: %v", err)
		return
	}
	failures := false
	for _, pair := range pairs {
		correct := r.timeEpsilonCorrect(tc.ExpectedValueAt, pair) || r.valueEpsilonCorrect(tc.ExpectedValueAt, pair)
		if correct {
			sampleResult.WithLabelValues(success).Inc()
		} else {
			failures = true
			sampleResult.WithLabelValues(fail).Inc()
			log.Errorf("Wrong value: %f !~ %f", tc.ExpectedValueAt(pair.Timestamp.Time()), pair.Value)
		}
	}
	if failures {
		testcaseResult.WithLabelValues(fail).Inc()
	} else {
		testcaseResult.WithLabelValues(success).Inc()
	}
}

func (r *Runner) timeEpsilonCorrect(f func(time.Time) float64, pair model.SamplePair) bool {
	minExpected := f(pair.Timestamp.Time().Add(-r.cfg.testTimeEpsilon))
	maxExpected := f(pair.Timestamp.Time().Add(r.cfg.testTimeEpsilon))
	if minExpected > maxExpected {
		minExpected, maxExpected = maxExpected, minExpected
	}
	return minExpected < float64(pair.Value) && float64(pair.Value) < maxExpected
}

func (r *Runner) valueEpsilonCorrect(f func(time.Time) float64, pair model.SamplePair) bool {
	expected := f(pair.Timestamp.Time())
	delta := math.Abs((float64(pair.Value) - expected) / expected)
	return delta < r.cfg.testEpsilon
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
