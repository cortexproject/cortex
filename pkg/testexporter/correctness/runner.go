package correctness

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

const (
	success = "success"
	fail    = "fail"
)

var (
	testcaseResult = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subsystem,
			Name:      "test_case_result_total",
			Help:      "Number of test cases by test name, that succeed / fail.",
		},
		[]string{"name", "result"},
	)
	startTime = time.Now()
)

// RunnerConfig is config, for the runner.
type RunnerConfig struct {
	testRate               float64
	testQueryMinSize       time.Duration
	testQueryMaxSize       time.Duration
	PrometheusAddr         string
	UserID                 string
	ExtraSelectors         string
	EnableDeleteSeriesTest bool
	CommonTestConfig       CommonTestConfig
	DeleteSeriesTestConfig DeleteSeriesTestConfig
}

// RegisterFlags does what it says.
func (cfg *RunnerConfig) RegisterFlags(f *flag.FlagSet) {
	f.Float64Var(&cfg.testRate, "test-rate", 1, "Query QPS")
	f.DurationVar(&cfg.testQueryMinSize, "test-query-min-size", 5*time.Minute, "The min query size to Prometheus.")
	f.DurationVar(&cfg.testQueryMaxSize, "test-query-max-size", 60*time.Minute, "The max query size to Prometheus.")

	f.StringVar(&cfg.PrometheusAddr, "prometheus-address", "", "Address of Prometheus instance to query.")
	f.StringVar(&cfg.UserID, "user-id", "", "UserID to send to Cortex.")

	f.StringVar(&cfg.ExtraSelectors, "extra-selectors", "", "Extra selectors to be included in queries, eg to identify different instances of this job.")
	f.BoolVar(&cfg.EnableDeleteSeriesTest, "enable-delete-series-test", false, "Enable tests for checking deletion of series.")

	cfg.CommonTestConfig.RegisterFlags(f)
	cfg.DeleteSeriesTestConfig.RegisterFlags(f)
}

// Runner runs a bunch of test cases, periodically checking their value.
type Runner struct {
	cfg    RunnerConfig
	mtx    sync.RWMutex
	cases  []Case
	quit   chan struct{}
	wg     sync.WaitGroup
	client v1.API
}

// NewRunner makes a new Runner.
func NewRunner(cfg RunnerConfig) (*Runner, error) {
	apiCfg := api.Config{
		Address: cfg.PrometheusAddr,
	}
	if cfg.UserID != "" {
		apiCfg.RoundTripper = &nethttp.Transport{
			RoundTripper: promhttp.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
				_ = user.InjectOrgIDIntoHTTPRequest(user.InjectOrgID(context.Background(), cfg.UserID), req)
				return api.DefaultRoundTripper.RoundTrip(req)
			}),
		}
	} else {
		apiCfg.RoundTripper = &nethttp.Transport{}
	}

	client, err := api.NewClient(apiCfg)
	if err != nil {
		return nil, err
	}

	tc := &Runner{
		cfg:    cfg,
		quit:   make(chan struct{}),
		client: v1.NewAPI(tracingClient{client}),
	}

	tc.wg.Add(1)
	go tc.verifyLoop()
	return tc, nil
}

type tracingClient struct {
	api.Client
}

func (t tracingClient) Do(ctx context.Context, req *http.Request) (*http.Response, []byte, error) {
	req = req.WithContext(ctx)
	req, tr := nethttp.TraceRequest(opentracing.GlobalTracer(), req)
	ctx = req.Context()
	defer tr.Finish()
	return t.Client.Do(ctx, req)
}

// Stop the checking goroutine.
func (r *Runner) Stop() {
	close(r.quit)
	r.wg.Wait()

	for _, tc := range r.cases {
		tc.Stop()
	}
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

	ctx := context.Background()
	log, ctx := spanlogger.New(ctx, "runRandomTest")
	span, trace := opentracing.SpanFromContext(ctx), "<none>"
	if span != nil {
		trace = fmt.Sprintf("%s", span.Context())
	}

	minQueryTime := tc.MinQueryTime()
	level.Info(log).Log("name", tc.Name(), "trace", trace, "minTime", minQueryTime)
	defer log.Finish()

	// pick a random time to start testStart and now
	// pick a random length between minDuration and maxDuration
	now := time.Now()
	start := minQueryTime.Add(time.Duration(rand.Int63n(int64(now.Sub(minQueryTime)))))
	duration := r.cfg.testQueryMinSize +
		time.Duration(rand.Int63n(int64(r.cfg.testQueryMaxSize)-int64(r.cfg.testQueryMinSize)))
	if start.Add(-duration).Before(minQueryTime) {
		duration = start.Sub(minQueryTime)
	}
	if duration < r.cfg.testQueryMinSize {
		return
	}

	// round off duration to minutes because we anyways have a window in minutes while doing queries.
	duration = (duration / time.Minute) * time.Minute
	level.Info(log).Log("start", start, "duration", duration)

	passed, err := tc.Test(ctx, r.client, r.cfg.ExtraSelectors, start, duration)
	if err != nil {
		level.Error(log).Log("err", err)
	}

	if passed {
		testcaseResult.WithLabelValues(tc.Name(), success).Inc()
	} else {
		testcaseResult.WithLabelValues(tc.Name(), fail).Inc()
	}
}
