package correctness

import (
	"context"
	"flag"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/go-kit/log/level"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"

	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

const (
	namespace = "prometheus"
	subsystem = "test_exporter"
)

var sampleResult = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Subsystem: subsystem,
		Name:      "sample_result_total",
		Help:      "Number of samples that succeed / fail.",
	},
	[]string{"test_name", "result"},
)

type simpleTestCase struct {
	prometheus.GaugeFunc
	name            string
	expectedValueAt func(time.Time) float64
	cfg             CommonTestConfig
}

type CommonTestConfig struct {
	testTimeEpsilon    time.Duration
	testEpsilon        float64
	ScrapeInterval     time.Duration
	samplesEpsilon     float64
	timeQueryStart     TimeValue
	durationQuerySince time.Duration
}

func (cfg *CommonTestConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.testTimeEpsilon, "test-time-epsilion", 1*time.Second, "Amount samples are allowed to be off by")
	f.Float64Var(&cfg.testEpsilon, "test-epsilion", 0.01, "Amount samples are allowed to be off by this %%")
	f.DurationVar(&cfg.ScrapeInterval, "scrape-interval", 15*time.Second, "Expected scrape interval.")
	f.Float64Var(&cfg.samplesEpsilon, "test-samples-epsilon", 0.1, "Amount that the number of samples are allowed to be off by")

	// By default, we only query for values from when this process started
	cfg.timeQueryStart = NewTimeValue(time.Now())
	f.Var(&cfg.timeQueryStart, "test-query-start", "Minimum start date for queries")
	f.DurationVar(&cfg.durationQuerySince, "test-query-since", 0, "Duration in the past to test.  Overrides -test-query-start")
}

// NewSimpleTestCase makes a new simpleTestCase
func NewSimpleTestCase(name string, f func(time.Time) float64, cfg CommonTestConfig) Case {
	return &simpleTestCase{
		GaugeFunc: prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      name,
				Help:      name,
			},
			func() float64 {
				return f(time.Now())
			},
		),
		name:            name,
		expectedValueAt: f,
		cfg:             cfg,
	}
}

func (tc *simpleTestCase) Stop() {
}

func (tc *simpleTestCase) Name() string {
	return tc.name
}

func (tc *simpleTestCase) ExpectedValueAt(t time.Time) float64 {
	return tc.expectedValueAt(t)
}

func (tc *simpleTestCase) Query(ctx context.Context, client v1.API, selectors string, start time.Time, duration time.Duration) ([]model.SamplePair, error) {
	log, ctx := spanlogger.New(ctx, "simpleTestCase.Query")
	defer log.Finish()

	metricName := prometheus.BuildFQName(namespace, subsystem, tc.name)
	query := fmt.Sprintf("%s{%s}[%dm]", metricName, selectors, duration/time.Minute)
	level.Info(log).Log("query", query)

	value, wrngs, err := client.Query(ctx, query, start)
	if err != nil {
		return nil, err
	}
	if wrngs != nil {
		level.Warn(log).Log(
			"query", query,
			"start", start,
			"warnings", wrngs,
		)
	}
	if value.Type() != model.ValMatrix {
		return nil, fmt.Errorf("didn't get matrix from Prom")
	}

	ms, ok := value.(model.Matrix)
	if !ok {
		return nil, fmt.Errorf("didn't get matrix from Prom")
	}

	// sort samples belonging to different series by first timestamp of the batch
	sort.Slice(ms, func(i, j int) bool {
		if len(ms[i].Values) == 0 {
			return true
		}
		if len(ms[j].Values) == 0 {
			return true
		}

		return ms[i].Values[0].Timestamp.Before(ms[j].Values[0].Timestamp)
	})

	var result []model.SamplePair
	for _, stream := range ms {
		result = append(result, stream.Values...)
	}
	return result, nil
}

func (tc *simpleTestCase) Test(ctx context.Context, client v1.API, selectors string, start time.Time, duration time.Duration) (bool, error) {
	log := spanlogger.FromContext(ctx)
	pairs, err := tc.Query(ctx, client, selectors, start, duration)
	if err != nil {
		level.Info(log).Log("err", err)
		return false, err
	}

	return verifySamples(spanlogger.FromContext(ctx), tc, pairs, duration, tc.cfg), nil
}

func (tc *simpleTestCase) MinQueryTime() time.Time {
	return calculateMinQueryTime(tc.cfg.durationQuerySince, tc.cfg.timeQueryStart)
}

func verifySamples(log *spanlogger.SpanLogger, tc Case, pairs []model.SamplePair, duration time.Duration, cfg CommonTestConfig) bool {
	for _, pair := range pairs {
		correct := timeEpsilonCorrect(tc.ExpectedValueAt, pair, cfg.testTimeEpsilon) || valueEpsilonCorrect(tc.ExpectedValueAt, pair, cfg.testEpsilon)
		if correct {
			sampleResult.WithLabelValues(tc.Name(), success).Inc()
		} else {
			sampleResult.WithLabelValues(tc.Name(), fail).Inc()
			level.Error(log).Log("msg", "wrong value", "at", pair.Timestamp, "expected", tc.ExpectedValueAt(pair.Timestamp.Time()), "actual", pair.Value)
			log.Error(fmt.Errorf("wrong value"))
			return false
		}
	}

	// when verifying a deleted series we get samples for very short interval. As small as 1 or 2 missing/extra samples can cause test to fail.
	if duration > 5*time.Minute {
		expectedNumSamples := int(duration / cfg.ScrapeInterval)
		if !epsilonCorrect(float64(len(pairs)), float64(expectedNumSamples), cfg.samplesEpsilon) {
			level.Error(log).Log("msg", "wrong number of samples", "expected", expectedNumSamples, "actual", len(pairs))
			log.Error(fmt.Errorf("wrong number of samples"))
			return false
		}
	} else {
		expectedNumSamples := int(duration / cfg.ScrapeInterval)
		if math.Abs(float64(expectedNumSamples-len(pairs))) > 2 {
			level.Error(log).Log("msg", "wrong number of samples", "expected", expectedNumSamples, "actual", len(pairs))
			log.Error(fmt.Errorf("wrong number of samples"))
			return false
		}
	}

	return true
}

func timeEpsilonCorrect(f func(time.Time) float64, pair model.SamplePair, testTimeEpsilon time.Duration) bool {
	minExpected := f(pair.Timestamp.Time().Add(-testTimeEpsilon))
	maxExpected := f(pair.Timestamp.Time().Add(testTimeEpsilon))
	if minExpected > maxExpected {
		minExpected, maxExpected = maxExpected, minExpected
	}
	return minExpected < float64(pair.Value) && float64(pair.Value) < maxExpected
}

func valueEpsilonCorrect(f func(time.Time) float64, pair model.SamplePair, testEpsilon float64) bool {
	return epsilonCorrect(float64(pair.Value), f(pair.Timestamp.Time()), testEpsilon)
}

func epsilonCorrect(actual, expected, epsilon float64) bool {
	delta := math.Abs((actual - expected) / expected)
	return delta < epsilon
}

func calculateMinQueryTime(durationQuerySince time.Duration, timeQueryStart TimeValue) time.Time {
	minQueryTime := startTime
	if durationQuerySince != 0 {
		minQueryTime = time.Now().Add(-durationQuerySince)
	} else if timeQueryStart.set {
		minQueryTime = timeQueryStart.Time
	}
	return minQueryTime
}
