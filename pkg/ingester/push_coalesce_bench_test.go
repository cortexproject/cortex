package ingester

import (
	"context"
	"fmt"
	"runtime"
	"runtime/metrics"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

// The benchmarks in this file reproduce one traffic shape: a single tenant whose write requests are
// sharded by series across a large ingester fleet, so each ingester sees a very
// high rate of very small pushes, every one of which commits on its own.
//
// Reported metrics, per configuration:
//
//	commits/push      TSDB commits per push. 1.0 means no coalescing happened.
//	requests/commit   Mean batch size.
//	p50_us, p99_us    Per-push latency as the pusher sees it.
//	pushes/s          Achieved throughput.
//	us/push           Wall time per push. Prefer this over ns/op, which the Go
//	                  harness computes from b.N and which is wrong whenever
//	                  concurrency exceeds b.N.
//
// Typical invocation:
//
//	go test -tags "netgo slicelabels" ./pkg/ingester/ -run XXX \
//	    -bench BenchmarkIngesterPushCoalescing -benchtime 200000x -timeout 60m
//
// To show where the contention went:
//
//	go test -tags "netgo slicelabels" ./pkg/ingester/ -run XXX \
//	    -bench 'BenchmarkIngesterPushCoalescing/concurrency=4096/coalescing=off' \
//	    -benchtime 200000x -mutexprofile /tmp/mutex-off.out

const (
	// Samples per request. Small on purpose: when a distributor shards a large
	// tenant's remote-write requests by series across many ingesters, each
	// ingester's share of a request is only a handful of samples.
	benchSamplesPerRequest = 5

	// Series each pusher owns in the disjoint workload. Kept small so that the
	// total series count stays bounded at high concurrency: a benchmark that
	// creates a new series on nearly every push measures series creation and
	// the label interner, not the append and commit path it is meant to
	// measure. Every series is created in a warm-up phase before the clock
	// starts, so the measured section is steady state, as a production
	// ingester is.
	benchSeriesPerPusher = 8

	// Series the shared workload draws from. Small enough that concurrent
	// pushers collide on series often, which is what exercises the cost of
	// keeping overlapping requests out of one batch.
	benchSharedSeriesCount = 512

	// Interval between successive samples of one series. Small enough that the
	// whole run stays well inside the head's appendable window even when a
	// pusher issues many thousands of requests.
	benchSampleIntervalMs = 100
)

// benchPusher holds everything one pushing goroutine needs, so that the
// benchmark harness itself introduces no shared mutable state. A shared mutex
// here would dominate the measurement and hide the WAL contention the
// benchmark exists to measure.
type benchPusher struct {
	series []labels.Labels

	// step is this pusher's own sample clock. It is deliberately not shared:
	// with a global clock a goroutine that lags behind the others ends up
	// pushing a timestamp the head has already moved its minValidTime past,
	// which the TSDB rejects as out of bounds and which has nothing to do with
	// what the benchmark measures.
	step int64

	lbls    []labels.Labels
	samples []cortexpb.Sample
	next    int
}

func benchSeriesLabels(idx int) labels.Labels {
	return labels.FromStrings(
		labels.MetricName, "bench_metric",
		"pod", fmt.Sprintf("pod-%d", idx%97),
		"instance", fmt.Sprintf("instance-%d", idx),
	)
}

// newBenchPushers builds one pusher per goroutine. In the disjoint workload
// every pusher owns its own series, as a distributor sharding by series
// produces. In the shared workload they all draw from one small pool, so
// concurrent requests frequently write the same series.
func newBenchPushers(concurrency int, shared bool) []*benchPusher {
	pushers := make([]*benchPusher, concurrency)

	var sharedSeries []labels.Labels
	if shared {
		sharedSeries = make([]labels.Labels, benchSharedSeriesCount)
		for i := range sharedSeries {
			sharedSeries[i] = benchSeriesLabels(i)
		}
	}

	for p := range pushers {
		pusher := &benchPusher{
			lbls:    make([]labels.Labels, benchSamplesPerRequest),
			samples: make([]cortexpb.Sample, benchSamplesPerRequest),
			// Stagger the starting offset so that pushers do not all begin on
			// the same series.
			next: (p * benchSamplesPerRequest) % max(1, benchSharedSeriesCount),
		}
		if shared {
			pusher.series = sharedSeries
		} else {
			pusher.series = make([]labels.Labels, benchSeriesPerPusher)
			for i := range pusher.series {
				pusher.series[i] = benchSeriesLabels(p*benchSeriesPerPusher + i)
			}
			pusher.next = 0
		}
		pushers[p] = pusher
	}
	return pushers
}

// request builds the next write request for this pusher: benchSamplesPerRequest
// consecutive series, the way one ingester's shard of a scrape looks.
func (p *benchPusher) request() *cortexpb.WriteRequest {
	p.step++
	ts := coalesceTestBaseMs + p.step*benchSampleIntervalMs

	for i := 0; i < benchSamplesPerRequest; i++ {
		idx := (p.next + i) % len(p.series)
		p.lbls[i] = p.series[idx]
		p.samples[i] = cortexpb.Sample{TimestampMs: ts, Value: float64(idx)}
	}
	p.next = (p.next + benchSamplesPerRequest) % len(p.series)

	return cortexpb.ToWriteRequest(p.lbls, p.samples, nil, nil, cortexpb.API)
}

// warmupRequests is how many requests this pusher needs in order to have
// touched, and therefore created, every series it owns.
func (p *benchPusher) warmupRequests() int {
	return (len(p.series) + benchSamplesPerRequest - 1) / benchSamplesPerRequest
}

func startBenchIngester(b *testing.B, coalesce PushCoalesceConfig) (*Ingester, *prometheus.Registry) {
	b.Helper()

	cfg := defaultIngesterTestConfig(b)
	cfg.PushCoalesce = coalesce
	// Active series bookkeeping is identical with and without coalescing and
	// only adds noise to the comparison.
	cfg.ActiveSeriesMetricsEnabled = false

	// Leave label string interning at its upstream default. With it on, the
	// interner's pool mutex dominates mutex contention during series creation
	// and masks the WAL contention being measured.
	cfg.LabelsStringInterningEnabled = false

	limits := defaultLimitsTestConfig()
	// An out-of-order window changes which code path a late sample takes, and
	// is commonly enabled in production, so the benchmark runs with one too.
	limits.OutOfOrderTimeWindow = model.Duration(time.Duration(coalesceTestOOOWindowMs) * time.Millisecond)
	limits.MaxLocalSeriesPerUser = 0
	limits.MaxLocalSeriesPerMetric = 0
	limits.MaxGlobalSeriesPerUser = 0
	limits.MaxGlobalSeriesPerMetric = 0

	reg := prometheus.NewPedanticRegistry()
	ing, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, nil, "", reg)
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), ing))
	b.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), ing) })

	test.Poll(b, time.Second, ring.ACTIVE, func() any {
		return ing.lifecycler.GetState()
	})
	return ing, reg
}

func BenchmarkIngesterPushCoalescing(b *testing.B) {
	variants := []struct {
		name string
		cfg  PushCoalesceConfig
	}{
		{name: "coalescing=off", cfg: PushCoalesceConfig{}},
		{name: "coalescing=on", cfg: defaultCoalesceConfig()},
	}

	for _, concurrency := range []int{1, 64, 1024, 4096, 16384, 30000} {
		for _, v := range variants {
			b.Run(fmt.Sprintf("concurrency=%d/%s", concurrency, v.name), func(b *testing.B) {
				benchmarkPush(b, concurrency, v.cfg, false)
			})
		}
	}
}

// BenchmarkIngesterPushCoalescingSaturated compares the two coalescer queue
// implementations with the scheduler saturated the way the production trace
// showed it: GOMAXPROCS=8 and several hundred runnable goroutines that have
// nothing to do with the push path (gRPC transport goroutines in production).
// That is the condition under which a woken mutex waiter waits more than 1ms
// for a P and flips sync.Mutex into starvation mode regardless of how short
// the critical section is. Run it with -cpu 8.
func BenchmarkIngesterPushCoalescingSaturated(b *testing.B) {
	variants := []struct {
		name string
		cfg  PushCoalesceConfig
	}{
		{name: "coalescing=off", cfg: PushCoalesceConfig{}},
		{name: "coalescing=on", cfg: defaultCoalesceConfig()},
	}

	for _, bg := range []int{0, 800} {
		for _, concurrency := range []int{1024, 4096} {
			for _, v := range variants {
				b.Run(fmt.Sprintf("bg=%d/concurrency=%d/%s", bg, concurrency, v.name), func(b *testing.B) {
					benchmarkPushWithLoad(b, concurrency, v.cfg, false, bg)
				})
			}
		}
	}
}

// benchBackgroundSpin is how long one background goroutine runs before
// yielding. With 800 of them on 8 Ps a runnable goroutine waits roughly
// 800*spin/8 for a P, which at 50us is in the 1-10ms range the production
// trace measured during a burst.
const benchBackgroundSpin = 50 * time.Microsecond

// startBackgroundLoad keeps n goroutines runnable until stop is called.
func startBackgroundLoad(n int) (stop func()) {
	if n == 0 {
		return func() {}
	}
	var (
		done atomic.Bool
		wg   sync.WaitGroup
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			x := uint64(i)
			for !done.Load() {
				deadline := time.Now().Add(benchBackgroundSpin)
				for time.Now().Before(deadline) {
					x = x*6364136223846793005 + 1442695040888963407
				}
				runtime.Gosched()
			}
			_ = x
		}()
	}
	return func() {
		done.Store(true)
		wg.Wait()
	}
}

// mutexWaitSeconds is the process-wide time goroutines have spent blocked on
// sync.Mutex and sync.RWMutex, as the runtime accounts it.
func mutexWaitSeconds() float64 {
	sample := []metrics.Sample{{Name: "/sync/mutex/wait/total:seconds"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindFloat64 {
		return 0
	}
	return sample[0].Value.Float64()
}

// schedLatencies snapshots the runtime's histogram of time goroutines spent
// runnable before running.
func schedLatencies() *metrics.Float64Histogram {
	sample := []metrics.Sample{{Name: "/sched/latencies:seconds"}}
	metrics.Read(sample)
	return sample[0].Value.Float64Histogram()
}

// schedLatencyQuantile returns quantile q of the scheduling latencies observed
// between two snapshots.
func schedLatencyQuantile(before, after *metrics.Float64Histogram, q float64) float64 {
	var total uint64
	delta := make([]uint64, len(after.Counts))
	for i := range after.Counts {
		delta[i] = after.Counts[i] - before.Counts[i]
		total += delta[i]
	}
	if total == 0 {
		return 0
	}
	target := uint64(q * float64(total))
	var seen uint64
	for i, c := range delta {
		seen += c
		if seen > target {
			return after.Buckets[i+1]
		}
	}
	return after.Buckets[len(after.Buckets)-1]
}

// BenchmarkIngesterPushCoalescingSharedSeries measures the same thing on a
// workload where concurrent requests frequently write the same series, so that
// a batch regularly carries several requests for one series. Batch size is
// unaffected by that, but the per-series locks the appends take are, so it is
// worth measuring separately from the disjoint case.
func BenchmarkIngesterPushCoalescingSharedSeries(b *testing.B) {
	variants := []struct {
		name string
		cfg  PushCoalesceConfig
	}{
		{name: "coalescing=off", cfg: PushCoalesceConfig{}},
		{name: "coalescing=on", cfg: defaultCoalesceConfig()},
	}

	for _, concurrency := range []int{1024, 4096} {
		for _, v := range variants {
			b.Run(fmt.Sprintf("concurrency=%d/%s", concurrency, v.name), func(b *testing.B) {
				benchmarkPush(b, concurrency, v.cfg, true)
			})
		}
	}
}

func benchmarkPush(b *testing.B, concurrency int, coalesce PushCoalesceConfig, sharedSeries bool) {
	benchmarkPushWithLoad(b, concurrency, coalesce, sharedSeries, 0)
}

func benchmarkPushWithLoad(b *testing.B, concurrency int, coalesce PushCoalesceConfig, sharedSeries bool, background int) {
	ing, reg := startBenchIngester(b, coalesce)
	ctx := user.InjectOrgID(context.Background(), coalesceTestUser)
	pushers := newBenchPushers(concurrency, sharedSeries)

	// Create the tenant's TSDB outside the measured section so that TSDB
	// creation and WAL replay are not attributed to the benchmark.
	_, err := ing.Push(ctx, pushers[0].request())
	require.NoError(b, err)

	commitsBefore := commitCount(b, reg)

	perPusher := b.N / concurrency
	if perPusher < 1 {
		perPusher = 1
	}
	total := concurrency * perPusher

	// Each pusher records its own latencies, so the measurement adds no shared
	// state of its own.
	latencies := make([][]time.Duration, concurrency)
	for i := range latencies {
		latencies[i] = make([]time.Duration, 0, perPusher)
	}

	var (
		failed   atomic.Int64
		firstErr atomic.Value
		wg       sync.WaitGroup
		ready    sync.WaitGroup
		startCh  = make(chan struct{})
	)

	for p := 0; p < concurrency; p++ {
		wg.Add(1)
		ready.Add(1)
		go func(p int) {
			defer wg.Done()
			pusher := pushers[p]

			// Create every series this pusher writes, so that the measured
			// section appends to existing series rather than paying series
			// creation, which a production ingester does not do per push.
			for n := 0; n < pusher.warmupRequests(); n++ {
				if _, err := ing.Push(ctx, pusher.request()); err != nil {
					failed.Add(1)
					firstErr.CompareAndSwap(nil, err)
				}
			}

			ready.Done()
			<-startCh
			for n := 0; n < perPusher; n++ {
				req := pusher.request()
				start := time.Now()
				if _, err := ing.Push(ctx, req); err != nil {
					failed.Add(1)
					firstErr.CompareAndSwap(nil, err)
				}
				latencies[p] = append(latencies[p], time.Since(start))
			}
		}(p)
	}

	// Have every goroutine spawned, warmed up and parked before the clock
	// starts, so that neither goroutine creation nor series creation is
	// measured and all pushers really do run at once.
	ready.Wait()
	require.Zerof(b, failed.Load(), "%d warm-up pushes failed, first error: %v",
		failed.Load(), firstErr.Load())

	failed.Store(0)
	commitsBefore = commitCount(b, reg)

	stopBackground := startBackgroundLoad(background)
	mutexWaitBefore := mutexWaitSeconds()
	schedBefore := schedLatencies()

	b.ResetTimer()
	wallStart := time.Now()
	close(startCh)
	wg.Wait()
	wall := time.Since(wallStart)
	b.StopTimer()

	mutexWait := mutexWaitSeconds() - mutexWaitBefore
	schedAfter := schedLatencies()
	stopBackground()

	require.Zerof(b, failed.Load(), "%d of %d measured pushes failed, first error: %v",
		failed.Load(), total, firstErr.Load())

	commits := commitCount(b, reg) - commitsBefore

	all := make([]time.Duration, 0, total)
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	b.ReportMetric(float64(commits)/float64(total), "commits/push")
	if commits > 0 {
		b.ReportMetric(float64(total)/float64(commits), "requests/commit")
	}
	b.ReportMetric(float64(percentile(all, 0.50).Microseconds()), "p50_us")
	b.ReportMetric(float64(percentile(all, 0.99).Microseconds()), "p99_us")
	b.ReportMetric(float64(total)/wall.Seconds(), "pushes/s")
	b.ReportMetric(wall.Seconds()*1e6/float64(total), "us/push")
	b.ReportMetric(mutexWait*1e6/float64(total), "mutexwait_us/push")
	b.ReportMetric(schedLatencyQuantile(schedBefore, schedAfter, 0.50)*1e6, "sched_p50_us")
	b.ReportMetric(schedLatencyQuantile(schedBefore, schedAfter, 0.99)*1e6, "sched_p99_us")
}

func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(q * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// commitCount returns the number of TSDB commits the ingester has issued.
//
// The two configurations are counted through different instruments because
// each one only observes on its own path, but both count exactly one event per
// Commit, so the ratio to the number of pushes is comparable.
func commitCount(b *testing.B, reg *prometheus.Registry) int64 {
	b.Helper()

	families, err := reg.Gather()
	require.NoError(b, err)

	for _, f := range families {
		if f.GetName() == "cortex_ingester_push_coalesce_commits_total" {
			if total := sumCounter(f); total > 0 {
				return int64(total)
			}
		}
	}
	for _, f := range families {
		if f.GetName() == "cortex_ingester_tsdb_appender_commit_duration_seconds" {
			return int64(histogramCount(f))
		}
	}
	return 0
}

func sumCounter(f *dto.MetricFamily) float64 {
	var total float64
	for _, m := range f.GetMetric() {
		total += m.GetCounter().GetValue()
	}
	return total
}

func histogramCount(f *dto.MetricFamily) uint64 {
	var total uint64
	for _, m := range f.GetMetric() {
		total += m.GetHistogram().GetSampleCount()
	}
	return total
}

// BenchmarkIngesterPushCoalescingBatchSize sweeps the batch size cap at a
// concurrency where the queue is deep, which is where the cap decides how many
// commits it takes to drain the queue and therefore how long a queued request
// waits.
func BenchmarkIngesterPushCoalescingBatchSize(b *testing.B) {
	for _, concurrency := range []int{1024, 4096} {
		for _, maxRequests := range []int{8, 32, 128, 512} {
			cfg := defaultCoalesceConfig()
			cfg.MaxBatchRequests = maxRequests
			// Raise the sample cap out of the way so that the request cap is
			// the only thing being varied.
			cfg.MaxBatchSamples = 0
			b.Run(fmt.Sprintf("concurrency=%d/max-batch-requests=%d", concurrency, maxRequests), func(b *testing.B) {
				benchmarkPush(b, concurrency, cfg, false)
			})
		}
	}
}
