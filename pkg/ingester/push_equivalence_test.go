package ingester

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

const (
	coalesceTestUser = "coalesce-user"

	// The out-of-order window the tests run with, in milliseconds.
	coalesceTestOOOWindowMs = int64(30 * 60 * 1000)

	// Base timestamp the scenarios build on. Far enough from zero that samples
	// can be placed before it without becoming out of bounds.
	coalesceTestBaseMs = int64(10 * 60 * 60 * 1000)
)

// writeRequestSpec describes one write request. Requests are rebuilt from the
// spec for every run, because Push returns the request's time series to a pool.
type writeRequestSpec struct {
	series []labels.Labels
	// samples[i] belongs to series[i].
	samples []cortexpb.Sample
	// histograms[i] belongs to series[i].
	histograms []cortexpb.WrappedHistogram
	// exemplars are all attached to the first series.
	exemplars []cortexpb.Exemplar
}

func (s writeRequestSpec) build() *cortexpb.WriteRequest {
	req := cortexpb.ToWriteRequest(s.series, s.samples, nil, s.histograms, cortexpb.API)
	for _, ex := range s.exemplars {
		// The exemplar's label adapters must be copied per build. Push returns
		// the time series to a pool, and cortexpb.ReuseTimeseries clears
		// exemplar label names and values in place, which would write through
		// to the spec's own slice and leave a later build with empty labels.
		copied := cortexpb.Exemplar{TimestampMs: ex.TimestampMs, Value: ex.Value}
		copied.Labels = append(copied.Labels, ex.Labels...)
		req.Timeseries[0].Exemplars = append(req.Timeseries[0].Exemplars, copied)
	}
	return req
}

func coalesceTestSeries(name string) labels.Labels {
	return labels.FromStrings(labels.MetricName, name)
}

// oneSample is a request carrying a single sample for a single series.
func oneSample(seriesName string, tMs int64, v float64) writeRequestSpec {
	return writeRequestSpec{
		series:  []labels.Labels{coalesceTestSeries(seriesName)},
		samples: []cortexpb.Sample{{TimestampMs: tMs, Value: v}},
	}
}

func startCoalesceTestIngester(t *testing.T, coalesce PushCoalesceConfig) (*Ingester, *prometheus.Registry) {
	t.Helper()

	cfg := defaultIngesterTestConfig(t)
	cfg.PushCoalesce = coalesce

	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = model.Duration(time.Duration(coalesceTestOOOWindowMs) * time.Millisecond)
	limits.EnableNativeHistograms = true

	reg := prometheus.NewPedanticRegistry()
	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), ing) })

	test.Poll(t, time.Second, ring.ACTIVE, func() any {
		return ing.lifecycler.GetState()
	})
	return ing, reg
}

// pushSequentially pushes each request on its own and returns the error each
// one produced. This is the behaviour coalescing has to reproduce.
func pushSequentially(t *testing.T, ing *Ingester, specs []writeRequestSpec) []error {
	t.Helper()

	ctx := user.InjectOrgID(context.Background(), coalesceTestUser)
	errs := make([]error, 0, len(specs))
	for _, spec := range specs {
		_, err := ing.Push(ctx, spec.build())
		errs = append(errs, err)
	}
	return errs
}

// pushAsOneBatch pushes every request into a single coalesced commit, and
// returns the error each one produced.
//
// Batch composition is made deterministic rather than left to chance: a gate
// holds one commit open, every request under test is queued behind it, and only
// then is the gate released, so the queued requests necessarily form one batch.
func pushAsOneBatch(t *testing.T, ing *Ingester, specs []writeRequestSpec) []error {
	t.Helper()

	ctx := user.InjectOrgID(context.Background(), coalesceTestUser)

	// The tenant's TSDB, and with it the coalescer, is created by the first
	// push. Seed it with a series none of the scenarios touch.
	_, err := ing.Push(ctx, oneSample("coalesce_gate_seed", coalesceTestBaseMs, 1).build())
	require.NoError(t, err)

	db, err := ing.getTSDB(coalesceTestUser)
	require.NoError(t, err)
	require.NotNil(t, db.pushCoalescer, "push coalescing must be enabled for this test")

	// Replace the coalescer with one whose first appender blocks in Commit.
	// Everything else about it, including the real TSDB appender, is unchanged.
	gate := make(chan struct{})
	gateHit := make(chan struct{})
	var (
		mtx       sync.Mutex
		appenders int
	)
	db.pushCoalescer = newPushCoalescer(
		ing.cfg.PushCoalesce,
		ing.metrics.pushCoalesce,
		log.NewNopLogger(),
		func() extendedAppender {
			real := db.Appender(context.Background()).(extendedAppender)
			mtx.Lock()
			first := appenders == 0
			appenders++
			mtx.Unlock()
			if !first {
				return real
			}
			return &gatedAppender{extendedAppender: real, gate: gate, gateHit: gateHit}
		},
		func() int64 { return db.Head().MaxTime() },
	)

	// Hold a commit open on a series no scenario uses.
	gateDone := make(chan error, 1)
	go func() {
		_, err := ing.Push(ctx, oneSample("coalesce_gate_holder", coalesceTestBaseMs, 1).build())
		gateDone <- err
	}()
	select {
	case <-gateHit:
	case <-time.After(30 * time.Second):
		t.Fatal("gate commit was never entered")
	}

	// Queue every request under test behind the held commit, one at a time so
	// that they reach the batch in the order the scenario declares them.
	results := make([]chan error, len(specs))
	for idx, spec := range specs {
		results[idx] = make(chan error, 1)
		go func(spec writeRequestSpec, out chan error) {
			_, err := ing.Push(ctx, spec.build())
			out <- err
		}(spec, results[idx])

		deadline := time.Now().Add(30 * time.Second)
		for {
			queued := db.pushCoalescer.pendingLen()
			if queued == idx+1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("expected %d queued requests, got %d", idx+1, queued)
			}
			time.Sleep(time.Millisecond)
		}
	}

	close(gate)
	require.NoError(t, <-gateDone)

	errs := make([]error, len(specs))
	for idx, res := range results {
		select {
		case errs[idx] = <-res:
		case <-time.After(30 * time.Second):
			t.Fatalf("request %d did not return", idx)
		}
	}
	return errs
}

// gatedAppender blocks in Commit until its gate is closed.
type gatedAppender struct {
	extendedAppender

	gate    chan struct{}
	gateHit chan struct{}
	once    sync.Once
}

func (a *gatedAppender) Commit() error {
	a.once.Do(func() { close(a.gateHit) })
	<-a.gate
	return a.extendedAppender.Commit()
}

// headSample is one sample as stored in the head, in-order or out-of-order.
type headSample struct {
	T int64
	V float64
}

// dumpHead returns every sample the tenant's head holds, keyed by series. The
// querier covers the out-of-order head as well, so a sample that landed there
// shows up here.
func dumpHead(t *testing.T, ing *Ingester) map[string][]headSample {
	t.Helper()

	db, err := ing.getTSDB(coalesceTestUser)
	require.NoError(t, err)

	q, err := db.Querier(0, coalesceTestBaseMs*10)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })

	matcher, err := labels.NewMatcher(labels.MatchRegexp, labels.MetricName, ".+")
	require.NoError(t, err)

	out := map[string][]headSample{}
	set := q.Select(context.Background(), true, nil, matcher)
	for set.Next() {
		s := set.At()
		key := s.Labels().String()
		it := s.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			out[key] = append(out[key], headSample{T: ts, V: v})
		}
		require.NoError(t, it.Err())
	}
	require.NoError(t, set.Err())
	require.Empty(t, set.Warnings().AsErrors())

	for key := range out {
		slices.SortFunc(out[key], func(a, b headSample) int { return cmp.Compare(a.T, b.T) })
	}
	return out
}

func errStrings(errs []error) []string {
	out := make([]string, len(errs))
	for i, err := range errs {
		if err == nil {
			out[i] = "<nil>"
			continue
		}
		out[i] = err.Error()
	}
	return out
}

// coalesceScenario is a set of write requests whose interaction with each other
// is what makes merging their commits interesting, together with what merging
// them is expected to change.
//
// A batch shares one appender, and Append only sees committed state while
// Commit re-checks against the state earlier samples in the same batch left
// behind, counting rather than returning what it then rejects. So a batch can
// lose an error a separate commit would have returned, and in one case can
// accept a sample a separate commit would have rejected. Every scenario below
// declares which, so that any change in that set fails here rather than being
// discovered in production.
type coalesceScenario struct {
	name  string
	specs []writeRequestSpec

	// errorsDiffer records that at least one request gets a different error
	// than it would from a separate commit.
	errorsDiffer bool
	// dataDiffers records that the head ends up holding different samples.
	dataDiffers bool
}

func coalesceScenarios() []coalesceScenario {
	const (
		base = coalesceTestBaseMs
		win  = coalesceTestOOOWindowMs
	)

	return []coalesceScenario{
		{
			// The control: nothing shares a series, so merging changes nothing.
			name: "disjoint series",
			specs: []writeRequestSpec{
				oneSample("disjoint_a", base+1000, 1),
				oneSample("disjoint_b", base+1000, 2),
				oneSample("disjoint_c", base+1000, 3),
			},
		},
		{
			// Separate commits reject the second sample at Append time with a
			// duplicate error. A shared appender only finds out at Commit
			// time, where the error is counted and dropped rather than
			// returned, so the client is told its write succeeded. The sample
			// is dropped either way.
			//
			// Note that Cortex already behaves this way when the duplicate
			// arrives inside one request: see
			// TestIngesterPush_DuplicateIsAlreadySilentWithinOneRequest.
			name: "same series, same timestamp, different value",
			specs: []writeRequestSpec{
				oneSample("dup", base+1000, 1),
				oneSample("dup", base+1000, 2),
			},
			errorsDiffer: true,
		},
		{
			// An exact duplicate is accepted by Prometheus on purpose, so both
			// paths must report success.
			name: "same series, same timestamp, same value",
			specs: []writeRequestSpec{
				oneSample("exact_dup", base+1000, 7),
				oneSample("exact_dup", base+1000, 7),
			},
		},
		{
			// In-order then out-of-order, inside the window: accepted into the
			// out-of-order head either way.
			name: "same series, out of order within the window",
			specs: []writeRequestSpec{
				oneSample("ooo_in_window", base+1000, 1),
				oneSample("ooo_in_window", base+1000-win/2, 2),
			},
		},
		{
			// Out-of-order beyond the window. Separate commits return a
			// too-old error on the second request; the batch drops the sample
			// without reporting it. Same data either way.
			name: "same series, out of order beyond the window",
			specs: []writeRequestSpec{
				oneSample("ooo_too_old", base+1000, 1),
				oneSample("ooo_too_old", base+1000-2*win, 2),
			},
			errorsDiffer: true,
		},
		{
			// The only scenario where the stored data differs: a sample that
			// Append accepts against committed state, and that a larger sample
			// earlier in the same batch would have pushed outside the window
			// had it been committed first. The batch accepts it into the
			// out-of-order head; separate commits reject it as too old.
			//
			// The timestamps here exaggerate the effect to make it reachable at
			// all. TestIngesterPushCoalescing_OutOfOrderEscapeIsBoundedByInBatchHeadAdvance
			// bounds how far it can actually reach.
			name: "same series, in-batch sample pushes a later one out of the window",
			specs: []writeRequestSpec{
				oneSample("ooo_pushed_out", base+1000, 1),
				oneSample("ooo_pushed_out", base+1000+4*win, 2),
				oneSample("ooo_pushed_out", base+1000+win, 3),
			},
			errorsDiffer: true,
			dataDiffers:  true,
		},
		{
			// Several samples for one series inside a single request behave
			// identically in both modes, because a single request has always
			// shared one appender.
			name: "several samples for one series inside one request",
			specs: []writeRequestSpec{
				{
					series: []labels.Labels{
						coalesceTestSeries("intra"),
						coalesceTestSeries("intra"),
					},
					samples: []cortexpb.Sample{
						{TimestampMs: base + 1000, Value: 1},
						{TimestampMs: base + 1000, Value: 2},
					},
				},
				oneSample("intra_other", base+1000, 3),
			},
		},
		{
			// A new series created by one request and written again by another
			// in the same batch: the second request finds the series through
			// GetRef even though it has not been committed yet.
			name: "new series written by two requests",
			specs: []writeRequestSpec{
				oneSample("fresh", base+1000, 1),
				oneSample("fresh", base+2000, 2),
				oneSample("fresh", base+1000, 3),
			},
		},
	}
}

// A coalesced commit must change exactly what each scenario says it changes,
// and nothing else: the same error for every other request, and the same data
// in the head afterwards, including the out-of-order head.
func TestIngesterPushCoalescing_DivergenceFromSeparateCommits(t *testing.T) {
	for _, sc := range coalesceScenarios() {
		t.Run(sc.name, func(t *testing.T) {
			sequential, _ := startCoalesceTestIngester(t, PushCoalesceConfig{})
			coalesced, _ := startCoalesceTestIngester(t, defaultCoalesceConfig())

			// The sequential run gets the same seed and gate-holder requests
			// the coalesced run needs, so the two heads start out identical.
			seedErrs := pushSequentially(t, sequential, []writeRequestSpec{
				oneSample("coalesce_gate_seed", coalesceTestBaseMs, 1),
				oneSample("coalesce_gate_holder", coalesceTestBaseMs, 1),
			})
			for _, err := range seedErrs {
				require.NoError(t, err)
			}

			wantErrs := errStrings(pushSequentially(t, sequential, sc.specs))
			gotErrs := errStrings(pushAsOneBatch(t, coalesced, sc.specs))
			wantHead := dumpHead(t, sequential)
			gotHead := dumpHead(t, coalesced)

			if sc.errorsDiffer {
				require.NotEqual(t, wantErrs, gotErrs,
					"this scenario is expected to lose an error; if it no longer does, "+
						"the upstream TSDB has changed and this expectation should be dropped")
				t.Logf("separate: %v", wantErrs)
				t.Logf("coalesced: %v", gotErrs)
			} else {
				require.Equal(t, wantErrs, gotErrs,
					"per-request errors must match separate commits")
			}

			if sc.dataDiffers {
				require.NotEqual(t, wantHead, gotHead,
					"this scenario is expected to store different data; if it no longer does, "+
						"the upstream TSDB has changed and this expectation should be dropped")
				t.Logf("separate head: %v", wantHead)
				t.Logf("coalesced head: %v", gotHead)
			} else {
				require.Equal(t, wantHead, gotHead,
					"head contents must match separate commits")
			}
		})
	}
}

// Cortex already reports a duplicate differently depending on whether it
// arrives in one request or two, with coalescing nowhere in the picture: one
// request sharing one appender hides it, two requests each committing on their
// own surface it as a 400. Coalescing widens the first case from "one request"
// to "one batch", so this is the baseline the lost duplicate error has to be
// judged against.
func TestIngesterPush_DuplicateIsAlreadySilentWithinOneRequest(t *testing.T) {
	ing, _ := startCoalesceTestIngester(t, PushCoalesceConfig{})

	withinOneRequest := pushSequentially(t, ing, []writeRequestSpec{{
		series: []labels.Labels{
			coalesceTestSeries("intra_dup"),
			coalesceTestSeries("intra_dup"),
		},
		samples: []cortexpb.Sample{
			{TimestampMs: coalesceTestBaseMs + 1000, Value: 1},
			{TimestampMs: coalesceTestBaseMs + 1000, Value: 2},
		},
	}})
	require.NoError(t, withinOneRequest[0],
		"a duplicate inside one request is silently dropped and reported as success")
	require.Equal(t,
		[]headSample{{T: coalesceTestBaseMs + 1000, V: 1}},
		dumpHead(t, ing)[coalesceTestSeries("intra_dup").String()],
		"only the first value is stored")

	other, _ := startCoalesceTestIngester(t, PushCoalesceConfig{})
	acrossTwoRequests := pushSequentially(t, other, []writeRequestSpec{
		oneSample("split_dup", coalesceTestBaseMs+1000, 1),
		oneSample("split_dup", coalesceTestBaseMs+1000, 2),
	})
	require.NoError(t, acrossTwoRequests[0])
	require.ErrorContains(t, acrossTwoRequests[1], "duplicate sample for timestamp",
		"the same duplicate split across two requests is a 400")
}

// Concurrent pushes for the same tenant must not lose, duplicate or corrupt
// samples. Run with -race.
func TestIngesterPushCoalescing_ConcurrentPushesAreConsistent(t *testing.T) {
	ing, _ := startCoalesceTestIngester(t, defaultCoalesceConfig())
	ctx := user.InjectOrgID(context.Background(), coalesceTestUser)

	const (
		goroutines      = 32
		samplesPerSerie = 20
	)

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			name := fmt.Sprintf("concurrent_%d", g)
			for n := range samplesPerSerie {
				// Ascending timestamps per series, so nothing is out of order
				// and every sample must be stored.
				tMs := coalesceTestBaseMs + int64(n)*1000
				_, err := ing.Push(ctx, oneSample(name, tMs, float64(n)).build())
				require.NoError(t, err)
			}
		})
	}
	wg.Wait()

	head := dumpHead(t, ing)
	for g := range goroutines {
		key := coalesceTestSeries(fmt.Sprintf("concurrent_%d", g)).String()
		got := head[key]
		require.Lenf(t, got, samplesPerSerie, "series %s", key)
		for n, s := range got {
			require.Equal(t, coalesceTestBaseMs+int64(n)*1000, s.T)
			require.Equal(t, float64(n), s.V)
		}
	}
}

var _ storage.Appender = (*gatedAppender)(nil)

// startIngesterAndWait starts an ingester and blocks until it is ACTIVE in the
// ring, registering the matching cleanup.
func startIngesterAndWait(t *testing.T, ing *Ingester) error {
	t.Helper()
	if err := services.StartAndAwaitRunning(context.Background(), ing); err != nil {
		return err
	}
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), ing) })
	test.Poll(t, time.Second, ring.ACTIVE, func() any {
		return ing.lifecycler.GetState()
	})
	return nil
}
