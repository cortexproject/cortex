package ingester

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

// The append phase does more than float samples: it appends native histograms
// and exemplars too, and a batch leader now runs all of that on behalf of other
// requests. Those branches were not reachable from the other tests, because
// exemplars are disabled by default and the float-only specs carry no
// histograms, so they get their own coverage here.

// startExemplarCoalesceTestIngester starts an ingester with exemplars enabled,
// which the shared helper deliberately leaves off.
func startExemplarCoalesceTestIngester(t *testing.T, coalesce PushCoalesceConfig) *Ingester {
	t.Helper()

	cfg := defaultIngesterTestConfig(t)
	cfg.PushCoalesce = coalesce

	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = model.Duration(time.Duration(coalesceTestOOOWindowMs) * time.Millisecond)
	limits.EnableNativeHistograms = true
	limits.MaxExemplars = 10

	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	require.NoError(t, startIngesterAndWait(t, ing))
	return ing
}

// histogramRequest builds a request carrying one native histogram.
func histogramRequest(seriesName string, tMs int64, seed int) writeRequestSpec {
	return writeRequestSpec{
		series: []labels.Labels{coalesceTestSeries(seriesName)},
		histograms: []cortexpb.WrappedHistogram{
			cortexpb.WrapHistogram(cortexpb.HistogramToHistogramProto(tMs, tsdbutil.GenerateTestHistogram(int64(seed)))),
		},
	}
}

// exemplarRequest builds a request carrying one float sample and one exemplar
// attached to it.
func exemplarRequest(seriesName string, tMs int64, v float64, traceID string) writeRequestSpec {
	return writeRequestSpec{
		series:  []labels.Labels{coalesceTestSeries(seriesName)},
		samples: []cortexpb.Sample{{TimestampMs: tMs, Value: v}},
		exemplars: []cortexpb.Exemplar{{
			Labels:      []cortexpb.LabelAdapter{{Name: "traceID", Value: traceID}},
			TimestampMs: tMs,
			Value:       v,
		}},
	}
}

// Native histograms appended by a leader on another request's behalf must end
// up where separate commits would have put them.
func TestIngesterPushCoalescing_NativeHistogramsMatchSeparateCommits(t *testing.T) {
	const base = coalesceTestBaseMs

	specs := []writeRequestSpec{
		histogramRequest("hist_a", base+1000, 1),
		histogramRequest("hist_b", base+1000, 2),
		// Same series as the first request, later timestamp: in order, so both
		// paths must store both.
		histogramRequest("hist_a", base+2000, 3),
		// A float sample in the same batch, to exercise both branches together.
		oneSample("hist_float", base+1000, 7),
	}

	sequential := startExemplarCoalesceTestIngester(t, PushCoalesceConfig{})
	mirrorBatchHarnessPushes(t, sequential)
	wantErrs := pushSequentially(t, sequential, specs)

	coalesced := startExemplarCoalesceTestIngester(t, defaultCoalesceConfig())
	gotErrs := pushAsOneBatch(t, coalesced, specs)

	require.Equal(t, errStrings(wantErrs), errStrings(gotErrs))
	require.Equal(t, dumpHeadAllTypes(t, sequential), dumpHeadAllTypes(t, coalesced))

	// The scenario has to have actually stored histograms, or it proves nothing.
	stored := dumpHeadAllTypes(t, coalesced)
	require.Len(t, stored[coalesceTestSeries("hist_a").String()], 2)
	require.Len(t, stored[coalesceTestSeries("hist_b").String()], 1)
}

// Exemplars appended by a leader on another request's behalf must end up where
// separate commits would have put them.
func TestIngesterPushCoalescing_ExemplarsMatchSeparateCommits(t *testing.T) {
	const base = coalesceTestBaseMs

	specs := []writeRequestSpec{
		exemplarRequest("ex_a", base+1000, 1, "trace-a"),
		exemplarRequest("ex_b", base+1000, 2, "trace-b"),
		exemplarRequest("ex_a", base+2000, 3, "trace-c"),
	}

	sequential := startExemplarCoalesceTestIngester(t, PushCoalesceConfig{})
	mirrorBatchHarnessPushes(t, sequential)
	wantErrs := pushSequentially(t, sequential, specs)

	coalesced := startExemplarCoalesceTestIngester(t, defaultCoalesceConfig())
	gotErrs := pushAsOneBatch(t, coalesced, specs)

	require.Equal(t, errStrings(wantErrs), errStrings(gotErrs))

	want := dumpExemplars(t, sequential, "ex_a", "ex_b")
	got := dumpExemplars(t, coalesced, "ex_a", "ex_b")
	require.Equal(t, want, got)
	require.Len(t, got, 3, "the scenario must have stored exemplars")
}

// dumpHeadAllTypes is dumpHead extended to histogram samples, which it reports
// by their count and sum so that the comparison does not depend on bucket
// layout details.
func dumpHeadAllTypes(t *testing.T, ing *Ingester) map[string][]headSample {
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
		for {
			switch it.Next() {
			case chunkenc.ValNone:
				goto done
			case chunkenc.ValFloat:
				ts, v := it.At()
				out[key] = append(out[key], headSample{T: ts, V: v})
			case chunkenc.ValHistogram:
				ts, h := it.AtHistogram(nil)
				out[key] = append(out[key], headSample{T: ts, V: float64(h.Count) + h.Sum})
			case chunkenc.ValFloatHistogram:
				ts, fh := it.AtFloatHistogram(nil)
				out[key] = append(out[key], headSample{T: ts, V: fh.Count + fh.Sum})
			}
		}
	done:
		require.NoError(t, it.Err())
	}
	require.NoError(t, set.Err())

	for key := range out {
		sort.Slice(out[key], func(i, j int) bool { return out[key][i].T < out[key][j].T })
	}
	return out
}

// dumpExemplars returns every exemplar stored for the named series, sorted so
// the comparison does not depend on query order.
func dumpExemplars(t *testing.T, ing *Ingester, seriesNames ...string) []string {
	t.Helper()

	db, err := ing.getTSDB(coalesceTestUser)
	require.NoError(t, err)

	q, err := db.ExemplarQuerier(context.Background())
	require.NoError(t, err)

	var matchers [][]*labels.Matcher
	for _, name := range seriesNames {
		m, err := labels.NewMatcher(labels.MatchEqual, labels.MetricName, name)
		require.NoError(t, err)
		matchers = append(matchers, []*labels.Matcher{m})
	}

	res, err := q.Select(0, coalesceTestBaseMs*10, matchers...)
	require.NoError(t, err)

	var out []string
	for _, r := range res {
		for _, e := range r.Exemplars {
			out = append(out, r.SeriesLabels.String()+" "+exemplarString(e))
		}
	}
	sort.Strings(out)
	return out
}

func exemplarString(e exemplar.Exemplar) string {
	return e.Labels.String() + "@" + model.Time(e.Ts).String()
}

// A request abandoned while queued keeps its own context error even when the
// batch it was queued in is rolled back because a different request failed.
// Without the explicit dropped flag, re-reading the context during the
// rollback could reclassify it.
func TestPushCoalescer_CanceledRequestKeepsItsErrorThroughRollback(t *testing.T) {
	t.Parallel()

	h := newCoalesceHarness(t, defaultCoalesceConfig(), nil)

	seed, _ := h.task(context.Background(), 0)
	seedDone := h.pushAsync(seed)
	h.waitForFirstCommit()

	dropCtx, cancel := context.WithCancel(context.Background())
	drop, dropAppends := h.task(dropCtx, 1)
	dropDone := h.pushAsync(drop)
	h.waitForPending(1)

	fatalErr := errTestFatalAppend
	bad, _ := h.task(context.Background(), 2)
	bad.appendFn = func(extendedAppender, int64) error { return fatalErr }
	badDone := h.pushAsync(bad)
	h.waitForPending(2)

	other, _ := h.task(context.Background(), 3)
	otherDone := h.pushAsync(other)
	h.waitForPending(3)

	// Abandon the first queued request only after it is safely in the queue,
	// so the leader meets it already canceled.
	cancel()
	h.release()

	require.NoError(t, mustRecv(t, seedDone))
	require.ErrorIs(t, mustRecv(t, dropDone), context.Canceled,
		"a dropped request must keep its context error, not the rollback error")
	require.ErrorIs(t, mustRecv(t, badDone), fatalErr)
	require.ErrorIs(t, mustRecv(t, otherDone), errPushCoalesceBatchRolledBack)
	require.Equal(t, int32(0), *dropAppends)
}
