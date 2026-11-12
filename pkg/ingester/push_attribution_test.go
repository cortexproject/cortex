package ingester

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// A leader runs the append phase on behalf of every request in its batch, so
// everything that phase records has to stay attributed to the request it
// belongs to. The implementation relies on the append phase being a closure
// over each request's own locals, with the metrics emitted afterwards by the
// request's own goroutine. These tests check that it actually works out that
// way, rather than taking the closure scoping on trust.
//
// The requests below write distinct series on purpose: a request whose verdict
// depends on another request in the same batch would be testing the divergence
// covered by TestIngesterPushCoalescing_DivergenceFromSeparateCommits instead
// of testing attribution.

// attributionSpecs builds a batch where several requests fail for different
// reasons, interleaved with requests that succeed. Every failure is decided
// against committed state, so a batch must produce the same per-request result
// as separate commits.
func attributionSpecs() []writeRequestSpec {
	const (
		base = coalesceTestBaseMs
		win  = coalesceTestOOOWindowMs
	)
	return []writeRequestSpec{
		oneSample("attr_ok_0", base+1000, 1),
		// Far enough back that the out-of-order window cannot take it.
		oneSample("attr_too_old", base-4*win, 2),
		oneSample("attr_ok_1", base+1000, 3),
		// attr_dup is seeded with value 1 before the batch, so this is a
		// duplicate against committed state and is rejected at Append time.
		oneSample("attr_dup", base+1000, 99),
		oneSample("attr_ok_2", base+1000, 4),
		oneSample("attr_too_old_2", base-5*win, 5),
		oneSample("attr_ok_3", base+1000, 6),
	}
}

// seedAttributionState gives attr_dup a committed value, so that the batch's
// duplicate is decided against committed state rather than against another
// request in the same batch.
func seedAttributionState(t *testing.T, ing *Ingester) {
	t.Helper()
	for _, err := range pushSequentially(t, ing, []writeRequestSpec{
		oneSample("attr_dup", coalesceTestBaseMs+1000, 1),
	}) {
		require.NoError(t, err)
	}
}

// mirrorBatchHarnessPushes replays, on an ingester driven sequentially, the two
// requests pushAsOneBatch issues of its own accord: one to create the tenant's
// TSDB and one to hold a commit open. Without this the two sides would have
// pushed a different number of requests and their counters could not be
// compared.
func mirrorBatchHarnessPushes(t *testing.T, ing *Ingester) {
	t.Helper()
	for _, err := range pushSequentially(t, ing, []writeRequestSpec{
		oneSample("coalesce_gate_seed", coalesceTestBaseMs, 1),
		oneSample("coalesce_gate_holder", coalesceTestBaseMs, 1),
	}) {
		require.NoError(t, err)
	}
}

// Each request in a batch must receive its own error, and only its own.
func TestIngesterPushCoalescing_ErrorsStayAttributedToTheirRequest(t *testing.T) {
	specs := attributionSpecs()

	coalesced, _ := startCoalesceTestIngester(t, defaultCoalesceConfig())
	seedAttributionState(t, coalesced)
	got := pushAsOneBatch(t, coalesced, specs)

	require.Len(t, got, 7)

	// The successful requests must not have picked up a neighbour's error.
	for _, idx := range []int{0, 2, 4, 6} {
		require.NoErrorf(t, got[idx], "request %d writes only a good sample", idx)
	}

	// Each failing request must carry the error for its own series and reason.
	require.ErrorContains(t, got[1], "too old sample")
	require.ErrorContains(t, got[1], `series={__name__="attr_too_old"}`)

	require.ErrorContains(t, got[3], "duplicate sample for timestamp")
	require.ErrorContains(t, got[3], `series={__name__="attr_dup"}`)

	require.ErrorContains(t, got[5], "too old sample")
	require.ErrorContains(t, got[5], `series={__name__="attr_too_old_2"}`)

	// And separate commits must agree, so the attribution is not merely
	// self-consistent but identical to the uncoalesced path.
	sequential, _ := startCoalesceTestIngester(t, PushCoalesceConfig{})
	seedAttributionState(t, sequential)
	mirrorBatchHarnessPushes(t, sequential)
	want := pushSequentially(t, sequential, specs)
	require.Equal(t, errStrings(want), errStrings(got))
}

// The per-user counters the append phase feeds must total the same whether one
// leader ran seven append phases or seven goroutines ran their own. A shared
// counter in the wrong scope would show up here as double counting or as
// counts that went missing.
func TestIngesterPushCoalescing_PerUserMetricTotalsMatchSeparateCommits(t *testing.T) {
	specs := attributionSpecs()

	// Metrics that must agree. Deliberately not a whole-registry comparison:
	// the commit duration histogram counts commits, not requests, and the
	// coalescer's own metrics exist on one side only.
	names := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_discarded_samples_total",
	}

	sequential, seqReg := startCoalesceTestIngester(t, PushCoalesceConfig{})
	seedAttributionState(t, sequential)
	mirrorBatchHarnessPushes(t, sequential)
	for _, err := range pushSequentially(t, sequential, specs) {
		_ = err // errors are asserted by the test above
	}

	coalesced, coalReg := startCoalesceTestIngester(t, defaultCoalesceConfig())
	seedAttributionState(t, coalesced)
	pushAsOneBatch(t, coalesced, specs)

	want := gatherValues(t, seqReg, names)
	got := gatherValues(t, coalReg, names)

	require.NotEmpty(t, want, "the scenario must move the metrics it claims to compare")
	require.Equal(t, want, got)

	// Spell out what the scenario is expected to have produced, so that a
	// change which happens to keep the two sides equal but wrong still fails.
	require.Equal(t, 7.0, want["cortex_ingester_ingested_samples_total{}"],
		"4 good samples in the batch, plus attr_dup and the two harness pushes")
	require.Equal(t, 3.0, want["cortex_ingester_ingested_samples_failures_total{}"])
	require.Equal(t, 2.0, want[`cortex_discarded_samples_total{reason="sample-too-old",user="`+coalesceTestUser+`"}`])
	require.Equal(t, 1.0, want[`cortex_discarded_samples_total{reason="new-value-for-timestamp",user="`+coalesceTestUser+`"}`])
}

// gatherValues flattens the named metric families into label-keyed values.
func gatherValues(t *testing.T, reg *prometheus.Registry, names []string) map[string]float64 {
	t.Helper()

	wanted := make(map[string]struct{}, len(names))
	for _, n := range names {
		wanted[n] = struct{}{}
	}

	families, err := reg.Gather()
	require.NoError(t, err)

	out := map[string]float64{}
	for _, f := range families {
		if _, ok := wanted[f.GetName()]; !ok {
			continue
		}
		for _, m := range f.GetMetric() {
			key := f.GetName() + "{"
			for i, l := range m.GetLabel() {
				if i > 0 {
					key += ","
				}
				key += l.GetName() + `="` + l.GetValue() + `"`
			}
			key += "}"
			out[key] = metricValue(m)
		}
	}
	return out
}

func metricValue(m *dto.Metric) float64 {
	switch {
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	default:
		return 0
	}
}
