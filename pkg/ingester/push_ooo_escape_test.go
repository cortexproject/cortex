package ingester

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// How far outside the out-of-order window can a sample get in, when two
// requests for the same series share one batch?
//
// The divergence window is derivable: a batch's appender keeps the head max
// time H it snapshotted before the batch, while separate commits would let the
// first request advance it to H' = max(H, t_X). A sample at t_Y is therefore
// accepted by the batch but rejected by separate commits exactly when
//
//	H - W <= t_Y < t_X - W
//
// so the band of timestamps that behave differently is (t_X - H) wide: the
// amount the batch's own newest sample advances the head max. In steady state
// that is one scrape interval, not a window's worth.
//
// This test pins that bound by placing t_Y just inside and just outside the
// band and showing where the stored data starts to differ.
func TestIngesterPushCoalescing_OutOfOrderEscapeIsBoundedByInBatchHeadAdvance(t *testing.T) {
	const (
		base = coalesceTestBaseMs
		win  = coalesceTestOOOWindowMs

		// How far beyond the pre-batch head max the batch's newest sample sits.
		// A scrape interval, which is what it is in steady state.
		advance = int64(15000)
	)

	for _, tc := range []struct {
		name string
		// Offset of t_Y relative to the band's lower edge, base-win.
		offsetIntoBand int64
		inBand         bool
	}{
		{
			name:           "t_Y inside the band: the batch stores a sample separate commits reject",
			offsetIntoBand: advance / 2,
			inBand:         true,
		},
		{
			name:           "t_Y below the band: both reject it, only the error differs",
			offsetIntoBand: -5000,
			inBand:         false,
		},
		{
			name:           "t_Y above the band: both accept it",
			offsetIntoBand: advance + 5000,
			inBand:         false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			series := fmt.Sprintf("escape_%d", tc.offsetIntoBand)
			tX := base + advance
			tY := base - win + tc.offsetIntoBand

			specs := []writeRequestSpec{
				oneSample(series, tX, 1),
				oneSample(series, tY, 2),
			}

			sequential, _ := startCoalesceTestIngester(t, PushCoalesceConfig{})
			for _, err := range pushSequentially(t, sequential, []writeRequestSpec{
				oneSample("coalesce_gate_seed", base, 1),
				oneSample("coalesce_gate_holder", base, 1),
			}) {
				require.NoError(t, err)
			}
			wantErrs := pushSequentially(t, sequential, specs)

			coalesced, _ := startCoalesceTestIngester(t, defaultCoalesceConfig())
			gotErrs := pushAsOneBatch(t, coalesced, specs)

			wantSamples := dumpHead(t, sequential)[coalesceTestSeries(series).String()]
			gotSamples := dumpHead(t, coalesced)[coalesceTestSeries(series).String()]

			t.Logf("t_X - H = %d ms, t_Y - (H - W) = %d ms", advance, tc.offsetIntoBand)
			t.Logf("separate: errs=%v samples=%v", errStrings(wantErrs), wantSamples)
			t.Logf("coalesced: errs=%v samples=%v", errStrings(gotErrs), gotSamples)

			if tc.inBand {
				require.Len(t, wantSamples, 1, "separate commits reject the out-of-window sample")
				require.Len(t, gotSamples, 2, "the batch accepts it")
				return
			}
			require.Equal(t, wantSamples, gotSamples,
				"outside the band the stored data must be identical")
		})
	}
}
