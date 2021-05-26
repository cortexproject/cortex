package ruler

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

type fakePusher struct {
	request  *cortexpb.WriteRequest
	response *cortexpb.WriteResponse
}

func (p *fakePusher) Push(ctx context.Context, r *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
	p.request = r
	return p.response, nil
}

func TestPusherAppendable(t *testing.T) {
	pusher := &fakePusher{}
	pa := &PusherAppendable{
		pusher: pusher,
		userID: "user-1",
	}

	for _, tc := range []struct {
		name       string
		series     string
		evalDelay  time.Duration
		value      float64
		expectedTS int64
	}{
		{
			name:       "tenant without delay, normal value",
			series:     "foo_bar",
			value:      1.234,
			expectedTS: 120_000,
		},
		{
			name:       "tenant without delay, stale nan value",
			series:     "foo_bar",
			value:      math.Float64frombits(value.StaleNaN),
			expectedTS: 120_000,
		},
		{
			name:       "tenant with delay, normal value",
			series:     "foo_bar",
			value:      1.234,
			expectedTS: 120_000,
			evalDelay:  time.Minute,
		},
		{
			name:       "tenant with delay, stale nan value",
			value:      math.Float64frombits(value.StaleNaN),
			expectedTS: 60_000,
			evalDelay:  time.Minute,
		},
		{
			name:       "ALERTS without delay, normal value",
			series:     `ALERTS{alertname="boop"}`,
			value:      1.234,
			expectedTS: 120_000,
		},
		{
			name:       "ALERTS without delay, stale nan value",
			series:     `ALERTS{alertname="boop"}`,
			value:      math.Float64frombits(value.StaleNaN),
			expectedTS: 120_000,
		},
		{
			name:       "ALERTS with delay, normal value",
			series:     `ALERTS{alertname="boop"}`,
			value:      1.234,
			expectedTS: 60_000,
			evalDelay:  time.Minute,
		},
		{
			name:       "ALERTS with delay, stale nan value",
			series:     `ALERTS_FOR_STATE{alertname="boop"}`,
			value:      math.Float64frombits(value.StaleNaN),
			expectedTS: 60_000,
			evalDelay:  time.Minute,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			pa.rulesLimits = &ruleLimits{
				evalDelay: tc.evalDelay,
			}

			lbls, err := parser.ParseMetric(tc.series)
			require.NoError(t, err)

			pusher.response = &cortexpb.WriteResponse{}
			a := pa.Appender(ctx)
			_, err = a.Append(0, lbls, 120_000, tc.value)
			require.NoError(t, err)

			require.NoError(t, a.Commit())

			require.Equal(t, tc.expectedTS, pusher.request.Timeseries[0].Samples[0].TimestampMs)

		})
	}
}
