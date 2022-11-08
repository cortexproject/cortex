package tripperware

import (
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSliceSamples(t *testing.T) {
	for _, tc := range []struct {
		name            string
		samples         []cortexpb.Sample
		minTs           int64
		expectedSamples []cortexpb.Sample
	}{
		{
			name:            "empty samples",
			samples:         nil,
			expectedSamples: nil,
		},
		{
			name: "minTs smaller than first sample's timestamp",
			samples: []cortexpb.Sample{
				{Value: 0, TimestampMs: 1},
			},
			minTs: 0,
			expectedSamples: []cortexpb.Sample{
				{Value: 0, TimestampMs: 1},
			},
		},
		{
			name: "input samples are not sorted, return all samples",
			samples: []cortexpb.Sample{
				{Value: 0, TimestampMs: 3}, {Value: 0, TimestampMs: 1},
			},
			minTs: 2,
			expectedSamples: []cortexpb.Sample{
				{Value: 0, TimestampMs: 3}, {Value: 0, TimestampMs: 1},
			},
		},
		{
			name: "minTs greater than the last sample's timestamp",
			samples: []cortexpb.Sample{
				{Value: 0, TimestampMs: 1}, {Value: 0, TimestampMs: 2},
			},
			minTs:           3,
			expectedSamples: []cortexpb.Sample{},
		},
		{
			name: "input samples not sorted, minTs greater than the last sample's timestamp",
			samples: []cortexpb.Sample{
				{Value: 0, TimestampMs: 0}, {Value: 0, TimestampMs: 3}, {Value: 0, TimestampMs: 1},
			},
			minTs:           2,
			expectedSamples: []cortexpb.Sample{},
		},
		{
			name: "input samples are sorted",
			samples: []cortexpb.Sample{
				{Value: 0, TimestampMs: 2}, {Value: 0, TimestampMs: 3}, {Value: 0, TimestampMs: 4},
			},
			minTs: 1,
			expectedSamples: []cortexpb.Sample{
				{Value: 0, TimestampMs: 2}, {Value: 0, TimestampMs: 3}, {Value: 0, TimestampMs: 4},
			},
		},
		{
			name: "input samples are sorted, get sliced samples",
			samples: []cortexpb.Sample{
				{Value: 0, TimestampMs: 1}, {Value: 0, TimestampMs: 2}, {Value: 0, TimestampMs: 3},
			},
			minTs: 2,
			expectedSamples: []cortexpb.Sample{
				{Value: 0, TimestampMs: 3},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := SliceSamples(tc.samples, tc.minTs)
			assert.Equal(t, tc.expectedSamples, actual)
		})
	}
}
