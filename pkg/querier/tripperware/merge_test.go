package tripperware

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	ingester_client "github.com/cortexproject/cortex/pkg/ingester/client"
)

func TestMergeSampleStreams(t *testing.T) {
	lbls := labels.FromMap(map[string]string{
		"foo":     "bar",
		"cluster": "test",
	})
	lbls1 := labels.FromMap(map[string]string{
		"foo":     "zoo",
		"cluster": "test",
	})
	for _, tc := range []struct {
		name           string
		sampleStreams  []SampleStream
		expectedOutput map[string]SampleStream
	}{
		{
			name: "one sample stream",
			sampleStreams: []SampleStream{
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Samples: []cortexpb.Sample{
						{Value: 0, TimestampMs: 0},
					},
				},
			},
			expectedOutput: map[string]SampleStream{
				ingester_client.LabelsToKeyString(lbls): {
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Samples: []cortexpb.Sample{
						{Value: 0, TimestampMs: 0},
					},
				},
			},
		},
		{
			name: "two sample streams with only one metric",
			sampleStreams: []SampleStream{
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Samples: []cortexpb.Sample{
						{Value: 0, TimestampMs: 0},
						{Value: 2, TimestampMs: 2},
						{Value: 3, TimestampMs: 3},
					},
				},
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Samples: []cortexpb.Sample{
						{Value: 0, TimestampMs: 0},
						{Value: 1, TimestampMs: 1},
						{Value: 4, TimestampMs: 4},
					},
				},
			},
			expectedOutput: map[string]SampleStream{
				ingester_client.LabelsToKeyString(lbls): {
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Samples: []cortexpb.Sample{
						{Value: 0, TimestampMs: 0},
						{Value: 2, TimestampMs: 2},
						{Value: 3, TimestampMs: 3},
						{Value: 4, TimestampMs: 4},
					},
				},
			},
		},
		{
			name: "two sample streams with two metrics",
			sampleStreams: []SampleStream{
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Samples: []cortexpb.Sample{
						{Value: 0, TimestampMs: 0},
						{Value: 2, TimestampMs: 2},
						{Value: 3, TimestampMs: 3},
					},
				},
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Samples: []cortexpb.Sample{
						{Value: 1, TimestampMs: 1},
						{Value: 4, TimestampMs: 4},
					},
				},
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls1),
					Samples: []cortexpb.Sample{
						{Value: 0, TimestampMs: 0},
						{Value: 1, TimestampMs: 1},
						{Value: 4, TimestampMs: 4},
					},
				},
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls1),
					Samples: []cortexpb.Sample{
						{Value: 2, TimestampMs: 2},
						{Value: 3, TimestampMs: 3},
					},
				},
			},
			expectedOutput: map[string]SampleStream{
				ingester_client.LabelsToKeyString(lbls): {
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Samples: []cortexpb.Sample{
						{Value: 0, TimestampMs: 0},
						{Value: 2, TimestampMs: 2},
						{Value: 3, TimestampMs: 3},
						{Value: 4, TimestampMs: 4},
					},
				},
				ingester_client.LabelsToKeyString(lbls1): {
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls1),
					Samples: []cortexpb.Sample{
						{Value: 0, TimestampMs: 0},
						{Value: 1, TimestampMs: 1},
						{Value: 4, TimestampMs: 4},
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := make(map[string]SampleStream)
			MergeSampleStreams(output, tc.sampleStreams)
			assert.Equal(t, tc.expectedOutput, output)
		})
	}
}

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
			actual := sliceSamples(tc.samples, tc.minTs)
			assert.Equal(t, tc.expectedSamples, actual)
		})
	}
}
