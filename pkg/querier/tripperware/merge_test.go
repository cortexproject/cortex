package tripperware

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	ingester_client "github.com/cortexproject/cortex/pkg/ingester/client"
)

var (
	testHistogram1 = SampleHistogram{
		Count: 13.5,
		Sum:   3897.1,
		Buckets: []*HistogramBucket{
			{
				Boundaries: 1,
				Lower:      -4870.992343051145,
				Upper:      -4466.7196729968955,
				Count:      1,
			},
			{
				Boundaries: 1,
				Lower:      -861.0779292198035,
				Upper:      -789.6119426088657,
				Count:      2,
			},
			{
				Boundaries: 1,
				Lower:      -558.3399591246119,
				Upper:      -512,
				Count:      3,
			},
			{
				Boundaries: 0,
				Lower:      2048,
				Upper:      2233.3598364984477,
				Count:      1.5,
			},
			{
				Boundaries: 0,
				Lower:      2896.3093757400984,
				Upper:      3158.4477704354626,
				Count:      2.5,
			},
			{
				Boundaries: 0,
				Lower:      4466.7196729968955,
				Upper:      4870.992343051145,
				Count:      3.5,
			},
		},
	}

	testHistogram2 = SampleHistogram{
		Count: 10,
		Sum:   100,
	}
)

func TestMergeSampleStreams(t *testing.T) {
	t.Parallel()
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
			name: "one sample stream with histograms",
			sampleStreams: []SampleStream{
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
					},
				},
			},
			expectedOutput: map[string]SampleStream{
				ingester_client.LabelsToKeyString(lbls): {
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
					},
				},
			},
		},
		{
			name: "one sample stream with both samples and histograms",
			sampleStreams: []SampleStream{
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Samples: []cortexpb.Sample{
						{Value: 0, TimestampMs: 0},
					},
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
					},
				},
			},
			expectedOutput: map[string]SampleStream{
				ingester_client.LabelsToKeyString(lbls): {
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Samples: []cortexpb.Sample{
						{Value: 0, TimestampMs: 0},
					},
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
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
			name: "two sample streams with only one metric, histograms",
			sampleStreams: []SampleStream{
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
						{Histogram: testHistogram1, TimestampMs: 2},
						{Histogram: testHistogram1, TimestampMs: 3},
					},
				},
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
						{Histogram: testHistogram1, TimestampMs: 1},
						{Histogram: testHistogram1, TimestampMs: 4},
					},
				},
			},
			expectedOutput: map[string]SampleStream{
				ingester_client.LabelsToKeyString(lbls): {
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
						{Histogram: testHistogram1, TimestampMs: 2},
						{Histogram: testHistogram1, TimestampMs: 3},
						{Histogram: testHistogram1, TimestampMs: 4},
					},
				},
			},
		},
		{
			name: "two sample streams with only one metric, samples and histograms",
			sampleStreams: []SampleStream{
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Samples: []cortexpb.Sample{
						{Value: 0, TimestampMs: 0},
						{Value: 2, TimestampMs: 2},
						{Value: 3, TimestampMs: 3},
					},
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
						{Histogram: testHistogram1, TimestampMs: 2},
						{Histogram: testHistogram1, TimestampMs: 3},
					},
				},
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Samples: []cortexpb.Sample{
						{Value: 0, TimestampMs: 0},
						{Value: 1, TimestampMs: 1},
						{Value: 4, TimestampMs: 4},
					},
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
						{Histogram: testHistogram1, TimestampMs: 1},
						{Histogram: testHistogram1, TimestampMs: 4},
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
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
						{Histogram: testHistogram1, TimestampMs: 2},
						{Histogram: testHistogram1, TimestampMs: 3},
						{Histogram: testHistogram1, TimestampMs: 4},
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
		{
			name: "two sample streams with two metrics, histograms",
			sampleStreams: []SampleStream{
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
						{Histogram: testHistogram1, TimestampMs: 2},
						{Histogram: testHistogram1, TimestampMs: 3},
					},
				},
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram2, TimestampMs: 1},
						{Histogram: testHistogram2, TimestampMs: 4},
					},
				},
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls1),
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
						{Histogram: testHistogram1, TimestampMs: 1},
						{Histogram: testHistogram1, TimestampMs: 4},
					},
				},
				{
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls1),
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram2, TimestampMs: 2},
						{Histogram: testHistogram2, TimestampMs: 3},
					},
				},
			},
			expectedOutput: map[string]SampleStream{
				ingester_client.LabelsToKeyString(lbls): {
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
						{Histogram: testHistogram1, TimestampMs: 2},
						{Histogram: testHistogram1, TimestampMs: 3},
						{Histogram: testHistogram2, TimestampMs: 4},
					},
				},
				ingester_client.LabelsToKeyString(lbls1): {
					Labels: cortexpb.FromLabelsToLabelAdapters(lbls1),
					Histograms: []SampleHistogramPair{
						{Histogram: testHistogram1, TimestampMs: 0},
						{Histogram: testHistogram1, TimestampMs: 1},
						{Histogram: testHistogram1, TimestampMs: 4},
					},
				},
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			output := make(map[string]SampleStream)
			MergeSampleStreams(output, tc.sampleStreams)
			assert.Equal(t, tc.expectedOutput, output)
		})
	}
}

func TestSliceSamples(t *testing.T) {
	t.Parallel()
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			actual := sliceSamples(tc.samples, tc.minTs)
			assert.Equal(t, tc.expectedSamples, actual)
		})
	}
}

func TestSliceHistograms(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name               string
		histograms         []SampleHistogramPair
		minTs              int64
		expectedHistograms []SampleHistogramPair
	}{
		{name: "empty histograms"},
		{
			name: "minTs smaller than first histogram's timestamp",
			histograms: []SampleHistogramPair{
				{
					TimestampMs: 1,
					Histogram:   testHistogram1,
				},
			},
			minTs: 0,
			expectedHistograms: []SampleHistogramPair{
				{
					TimestampMs: 1,
					Histogram:   testHistogram1,
				},
			},
		},
		{
			name: "input histograms are not sorted, return all histograms",
			histograms: []SampleHistogramPair{
				{
					TimestampMs: 3,
					Histogram:   testHistogram1,
				},
				{
					TimestampMs: 1,
					Histogram:   testHistogram1,
				},
			},
			minTs: 2,
			expectedHistograms: []SampleHistogramPair{
				{
					TimestampMs: 3,
					Histogram:   testHistogram1,
				},
				{
					TimestampMs: 1,
					Histogram:   testHistogram1,
				},
			},
		},
		{
			name: "minTs greater than the last histogram's timestamp",
			histograms: []SampleHistogramPair{
				{
					TimestampMs: 1,
					Histogram:   testHistogram1,
				},
				{
					TimestampMs: 2,
					Histogram:   testHistogram1,
				},
			},
			minTs:              3,
			expectedHistograms: []SampleHistogramPair{},
		},
		{
			name: "input histograms not sorted, minTs greater than the last histogram's timestamp",
			histograms: []SampleHistogramPair{
				{
					TimestampMs: 0,
					Histogram:   testHistogram1,
				},
				{
					TimestampMs: 3,
					Histogram:   testHistogram1,
				},
				{
					TimestampMs: 1,
					Histogram:   testHistogram1,
				},
			},
			minTs:              2,
			expectedHistograms: []SampleHistogramPair{},
		},
		{
			name: "input histograms are sorted",
			histograms: []SampleHistogramPair{
				{
					TimestampMs: 2,
					Histogram:   testHistogram1,
				},
				{
					TimestampMs: 3,
					Histogram:   testHistogram1,
				},
				{
					TimestampMs: 4,
					Histogram:   testHistogram1,
				},
			},
			minTs: 1,
			expectedHistograms: []SampleHistogramPair{
				{
					TimestampMs: 2,
					Histogram:   testHistogram1,
				},
				{
					TimestampMs: 3,
					Histogram:   testHistogram1,
				},
				{
					TimestampMs: 4,
					Histogram:   testHistogram1,
				},
			},
		},
		{
			name: "input histograms are sorted, get sliced histograms",
			histograms: []SampleHistogramPair{
				{
					TimestampMs: 1,
					Histogram:   testHistogram1,
				},
				{
					TimestampMs: 2,
					Histogram:   testHistogram1,
				},
				{
					TimestampMs: 3,
					Histogram:   testHistogram1,
				},
			},
			minTs: 2,
			expectedHistograms: []SampleHistogramPair{
				{
					TimestampMs: 3,
					Histogram:   testHistogram1,
				},
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			actual := sliceHistograms(tc.histograms, tc.minTs)
			assert.Equal(t, tc.expectedHistograms, actual)
		})
	}
}
