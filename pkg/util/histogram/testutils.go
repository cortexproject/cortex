// Copyright 2023 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package histogram

import "github.com/prometheus/prometheus/model/histogram"

// Adapted from Prometheus model/histogram/test_utils.go GenerateBigTestHistograms.
func GenerateTestHistograms(from, step, numHistograms, numSpans, numBuckets int) []*histogram.Histogram {
	bucketsPerSide := numBuckets / 2
	spanLength := uint32(bucketsPerSide / numSpans)
	// Given all bucket deltas are 1, sum bucketsPerSide + 1.
	observationCount := bucketsPerSide * (1 + bucketsPerSide)

	var histograms []*histogram.Histogram
	for i := 0; i < numHistograms; i++ {
		v := from + i*step
		h := &histogram.Histogram{
			CounterResetHint: histogram.GaugeType,
			Count:            uint64(v + observationCount),
			ZeroCount:        uint64(v),
			ZeroThreshold:    1e-128,
			Sum:              18.4 * float64(v+1),
			Schema:           2,
			NegativeSpans:    make([]histogram.Span, numSpans),
			PositiveSpans:    make([]histogram.Span, numSpans),
			NegativeBuckets:  make([]int64, bucketsPerSide),
			PositiveBuckets:  make([]int64, bucketsPerSide),
		}

		for j := 0; j < numSpans; j++ {
			s := histogram.Span{Offset: 1, Length: spanLength}
			h.NegativeSpans[j] = s
			h.PositiveSpans[j] = s
		}

		for j := 0; j < bucketsPerSide; j++ {
			h.NegativeBuckets[j] = 1
			h.PositiveBuckets[j] = 1
		}

		histograms = append(histograms, h)
	}
	return histograms
}

func GenerateTestHistogram(i int) *histogram.Histogram {
	bucketsPerSide := 10
	spanLength := uint32(2)
	// Given all bucket deltas are 1, sum bucketsPerSide + 1.
	observationCount := bucketsPerSide * (1 + bucketsPerSide)

	v := 10 + i
	h := &histogram.Histogram{
		CounterResetHint: histogram.GaugeType,
		Count:            uint64(v + observationCount),
		ZeroCount:        uint64(v),
		ZeroThreshold:    1e-128,
		Sum:              18.4 * float64(v+1),
		Schema:           2,
		NegativeSpans:    make([]histogram.Span, 5),
		PositiveSpans:    make([]histogram.Span, 5),
		NegativeBuckets:  make([]int64, bucketsPerSide),
		PositiveBuckets:  make([]int64, bucketsPerSide),
	}

	for j := 0; j < 5; j++ {
		s := histogram.Span{Offset: 1, Length: spanLength}
		h.NegativeSpans[j] = s
		h.PositiveSpans[j] = s
	}

	for j := 0; j < bucketsPerSide; j++ {
		h.NegativeBuckets[j] = 1
		h.PositiveBuckets[j] = 1
	}

	return h
}

func GenerateTestFloatHistogram(i int) *histogram.FloatHistogram {
	return GenerateTestHistogram(i).ToFloat(nil)
}
