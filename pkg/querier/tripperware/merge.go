package tripperware

import (
	"sort"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

// MergeSampleStreams deduplicates sample streams using a map.
func MergeSampleStreams(output map[string]SampleStream, sampleStreams []SampleStream) {
	buf := make([]byte, 0, 1024)
	for _, stream := range sampleStreams {
		metric := string(cortexpb.FromLabelAdaptersToLabels(stream.Labels).Bytes(buf))
		existing, ok := output[metric]
		if !ok {
			existing = SampleStream{
				Labels: stream.Labels,
			}
		}
		// We need to make sure we don't repeat samples. This causes some visualisations to be broken in Grafana.
		// The prometheus API is inclusive of start and end timestamps.
		if len(existing.Samples) > 0 && len(stream.Samples) > 0 {
			existingEndTs := existing.Samples[len(existing.Samples)-1].TimestampMs
			if existingEndTs == stream.Samples[0].TimestampMs {
				// Typically this the cases where only 1 sample point overlap,
				// so optimize with simple code.
				stream.Samples = stream.Samples[1:]
			} else if existingEndTs > stream.Samples[0].TimestampMs {
				// Overlap might be big, use heavier algorithm to remove overlap.
				stream.Samples = sliceSamples(stream.Samples, existingEndTs)
			} // else there is no overlap, yay!
		}
		// Same for histograms as for samples above.
		if len(existing.Histograms) > 0 && len(stream.Histograms) > 0 {
			existingEndTs := existing.Histograms[len(existing.Histograms)-1].GetTimestampMs()
			if existingEndTs == stream.Histograms[0].GetTimestampMs() {
				stream.Histograms = stream.Histograms[1:]
			} else if existingEndTs > stream.Histograms[0].GetTimestampMs() {
				stream.Histograms = sliceHistograms(stream.Histograms, existingEndTs)
			}
		}
		existing.Samples = append(existing.Samples, stream.Samples...)
		existing.Histograms = append(existing.Histograms, stream.Histograms...)

		output[metric] = existing
	}
}

// sliceSamples assumes given samples are sorted by timestamp in ascending order and
// return a sub slice whose first element's is the smallest timestamp that is strictly
// bigger than the given minTs. Empty slice is returned if minTs is bigger than all the
// timestamps in samples.
// If the given samples slice is not sorted then unexpected samples would be returned.
func sliceSamples(samples []cortexpb.Sample, minTs int64) []cortexpb.Sample {
	if len(samples) == 0 || minTs < samples[0].TimestampMs {
		return samples
	}

	if len(samples) > 0 && minTs > samples[len(samples)-1].TimestampMs {
		return samples[len(samples):]
	}

	searchResult := sort.Search(len(samples), func(i int) bool {
		return samples[i].TimestampMs > minTs
	})

	return samples[searchResult:]
}

// sliceHistogram assumes given histogram are sorted by timestamp in ascending order and
// return a sub slice whose first element's is the smallest timestamp that is strictly
// bigger than the given minTs. Empty slice is returned if minTs is bigger than all the
// timestamps in histogram.
func sliceHistograms(histograms []SampleHistogramPair, minTs int64) []SampleHistogramPair {
	if len(histograms) <= 0 || minTs < histograms[0].GetTimestampMs() {
		return histograms
	}

	if len(histograms) > 0 && minTs > histograms[len(histograms)-1].GetTimestampMs() {
		return histograms[len(histograms):]
	}

	searchResult := sort.Search(len(histograms), func(i int) bool {
		return histograms[i].GetTimestampMs() > minTs
	})

	return histograms[searchResult:]
}
