package tripperware

import (
	"context"
	"fmt"
	"sort"

	"github.com/prometheus/common/model"
	promqlparser "github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/thanos/pkg/strutil"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

const StatusSuccess = "success"

type byFirstTime []*PrometheusResponse

func (a byFirstTime) Len() int           { return len(a) }
func (a byFirstTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byFirstTime) Less(i, j int) bool { return a[i].minTime() < a[j].minTime() }

func (resp *PrometheusResponse) minTime() int64 {
	data := resp.GetData()
	res := data.GetResult()
	// minTime should only be called when the response is fron range query.
	matrix := res.GetMatrix()
	sampleStreams := matrix.GetSampleStreams()
	if len(sampleStreams) == 0 {
		return -1
	}
	if len(sampleStreams[0].Samples) == 0 {
		return -1
	}
	return sampleStreams[0].Samples[0].TimestampMs
}

// MergeResponse merges multiple Response into one.
func MergeResponse(ctx context.Context, sumStats bool, req Request, responses ...Response) (Response, error) {
	if len(responses) == 1 {
		return responses[0], nil
	}
	promResponses := make([]*PrometheusResponse, 0, len(responses))
	warnings := make([][]string, 0, len(responses))
	infos := make([][]string, 0, len(responses))
	for _, resp := range responses {
		promResponses = append(promResponses, resp.(*PrometheusResponse))
		if w := resp.(*PrometheusResponse).Warnings; w != nil {
			warnings = append(warnings, w)
		}
		if i := resp.(*PrometheusResponse).Infos; i != nil {
			infos = append(infos, i)
		}
	}

	// Check if it is a range query. Range query passed req as nil since
	// we only use request when result type is a vector.
	if req == nil {
		sort.Sort(byFirstTime(promResponses))
	}
	var data PrometheusData
	// For now, we only shard queries that returns a vector.
	switch promResponses[0].Data.ResultType {
	case model.ValVector.String():
		v, err := vectorMerge(ctx, req, promResponses)
		if err != nil {
			return nil, err
		}
		data = PrometheusData{
			ResultType: model.ValVector.String(),
			Result: PrometheusQueryResult{
				Result: &PrometheusQueryResult_Vector{
					Vector: v,
				},
			},
			Stats: statsMerge(sumStats, promResponses),
		}
	case model.ValMatrix.String():
		sampleStreams, err := matrixMerge(ctx, promResponses)
		if err != nil {
			return nil, err
		}

		data = PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result: PrometheusQueryResult{
				Result: &PrometheusQueryResult_Matrix{
					Matrix: &Matrix{
						SampleStreams: sampleStreams,
					},
				},
			},
			Stats: statsMerge(sumStats, promResponses),
		}
	default:
		return nil, fmt.Errorf("unexpected result type: %s", promResponses[0].Data.ResultType)
	}

	res := &PrometheusResponse{
		Status:   StatusSuccess,
		Data:     data,
		Warnings: strutil.MergeUnsortedSlices(0, warnings...),
		Infos:    strutil.MergeUnsortedSlices(0, infos...),
	}
	return res, nil
}

func matrixMerge(ctx context.Context, resps []*PrometheusResponse) ([]SampleStream, error) {
	output := make(map[string]SampleStream)
	for _, resp := range resps {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if resp == nil {
			continue
		}
		if resp.Data.Result.GetMatrix() == nil {
			continue
		}
		mergeSampleStreams(output, resp.Data.Result.GetMatrix().GetSampleStreams())
	}

	keys := make([]string, 0, len(output))
	for key := range output {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make([]SampleStream, 0, len(output))
	for _, key := range keys {
		result = append(result, output[key])
	}

	return result, nil
}

func vectorMerge(ctx context.Context, req Request, resps []*PrometheusResponse) (*Vector, error) {
	output := map[string]Sample{}
	metrics := []string{} // Used to preserve the order for topk and bottomk.
	sortPlan, err := sortPlanForQuery(req.GetQuery())
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 1024)
	for _, resp := range resps {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if resp == nil {
			continue
		}
		// Merge vector result samples only. Skip other types such as
		// string, scalar as those are not sharable.
		if resp.Data.Result.GetVector() == nil {
			continue
		}
		for _, sample := range resp.Data.Result.GetVector().Samples {
			s := sample
			metric := string(cortexpb.FromLabelAdaptersToLabels(sample.Labels).Bytes(buf))
			if existingSample, ok := output[metric]; !ok {
				output[metric] = s
				metrics = append(metrics, metric) // Preserve the order of metric.
			} else if existingSample.GetTimestampMs() < s.GetTimestampMs() {
				// Choose the latest sample if we see overlap.
				output[metric] = s
			}
		}
	}

	result := &Vector{
		Samples: make([]Sample, 0, len(output)),
	}

	if len(output) == 0 {
		return result, nil
	}

	if sortPlan == mergeOnly {
		for _, k := range metrics {
			result.Samples = append(result.Samples, output[k])
		}
		return result, nil
	}

	samples := make([]*pair, 0, len(output))
	for k, v := range output {
		samples = append(samples, &pair{
			metric: k,
			s:      v,
		})
	}

	// TODO: What if we have mixed float and histogram samples in the response?
	// Then the sorting behavior is undefined. Prometheus doesn't handle it.
	sort.Slice(samples, func(i, j int) bool {
		// Order is determined by vector.
		switch sortPlan {
		case sortByValuesAsc:
			return getSortValueFromPair(samples, i) < getSortValueFromPair(samples, j)
		case sortByValuesDesc:
			return getSortValueFromPair(samples, i) > getSortValueFromPair(samples, j)
		}
		return samples[i].metric < samples[j].metric
	})

	for _, p := range samples {
		result.Samples = append(result.Samples, p.s)
	}
	return result, nil
}

// statsMerge merge the stats from 2 responses this function is similar to matrixMerge
func statsMerge(shouldSumStats bool, resps []*PrometheusResponse) *PrometheusResponseStats {
	output := map[int64]*PrometheusResponseQueryableSamplesStatsPerStep{}
	hasStats := false
	var peakSamples int64
	for _, resp := range resps {
		if resp.Data.Stats == nil {
			continue
		}

		hasStats = true
		if resp.Data.Stats.Samples == nil {
			continue
		}

		for _, s := range resp.Data.Stats.Samples.TotalQueryableSamplesPerStep {
			if shouldSumStats {
				if stats, ok := output[s.GetTimestampMs()]; ok {
					stats.Value += s.Value
				} else {
					output[s.GetTimestampMs()] = s
				}
			} else {
				output[s.GetTimestampMs()] = s
			}
		}
		peakSamples = max(peakSamples, resp.Data.Stats.Samples.PeakSamples)
	}

	if !hasStats {
		return nil
	}
	keys := make([]int64, 0, len(output))
	for key := range output {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	result := &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{}}
	for _, key := range keys {
		result.Samples.TotalQueryableSamplesPerStep = append(result.Samples.TotalQueryableSamplesPerStep, output[key])
		result.Samples.TotalQueryableSamples += output[key].Value
	}
	result.Samples.PeakSamples = peakSamples

	return result
}

type sortPlan int

const (
	mergeOnly        sortPlan = 0
	sortByValuesAsc  sortPlan = 1
	sortByValuesDesc sortPlan = 2
	sortByLabels     sortPlan = 3
)

type pair struct {
	metric string
	s      Sample
}

// getSortValueFromPair gets the float value used for sorting from samples.
// If float sample, use sample value. If histogram sample, use histogram sum.
// This is the same behavior as Prometheus https://github.com/prometheus/prometheus/blob/v2.53.0/promql/functions.go#L1595.
func getSortValueFromPair(samples []*pair, i int) float64 {
	if samples[i].s.Histogram != nil {
		return samples[i].s.Histogram.Histogram.Sum
	}
	// Impossible to have both histogram and sample nil.
	return samples[i].s.Sample.Value
}

func sortPlanForQuery(q string) (sortPlan, error) {
	expr, err := promqlparser.ParseExpr(q)
	if err != nil {
		return 0, err
	}
	// Check if the root expression is topk, bottomk, limitk, or limit_ratio
	if aggr, ok := expr.(*promqlparser.AggregateExpr); ok {
		if aggr.Op == promqlparser.TOPK || aggr.Op == promqlparser.BOTTOMK || aggr.Op == promqlparser.LIMITK || aggr.Op == promqlparser.LIMIT_RATIO {
			return mergeOnly, nil
		}
	}
	checkForSort := func(expr promqlparser.Expr) (sortAsc, sortDesc bool) {
		if n, ok := expr.(*promqlparser.Call); ok {
			if n.Func != nil {
				if n.Func.Name == "sort" {
					sortAsc = true
				}
				if n.Func.Name == "sort_desc" {
					sortDesc = true
				}
				if n.Func.Name == "sort_by_label" {
					sortAsc = true
				}
				if n.Func.Name == "sort_by_label_desc" {
					sortDesc = true
				}
			}
		}
		return sortAsc, sortDesc
	}
	// Check the root expression for sort
	if sortAsc, sortDesc := checkForSort(expr); sortAsc || sortDesc {
		if sortAsc {
			return sortByValuesAsc, nil
		}
		return sortByValuesDesc, nil
	}

	// If the root expression is a binary expression, check the LHS and RHS for sort
	if bin, ok := expr.(*promqlparser.BinaryExpr); ok {
		if sortAsc, sortDesc := checkForSort(bin.LHS); sortAsc || sortDesc {
			if sortAsc {
				return sortByValuesAsc, nil
			}
			return sortByValuesDesc, nil
		}
		if sortAsc, sortDesc := checkForSort(bin.RHS); sortAsc || sortDesc {
			if sortAsc {
				return sortByValuesAsc, nil
			}
			return sortByValuesDesc, nil
		}
	}
	return sortByLabels, nil
}

// mergeSampleStreams deduplicates sample streams using a map.
func mergeSampleStreams(output map[string]SampleStream, sampleStreams []SampleStream) {
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
