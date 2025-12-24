package distributed_execution

import (
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/distributed_execution/querierpb"
)

const (
	BATCHSIZE      = 1000
	WritingTimeout = 100 * time.Millisecond
	MaxRetries     = 3
	RetryDelay     = 100 * time.Millisecond
)

// QuerierServer handles streaming of partial query results from a querier to clients
// during distributed query execution. It maintains query state through a QueryTracker.
type QuerierServer struct {
	queryTracker *QueryTracker
}

// NewQuerierServer creates a new QuerierServer instance with the provided QueryTracker
// to manage query fragment results.
func NewQuerierServer(cache *QueryTracker) *QuerierServer {
	return &QuerierServer{
		queryTracker: cache,
	}
}

// Series streams series metadata to the client, allowing discovery of the data shape
// before receiving actual values. This should be called before Next().
func (s *QuerierServer) Series(req *querierpb.SeriesRequest, srv querierpb.Querier_SeriesServer) error {
	key := MakeFragmentKey(req.QueryID, req.FragmentID)

	for {
		var result FragmentResult
		var ok bool
		for attempt := 0; attempt < MaxRetries; attempt++ {
			result, ok = s.queryTracker.Get(key)
			if ok {
				break
			}
			if attempt == MaxRetries {
				return fmt.Errorf("fragment not found after %d attempts: %v", MaxRetries, key)
			}
			time.Sleep(RetryDelay)
		}

		switch result.Status {
		case StatusDone:
			v1ResultData := result.Data.(*v1.QueryData)

			switch v1ResultData.ResultType {
			case parser.ValueTypeMatrix:
				series := v1ResultData.Result.(promql.Matrix)

				seriesBatch := []*querierpb.OneSeries{}
				for _, s := range series {
					oneSeries := &querierpb.OneSeries{
						Labels: make([]*querierpb.Label, s.Metric.Len()),
					}

					j := 0
					for name, val := range s.Metric.Map() {
						oneSeries.Labels[j] = &querierpb.Label{
							Name:  name,
							Value: val,
						}
						j++
					}
					seriesBatch = append(seriesBatch, oneSeries)
				}
				if err := srv.Send(&querierpb.SeriesBatch{
					OneSeries: seriesBatch}); err != nil {
					return err
				}

				return nil

			case parser.ValueTypeVector:
				samples := v1ResultData.Result.(promql.Vector)

				seriesBatch := []*querierpb.OneSeries{}
				for _, s := range samples {
					oneSeries := &querierpb.OneSeries{
						Labels: make([]*querierpb.Label, s.Metric.Len()),
					}

					j := 0
					for name, val := range s.Metric.Map() {
						oneSeries.Labels[j] = &querierpb.Label{
							Name:  name,
							Value: val,
						}
						j++
					}
					seriesBatch = append(seriesBatch, oneSeries)
				}
				if err := srv.Send(&querierpb.SeriesBatch{
					OneSeries: seriesBatch,
				}); err != nil {
					return err
				}
				return nil
			}

		case StatusError:
			return fmt.Errorf("fragment processing failed")

		case StatusWriting:
			time.Sleep(WritingTimeout)
			continue
		}
	}
}

// Next streams query result data to the client in batches. It should be called
// after Series() to receive the actual data values.
func (s *QuerierServer) Next(req *querierpb.NextRequest, srv querierpb.Querier_NextServer) error {
	key := MakeFragmentKey(req.QueryID, req.FragmentID)

	batchSize := int(req.Batchsize)
	if batchSize <= 0 {
		batchSize = BATCHSIZE
	}

	for {
		var result FragmentResult
		var ok bool
		for attempt := 0; attempt < MaxRetries; attempt++ {
			result, ok = s.queryTracker.Get(key)
			if ok {
				break
			}
			if attempt == MaxRetries {
				return fmt.Errorf("fragment not found after %d attempts: %v", MaxRetries, key)
			}
			time.Sleep(RetryDelay)
		}

		switch result.Status {
		case StatusDone:
			v1ResultData := result.Data.(*v1.QueryData)

			switch v1ResultData.ResultType {
			case parser.ValueTypeMatrix:
				matrix := v1ResultData.Result.(promql.Matrix)

				numTimeSteps := matrix.TotalSamples()

				for timeStep := 0; timeStep < numTimeSteps; timeStep += batchSize {
					batch := &querierpb.StepVectorBatch{
						StepVectors: make([]*querierpb.StepVector, 0, len(matrix)),
					}
					for t := 0; t < batchSize; t++ {
						for i, series := range matrix {
							vector, err := s.createVectorForTimestep(&series, timeStep+t, uint64(i))
							if err != nil {
								return err
							}
							batch.StepVectors = append(batch.StepVectors, vector)
						}
					}
					if err := srv.Send(batch); err != nil {
						return fmt.Errorf("error sending batch: %w", err)
					}
				}
				return nil

			case parser.ValueTypeVector:
				vector := v1ResultData.Result.(promql.Vector)

				for i := 0; i < len(vector); i += batchSize {
					end := i + batchSize
					if end > len(vector) {
						end = len(vector)
					}

					batch := &querierpb.StepVectorBatch{
						StepVectors: []*querierpb.StepVector{},
					}

					var timestamp int64
					sampleIDs := make([]uint64, 0, batchSize)
					samples := make([]float64, 0, batchSize)
					histogramIDs := make([]uint64, 0, batchSize)
					histograms := make([]*histogram.FloatHistogram, 0, batchSize)

					for j, sample := range (vector)[i:end] {
						if sample.H == nil {
							sampleIDs = append(sampleIDs, uint64(j))
							samples = append(samples, sample.F)
						} else {
							histogramIDs = append(histogramIDs, uint64(j))
							histograms = append(histograms, sample.H)
						}
					}
					vec := &querierpb.StepVector{
						T:             timestamp,
						Sample_IDs:    sampleIDs,
						Samples:       samples,
						Histogram_IDs: histogramIDs,
						Histograms:    floatHistogramsToFloatHistogramProto(histograms),
					}
					batch.StepVectors = append(batch.StepVectors, vec)
					if err := srv.Send(batch); err != nil {
						return err
					}
				}
				return nil

			default:
				return fmt.Errorf("unsupported result type: %v", v1ResultData.ResultType)
			}
		case StatusError:
			return fmt.Errorf("fragment processing failed")
		case StatusWriting:
			time.Sleep(WritingTimeout)
			continue
		}
	}
}

func (s *QuerierServer) createVectorForTimestep(series *promql.Series, timeStep int, sampleID uint64) (*querierpb.StepVector, error) {
	var samples []float64
	var sampleIDs []uint64
	var histograms []*histogram.FloatHistogram
	var histogramIDs []uint64
	var timestamp int64

	if timeStep < len(series.Floats) {
		point := series.Floats[timeStep]
		timestamp = point.T
		samples = append(samples, point.F)
		sampleIDs = append(sampleIDs, sampleID)
	}

	if timeStep < len(series.Histograms) {
		point := series.Histograms[timeStep]
		timestamp = point.T
		histograms = append(histograms, point.H)
		histogramIDs = append(histogramIDs, uint64(timeStep))
	}

	return &querierpb.StepVector{
		T:             timestamp,
		Sample_IDs:    sampleIDs,
		Samples:       samples,
		Histogram_IDs: histogramIDs,
		Histograms:    floatHistogramsToFloatHistogramProto(histograms),
	}, nil
}

func floatHistogramsToFloatHistogramProto(histograms []*histogram.FloatHistogram) []cortexpb.Histogram {
	if histograms == nil {
		return []cortexpb.Histogram{}
	}

	protoHistograms := make([]cortexpb.Histogram, 0, len(histograms))
	for _, h := range histograms {
		if h != nil {
			protoHist := floatHistogramToFloatHistogramProto(h)
			protoHistograms = append(protoHistograms, *protoHist)
		}
	}
	return protoHistograms
}

func floatHistogramToFloatHistogramProto(h *histogram.FloatHistogram) *cortexpb.Histogram {
	if h == nil {
		return nil
	}

	return &cortexpb.Histogram{
		ResetHint:     cortexpb.Histogram_ResetHint(h.CounterResetHint),
		Schema:        h.Schema,
		ZeroThreshold: h.ZeroThreshold,
		Count: &cortexpb.Histogram_CountFloat{
			CountFloat: h.Count,
		},
		ZeroCount: &cortexpb.Histogram_ZeroCountFloat{
			ZeroCountFloat: h.ZeroCount,
		},
		Sum:            h.Sum,
		PositiveSpans:  spansToSpansProto(h.PositiveSpans),
		PositiveCounts: h.PositiveBuckets,
		NegativeSpans:  spansToSpansProto(h.NegativeSpans),
		NegativeCounts: h.NegativeBuckets,
	}
}

func spansToSpansProto(spans []histogram.Span) []cortexpb.BucketSpan {
	if spans == nil {
		return nil
	}
	protoSpans := make([]cortexpb.BucketSpan, len(spans))
	for i, span := range spans {
		protoSpans[i] = cortexpb.BucketSpan{
			Offset: span.Offset,
			Length: span.Length,
		}
	}
	return protoSpans
}
