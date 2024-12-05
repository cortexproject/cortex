package codec

import (
	"github.com/gogo/protobuf/proto"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/stats"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

type ProtobufCodec struct {
	// cortexInternal enables encoding the whole native histogram data fields in response instead of keeping
	// only few sparse information like the default JSON/Protobuf codec does.
	// This will be used by Cortex Ruler to get native histograms data from Cortex Query Frontend because
	// rule evaluation requires the full native histogram data.
	CortexInternal bool
}

func (p ProtobufCodec) ContentType() v1.MIMEType {
	if !p.CortexInternal {
		return v1.MIMEType{Type: "application", SubType: "x-protobuf"}
	}
	// TODO: switch to use constants.
	return v1.MIMEType{Type: "application", SubType: "x-cortex-query+proto"}
}

func (p ProtobufCodec) CanEncode(resp *v1.Response) bool {
	// Errors are parsed by default json codec
	if resp.Error != "" || resp.Data == nil {
		return false
	}
	return true
}

// ProtobufCodec implementation is derived from https://github.com/prometheus/prometheus/blob/main/web/api/v1/json_codec.go
func (p ProtobufCodec) Encode(resp *v1.Response) ([]byte, error) {
	prometheusQueryResponse, err := createPrometheusQueryResponse(resp, p.CortexInternal)
	if err != nil {
		return []byte{}, err
	}
	b, err := proto.Marshal(prometheusQueryResponse)
	return b, err
}

func createPrometheusQueryResponse(resp *v1.Response, cortexInternal bool) (*tripperware.PrometheusResponse, error) {
	var data = resp.Data.(*v1.QueryData)

	var queryResult tripperware.PrometheusQueryResult
	switch string(data.ResultType) {
	case model.ValMatrix.String():
		queryResult.Result = &tripperware.PrometheusQueryResult_Matrix{
			Matrix: &tripperware.Matrix{
				SampleStreams: *getMatrixSampleStreams(data),
			},
		}
	case model.ValVector.String():
		queryResult.Result = &tripperware.PrometheusQueryResult_Vector{
			Vector: &tripperware.Vector{
				// cortexInternal tries to encode native histogram as dense format instead of sparse format.
				// This is only used for vector response type since internal response is only available for Ruler
				// client and Ruler only expects vector or scalar response type.
				Samples: *getVectorSamples(data, cortexInternal),
			},
		}
	default:
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		rawBytes, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		queryResult.Result = &tripperware.PrometheusQueryResult_RawBytes{RawBytes: rawBytes}
	}

	var stats *tripperware.PrometheusResponseStats
	if data.Stats != nil {
		builtin := data.Stats.Builtin()
		stats = &tripperware.PrometheusResponseStats{Samples: getStats(&builtin)}
	}

	return &tripperware.PrometheusResponse{
		Status: string(resp.Status),
		Data: tripperware.PrometheusData{
			ResultType: string(data.ResultType),
			Result:     queryResult,
			Stats:      stats,
		},
		ErrorType: string(resp.ErrorType),
		Error:     resp.Error,
		Warnings:  resp.Warnings,
	}, nil
}

func getMatrixSampleStreams(data *v1.QueryData) *[]tripperware.SampleStream {
	sampleStreamsLen := len(data.Result.(promql.Matrix))
	sampleStreams := make([]tripperware.SampleStream, sampleStreamsLen)

	for i := 0; i < sampleStreamsLen; i++ {
		sampleStream := data.Result.(promql.Matrix)[i]
		labelsLen := len(sampleStream.Metric)
		var labels []cortexpb.LabelAdapter
		if labelsLen > 0 {
			labels = make([]cortexpb.LabelAdapter, labelsLen)
			for j := 0; j < labelsLen; j++ {
				labels[j] = cortexpb.LabelAdapter{
					Name:  sampleStream.Metric[j].Name,
					Value: sampleStream.Metric[j].Value,
				}
			}
		}

		samplesLen := len(sampleStream.Floats)
		var samples []cortexpb.Sample
		if samplesLen > 0 {
			samples = make([]cortexpb.Sample, samplesLen)
			for j := 0; j < samplesLen; j++ {
				samples[j] = cortexpb.Sample{
					Value:       sampleStream.Floats[j].F,
					TimestampMs: sampleStream.Floats[j].T,
				}
			}
		}

		histogramsLen := len(sampleStream.Histograms)
		var histograms []tripperware.SampleHistogramPair
		if histogramsLen > 0 {
			histograms = make([]tripperware.SampleHistogramPair, histogramsLen)
			for j := 0; j < histogramsLen; j++ {
				bucketsLen := len(sampleStream.Histograms[j].H.NegativeBuckets) + len(sampleStream.Histograms[j].H.PositiveBuckets)
				if sampleStream.Histograms[j].H.ZeroCount > 0 {
					bucketsLen = len(sampleStream.Histograms[j].H.NegativeBuckets) + len(sampleStream.Histograms[j].H.PositiveBuckets) + 1
				}
				buckets := make([]*tripperware.HistogramBucket, bucketsLen)
				it := sampleStream.Histograms[j].H.AllBucketIterator()
				getBuckets(buckets, it)
				histograms[j] = tripperware.SampleHistogramPair{
					TimestampMs: sampleStream.Histograms[j].T,
					Histogram: tripperware.SampleHistogram{
						Count:   sampleStream.Histograms[j].H.Count,
						Sum:     sampleStream.Histograms[j].H.Sum,
						Buckets: buckets,
					},
				}
			}
		}
		sampleStreams[i] = tripperware.SampleStream{Labels: labels, Samples: samples, Histograms: histograms}
	}
	return &sampleStreams
}

func getVectorSamples(data *v1.QueryData, cortexInternal bool) *[]tripperware.Sample {
	vectorSamplesLen := len(data.Result.(promql.Vector))
	vectorSamples := make([]tripperware.Sample, vectorSamplesLen)

	for i := 0; i < vectorSamplesLen; i++ {
		sample := data.Result.(promql.Vector)[i]
		labelsLen := len(sample.Metric)
		var labels []cortexpb.LabelAdapter
		if labelsLen > 0 {
			labels = make([]cortexpb.LabelAdapter, labelsLen)
			for j := 0; j < labelsLen; j++ {
				labels[j] = cortexpb.LabelAdapter{
					Name:  sample.Metric[j].Name,
					Value: sample.Metric[j].Value,
				}
			}
		}
		vectorSamples[i].Labels = labels

		// Float samples only.
		if sample.H == nil {
			vectorSamples[i].Sample = &cortexpb.Sample{
				TimestampMs: sample.T,
				Value:       sample.F,
			}
			continue
		}

		// Cortex Internal request. Encode dense float native histograms.
		if cortexInternal {
			hp := cortexpb.FloatHistogramToHistogramProto(sample.T, sample.H)
			vectorSamples[i].RawHistogram = &hp
			continue
		}

		// Encode sparse native histograms.
		bucketsLen := len(sample.H.NegativeBuckets) + len(sample.H.PositiveBuckets)
		if sample.H.ZeroCount > 0 {
			bucketsLen = len(sample.H.NegativeBuckets) + len(sample.H.PositiveBuckets) + 1
		}
		buckets := make([]*tripperware.HistogramBucket, bucketsLen)
		it := sample.H.AllBucketIterator()
		getBuckets(buckets, it)
		vectorSamples[i].Histogram = &tripperware.SampleHistogramPair{
			TimestampMs: sample.T,
			Histogram: tripperware.SampleHistogram{
				Count:   sample.H.Count,
				Sum:     sample.H.Sum,
				Buckets: buckets,
			},
		}
	}
	return &vectorSamples
}

func getBuckets(bucketsList []*tripperware.HistogramBucket, it histogram.BucketIterator[float64]) {
	bucketIdx := 0
	for it.Next() {
		bucket := it.At()
		if bucket.Count == 0 {
			continue
		}
		boundaries := 2 // Exclusive on both sides AKA open interval.
		if bucket.LowerInclusive {
			if bucket.UpperInclusive {
				boundaries = 3 // Inclusive on both sides AKA closed interval.
			} else {
				boundaries = 1 // Inclusive only on lower end AKA right open.
			}
		} else {
			if bucket.UpperInclusive {
				boundaries = 0 // Inclusive only on upper end AKA left open.
			}
		}
		bucketsList[bucketIdx] = &tripperware.HistogramBucket{
			Boundaries: int32(boundaries),
			Lower:      bucket.Lower,
			Upper:      bucket.Upper,
			Count:      bucket.Count,
		}
		bucketIdx += 1
	}
}

func getStats(builtin *stats.BuiltinStats) *tripperware.PrometheusResponseSamplesStats {
	queryableSamplesStatsPerStepLen := len(builtin.Samples.TotalQueryableSamplesPerStep)
	queryableSamplesStatsPerStep := make([]*tripperware.PrometheusResponseQueryableSamplesStatsPerStep, queryableSamplesStatsPerStepLen)
	for i := 0; i < queryableSamplesStatsPerStepLen; i++ {
		queryableSamplesStatsPerStep[i] = &tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
			Value:       builtin.Samples.TotalQueryableSamplesPerStep[i].V,
			TimestampMs: builtin.Samples.TotalQueryableSamplesPerStep[i].T,
		}
	}

	statSamples := tripperware.PrometheusResponseSamplesStats{
		TotalQueryableSamples:        builtin.Samples.TotalQueryableSamples,
		TotalQueryableSamplesPerStep: queryableSamplesStatsPerStep,
		PeakSamples:                  int64(builtin.Samples.PeakSamples),
	}
	return &statSamples
}
