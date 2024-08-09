package codec

import (
	"net/http"
	"strings"

	"github.com/prometheus/prometheus/util/stats"
	"github.com/prometheus/prometheus/promql"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/instantquery"
	"github.com/prometheus/prometheus/web/api/v1"
	"github.com/gogo/protobuf/proto"
)

type QueryRangeProtobufCodec struct{}

func (p QueryRangeProtobufCodec) ContentType() v1.MIMEType {
	return v1.MIMEType{Type: "application", SubType: "x-protobuf"}
}

func (p QueryRangeProtobufCodec) CanEncode(req *http.Request, resp *v1.Response) bool {
	if resp.Error != "" || resp.Data == nil {
		return false
	}
	return strings.HasSuffix(req.URL.Path, "/query_range")
}

func (p QueryRangeProtobufCodec) Encode(_ *http.Request, resp *v1.Response) ([]byte, error) {
	promtheusResponse := createPrometheusResponse(resp)
	b, err := proto.Marshal(promtheusResponse)
	return b, err
}

func createPrometheusResponse(resp *v1.Response) (*queryrange.PrometheusResponse) {
	data := resp.Data.(*v1.QueryData)

	sampleStreams := getSampleStreams(data)

	var stats *tripperware.PrometheusResponseStats
	if data.Stats != nil {
		builtin := data.Stats.Builtin()
		stats = &tripperware.PrometheusResponseStats{Samples: getStats(&builtin)}
	}

	return &queryrange.PrometheusResponse{
		Status: string(resp.Status),
		Data: queryrange.PrometheusData{
			ResultType: string(data.ResultType),
			Result:     *sampleStreams,
			Stats:      stats,
		},
		ErrorType: string(resp.ErrorType),
		Error:     resp.Error,
		Warnings: resp.Warnings,
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
	}

	return &statSamples
}

func getSampleStreams(data *v1.QueryData) *[]tripperware.SampleStream {
	sampleStreamsLen := len(data.Result.(promql.Matrix))
	sampleStreams := make([]tripperware.SampleStream, sampleStreamsLen)

	for i := 0; i < sampleStreamsLen; i++ {
		labelsLen := len(data.Result.(promql.Matrix)[i].Metric)
		var labels []cortexpb.LabelAdapter
		if labelsLen > 0 {
			labels = make([]cortexpb.LabelAdapter, labelsLen)
			for j := 0; j < labelsLen; j++ {
				labels[j] = cortexpb.LabelAdapter{
					Name:  data.Result.(promql.Matrix)[i].Metric[j].Name,
					Value: data.Result.(promql.Matrix)[i].Metric[j].Value,
				}
			}
		}

		samplesLen := len(data.Result.(promql.Matrix)[i].Floats)
		var samples []cortexpb.Sample
		if samplesLen > 0 {
			samples = make([]cortexpb.Sample, samplesLen)
			for j := 0; j < samplesLen; j++ {
				samples[j] = cortexpb.Sample{
					Value:       data.Result.(promql.Matrix)[i].Floats[j].F,
					TimestampMs: data.Result.(promql.Matrix)[i].Floats[j].T,
				}
			}
		}
		sampleStreams[i] = tripperware.SampleStream{Labels: labels, Samples: samples}
	}
	return &sampleStreams
}

func getSamples(data *v1.QueryData) *[]*instantquery.Sample {
	vectorSamplesLen := len(data.Result.(promql.Vector))
	vectorSamples := make([]*instantquery.Sample, vectorSamplesLen)

	for i := 0; i < vectorSamplesLen; i++ {
		labelsLen := len(data.Result.(promql.Vector)[i].Metric)
		var labels []cortexpb.LabelAdapter
		if labelsLen > 0 {
			labels = make([]cortexpb.LabelAdapter, labelsLen)
			for j := 0; j < labelsLen; j++ {
				labels[j] = cortexpb.LabelAdapter{
					Name:  data.Result.(promql.Vector)[i].Metric[j].Name,
					Value: data.Result.(promql.Vector)[i].Metric[j].Value,
				}
			}
		}

		vectorSamples[i] = &instantquery.Sample{Labels: labels,
			Sample: &cortexpb.Sample{
				TimestampMs: data.Result.(promql.Vector)[i].T,
				Value:       data.Result.(promql.Vector)[i].F,
			},
		}
	}
	return &vectorSamples
}
