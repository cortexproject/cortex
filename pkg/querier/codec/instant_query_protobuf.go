package codec

import (
	"net/http"
	"strings"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/instantquery"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/web/api/v1"
	"github.com/gogo/protobuf/proto"
)
type InstantQueryProtobufCodec struct{}

func (p InstantQueryProtobufCodec) ContentType() v1.MIMEType {
	return v1.MIMEType{Type: "application", SubType: "x-protobuf"}
}

func (p InstantQueryProtobufCodec) CanEncode(req *http.Request, resp *v1.Response) bool {
	if resp.Error != "" || resp.Data == nil {
		return false
	}
	return strings.HasSuffix(req.URL.Path, "/query")
}

func (p InstantQueryProtobufCodec) Encode(_ *http.Request, resp *v1.Response) ([]byte, error) {
	prometheusInstantQueryResponse, err := createPrometheusInstantQueryResponse(resp)
	if err != nil {
		return []byte{}, err
	}
	b, err := proto.Marshal(prometheusInstantQueryResponse)
	return b, err
}


func createPrometheusInstantQueryResponse(resp *v1.Response) (*instantquery.PrometheusInstantQueryResponse, error) {
	var data = resp.Data.(*v1.QueryData)

	var instantQueryResult instantquery.PrometheusInstantQueryResult
	switch string(data.ResultType) {
	case model.ValMatrix.String():
		instantQueryResult.Result = &instantquery.PrometheusInstantQueryResult_Matrix{
			Matrix: &instantquery.Matrix{
				SampleStreams: *getSampleStreams(data),
			},
		}
	case model.ValVector.String():
		instantQueryResult.Result = &instantquery.PrometheusInstantQueryResult_Vector{
			Vector: &instantquery.Vector{
				Samples: *getSamples(data),
			},
		}
	default:
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		rawBytes, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		instantQueryResult.Result = &instantquery.PrometheusInstantQueryResult_RawBytes{RawBytes: rawBytes}
	}

	var stats *tripperware.PrometheusResponseStats
	if data.Stats != nil {
		builtin := data.Stats.Builtin()
		stats = &tripperware.PrometheusResponseStats{Samples: getStats(&builtin)}
	}

	return &instantquery.PrometheusInstantQueryResponse{
		Status: string(resp.Status),
		Data: instantquery.PrometheusInstantQueryData{
			ResultType: string(data.ResultType),
			Result:     instantQueryResult,
			Stats:      stats,
		},
		ErrorType: string(resp.ErrorType),
		Error:     resp.Error,
		Warnings: resp.Warnings,
	}, nil
}
