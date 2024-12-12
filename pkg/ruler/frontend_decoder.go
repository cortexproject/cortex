package ruler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/util/api"
)

const (
	statusError = "error"
)

type JsonDecoder struct{}
type ProtobufDecoder struct{}
type Warnings []string

type Decoder interface {
	Decode(body []byte) (promql.Vector, Warnings, error)
	ContentType() string
}

func (j JsonDecoder) ContentType() string {
	return "application/json"
}

func (j JsonDecoder) Decode(body []byte) (promql.Vector, Warnings, error) {
	var response api.Response

	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&response); err != nil {
		return nil, nil, err
	}
	if response.Status == statusError {
		return nil, response.Warnings, fmt.Errorf("failed to execute query with error: %s", response.Error)
	}
	data := struct {
		Type   model.ValueType `json:"resultType"`
		Result json.RawMessage `json:"result"`
	}{}

	if responseDataBytes, err := json.Marshal(response.Data); err != nil {
		return nil, response.Warnings, err
	} else {
		if err = json.Unmarshal(responseDataBytes, &data); err != nil {
			return nil, response.Warnings, err
		}
	}

	switch data.Type {
	case model.ValScalar:
		var scalar model.Scalar
		if err := json.Unmarshal(data.Result, &scalar); err != nil {
			return nil, nil, err
		}
		return scalarToPromQLVector(scalar), response.Warnings, nil
	case model.ValVector:
		var vector model.Vector
		if err := json.Unmarshal(data.Result, &vector); err != nil {
			return nil, nil, err
		}
		return j.vectorToPromQLVector(vector), response.Warnings, nil
	default:
		return nil, response.Warnings, errors.New("rule result is not a vector or scalar")
	}
}

func (j JsonDecoder) vectorToPromQLVector(vector model.Vector) promql.Vector {
	v := make([]promql.Sample, 0, len(vector))
	for _, sample := range vector {
		metric := make([]labels.Label, 0, len(sample.Metric))
		for k, v := range sample.Metric {
			metric = append(metric, labels.Label{
				Name:  string(k),
				Value: string(v),
			})
		}
		v = append(v, promql.Sample{
			T:      int64(sample.Timestamp),
			F:      float64(sample.Value),
			Metric: metric,
		})
	}
	return v
}

func (p ProtobufDecoder) ContentType() string {
	return tripperware.QueryResponseCortexMIMEType
}

func (p ProtobufDecoder) Decode(body []byte) (promql.Vector, Warnings, error) {
	resp := tripperware.PrometheusResponse{}
	if err := resp.Unmarshal(body); err != nil {
		return nil, nil, err
	}

	if resp.Status == statusError {
		return nil, resp.Warnings, fmt.Errorf("failed to execute query with error: %s", resp.Error)
	}

	switch resp.Data.ResultType {
	case "scalar":
		data := struct {
			Type   model.ValueType `json:"resultType"`
			Result json.RawMessage `json:"result"`
		}{}

		if err := json.Unmarshal(resp.Data.Result.GetRawBytes(), &data); err != nil {
			return nil, nil, err
		}

		var s model.Scalar
		if err := json.Unmarshal(data.Result, &s); err != nil {
			return nil, nil, err
		}
		return scalarToPromQLVector(s), resp.Warnings, nil
	case "vector":
		return p.vectorToPromQLVector(resp.Data.Result.GetVector()), resp.Warnings, nil
	default:
		return nil, resp.Warnings, errors.New("rule result is not a vector or scalar")
	}
}

func (p ProtobufDecoder) vectorToPromQLVector(vector *tripperware.Vector) promql.Vector {
	v := make([]promql.Sample, 0, len(vector.Samples))
	for _, sample := range vector.Samples {
		metric := cortexpb.FromLabelAdaptersToLabels(sample.Labels)

		if sample.Sample != nil {
			v = append(v, promql.Sample{
				T:      sample.Sample.TimestampMs,
				F:      sample.Sample.Value,
				Metric: metric,
			})
		}

		if sample.RawHistogram != nil {
			v = append(v, promql.Sample{
				T:      sample.RawHistogram.TimestampMs,
				H:      cortexpb.FloatHistogramProtoToFloatHistogram(*sample.RawHistogram),
				Metric: metric,
			})
		}
	}

	return v
}

func scalarToPromQLVector(s model.Scalar) promql.Vector {
	return promql.Vector{promql.Sample{
		T:      int64(s.Timestamp),
		F:      float64(s.Value),
		Metric: labels.Labels{},
	}}
}
